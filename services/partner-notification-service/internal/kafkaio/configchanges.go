// configchanges.go — BACKOFFICE_ROADMAP.md "Production Readiness Review"
// P0 #4: on_config_change for this service's own partner snapshot. Mirrors
// config-cache-projector's DecodeConfigChangeEvent
// (services/config-cache-projector/internal/kafkaio/consumer.go) but with
// a narrower job — we don't project anything, we just re-fetch the
// touched partner_id from Configuration Redis (the thing
// config-cache-projector already projects into) and atomically swap it
// into config.Store, so live traffic sees a partner's self-service config
// change without a restart.
package kafkaio

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/partner-notification-service/internal/config"
)

// DecodeConfigChangeEvent — chiefly a filter: config.changes carries every
// entity_type (pipeline, policy_ruleset, billing_tariff, ...), and this
// service only cares about PARTNER. Returns (nil, nil) for anything else —
// not our concern, but not an error either (symmetric with
// config-cache-projector's subscriber_consent skip). Returns an error only
// for genuinely malformed records (bad protobuf bytes, or a PARTNER event
// missing entity_id) so the caller can distinguish "skip, commit" from
// "poison, log and commit anyway" from "retriable".
func DecodeConfigChangeEvent(payload []byte) (*eventsv1.ConfigChangeEvent, error) {
	var event eventsv1.ConfigChangeEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ConfigChangeEvent: %w", err)
	}
	if event.GetEntityType() != commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER {
		return nil, nil
	}
	if event.GetEntityId() == "" {
		return nil, fmt.Errorf("ConfigChangeEvent(entity_type=PARTNER) без entity_id")
	}
	return &event, nil
}

// HandleConfigChangeRecord — the config.changes side of P0 #4's fix.
// Called from main.go's PollOnce loop whenever record.Topic ==
// TopicConfigChanges. Re-fetches (does NOT trust the event's own
// payload_json — see RedisSource.FetchPartner's doc comment) partner_id
// from Configuration Redis and publishes the result into store:
//
//   - fetch error (Redis down/timeout) -> returns error, caller does not
//     commit the offset, record is redelivered — same at-least-once
//     retry semantics HandleRecord already uses for its own infra errors.
//   - not found in Configuration Redis, or found but Partner.IsArchived()
//     -> store.Remove: partner stops resolving to a delivery channel
//     immediately, instead of lingering with stale config until a
//     restart (this is the "handle removal/archival" half of P0 #4 —
//     status="suspended" is deliberately NOT removed, see
//     Partner.IsArchived's doc comment: suspended partners should still
//     resolve, just presumably fail delivery/policy checks elsewhere).
//   - found and active/suspended -> store.Upsert: new config live for the
//     next message routed to this partner_id.
func HandleConfigChangeRecord(ctx context.Context, store *config.Store, source *config.RedisSource, record *kgo.Record) error {
	event, err := DecodeConfigChangeEvent(record.Value)
	if err != nil {
		log.Printf("config.changes: poison message пропущен (partition=%d offset=%d): %v", record.Partition, record.Offset, err)
		return nil
	}
	if event == nil {
		return nil // другой entity_type — не наш путь
	}

	partnerID := event.GetEntityId()
	partner, found, err := source.FetchPartner(ctx, partnerID)
	if err != nil {
		return fmt.Errorf("config.changes: re-fetch partner_id=%s из Configuration Redis: %w", partnerID, err)
	}

	if !found || partner.IsArchived() {
		store.Remove(partnerID)
		log.Printf("config.changes: partner_id=%s удалён из живого снапшота (found=%v)", partnerID, found)
		return nil
	}

	store.Upsert(partner)
	log.Printf("config.changes: partner_id=%s (version=%d, status=%s) обновлён в живом снапшоте", partnerID, partner.Version, partner.Status)
	return nil
}
