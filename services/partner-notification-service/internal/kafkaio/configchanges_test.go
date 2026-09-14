package kafkaio

import (
	"errors"
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/partner-notification-service/internal/config"
)

func partnerPayload(id string, version int64, status string) []byte {
	return []byte(`{"partner_id":"` + id + `","version":` + strconv.FormatInt(version, 10) + `,"status":"` + status + `","applications":[{"application_id":"` + id + `_app"}]}`)
}

func configRecord(t *testing.T, key string, event *eventsv1.ConfigChangeEvent) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Topic: TopicConfigChanges, Key: []byte(key), Value: payload}
}

func partnerEvent(id string, version int64, eventStatus, payloadStatus string) *eventsv1.ConfigChangeEvent {
	event := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   id,
		Version:    version,
		Status:     eventStatus,
	}
	if eventStatus == "active" {
		event.PayloadJson = partnerPayload(id, version, payloadStatus)
	}
	return event
}

func TestDecodeSkipsNonPartnerAndUntypedTombstone(t *testing.T) {
	nonPartner := configRecord(t, "tariff", &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF,
		EntityId:   "tariff",
	})
	if got, err := DecodeConfigChangeEvent(nonPartner.Key, nonPartner.Value); err != nil || got != nil {
		t.Fatalf("non-partner: got=%+v err=%v", got, err)
	}
	if got, err := DecodeConfigChangeEvent([]byte("acme"), nil); err != nil || got != nil {
		t.Fatalf("Kafka tombstone: got=%+v err=%v", got, err)
	}
}

func TestDecodeRejectsMalformedIdentityAndPayload(t *testing.T) {
	mismatchedID := partnerEvent("acme", 1, "active", "active")
	mismatchedID.PayloadJson = partnerPayload("other", 1, "active")
	mismatchedVersion := partnerEvent("acme", 2, "active", "active")
	mismatchedVersion.PayloadJson = partnerPayload("acme", 1, "active")
	cases := []struct {
		name  string
		key   string
		event *eventsv1.ConfigChangeEvent
		value []byte
	}{
		{name: "missing entity id", key: "", event: partnerEvent("", 1, "active", "active")},
		{name: "missing version", key: "acme", event: partnerEvent("acme", 0, "active", "active")},
		{name: "key mismatch", key: "other", event: partnerEvent("acme", 1, "active", "active")},
		{name: "unsupported lifecycle status", key: "acme", event: partnerEvent("acme", 1, "draft", "active")},
		{name: "payload id mismatch", key: "acme", event: mismatchedID},
		{name: "payload version mismatch", key: "acme", event: mismatchedVersion},
		{name: "garbage", key: "acme", value: []byte("not-protobuf")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := tc.value
			if tc.event != nil {
				value = configRecord(t, tc.key, tc.event).Value
			}
			if _, err := DecodeConfigChangeEvent([]byte(tc.key), value); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestDecodeAcceptsSuspendedPayloadInsideActiveConfigVersion(t *testing.T) {
	record := configRecord(t, "acme", partnerEvent("acme", 2, "active", "suspended"))
	update, err := DecodeConfigChangeEvent(record.Key, record.Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if update.partner.Status != "suspended" || update.eventStatus != "active" {
		t.Fatalf("update=%+v", update)
	}
}

func TestApplyUsesDirectPayloadAndVersionFence(t *testing.T) {
	store := config.NewStore(config.NewSnapshot(nil))
	active := configRecord(t, "acme", partnerEvent("acme", 2, "active", "active"))
	if err := applyConfigChangeRecord(store, active); err != nil {
		t.Fatalf("apply active: %v", err)
	}
	partner, _, found := store.FirstApplication("acme")
	if !found || partner.Version != 2 {
		t.Fatalf("active partner not applied: found=%v partner=%+v", found, partner)
	}

	stale := configRecord(t, "acme", partnerEvent("acme", 1, "active", "active"))
	if err := applyConfigChangeRecord(store, stale); err != nil {
		t.Fatalf("stale replay: %v", err)
	}
	partner, _, _ = store.FirstApplication("acme")
	if partner.Version != 2 {
		t.Fatalf("stale replay rolled state back to version %d", partner.Version)
	}
}

func TestArchiveSameVersionAndOldReplayCannotRevive(t *testing.T) {
	initial := config.Partner{
		PartnerID: "acme", Version: 2, Status: "active",
		Applications: []config.Application{{ApplicationID: "acme_app"}},
	}
	store := config.NewStore(config.NewSnapshot([]config.Partner{initial}))
	archive := configRecord(t, "acme", partnerEvent("acme", 2, "archived", ""))
	if err := applyConfigChangeRecord(store, archive); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, _, found := store.FirstApplication("acme"); found {
		t.Fatal("archive did not remove partner")
	}

	oldActive := configRecord(t, "acme", partnerEvent("acme", 2, "active", "active"))
	if err := applyConfigChangeRecord(store, oldActive); err != nil {
		t.Fatalf("old active replay: %v", err)
	}
	if _, _, found := store.FirstApplication("acme"); found {
		t.Fatal("same-version active replay revived archived partner")
	}
}

func TestCapturedReplayRangeAndCaughtUp(t *testing.T) {
	starts := kadm.ListedOffsets{TopicConfigChanges: {
		0: {Topic: TopicConfigChanges, Partition: 0, Offset: 4},
		1: {Topic: TopicConfigChanges, Partition: 1, Offset: 8},
	}}
	ends := kadm.ListedOffsets{TopicConfigChanges: {
		0: {Topic: TopicConfigChanges, Partition: 0, Offset: 10},
		1: {Topic: TopicConfigChanges, Partition: 1, Offset: 8},
	}}
	positions, replayEnds, err := capturedReplayRange(starts, ends)
	if err != nil {
		t.Fatalf("capturedReplayRange: %v", err)
	}
	if caughtUp(replayEnds, positions) {
		t.Fatal("partition 0 has not reached captured EOF")
	}
	positions[0] = 10
	if !caughtUp(replayEnds, positions) {
		t.Fatal("all partitions reached captured EOF")
	}
}

func TestCapturedReplayRangeRejectsMissingOrBrokenPartition(t *testing.T) {
	starts := kadm.ListedOffsets{TopicConfigChanges: {0: {Offset: 0}}}
	ends := kadm.ListedOffsets{TopicConfigChanges: {0: {Offset: 1}, 1: {Offset: 2}}}
	if _, _, err := capturedReplayRange(starts, ends); err == nil {
		t.Fatal("missing start partition must fail")
	}
	ends = kadm.ListedOffsets{TopicConfigChanges: {0: {Offset: 1, Err: errors.New("offline")}}}
	if _, _, err := capturedReplayRange(starts, ends); err == nil {
		t.Fatal("partition error must fail")
	}
}
