// configchanges.go maintains a full per-process PARTNER configuration mirror.
// Every replica reads every config.changes partition from the beginning; no
// shared consumer group is used because the resulting snapshot is in-memory.
package kafkaio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/partner-notification-service/internal/config"
)

type partnerUpdate struct {
	partnerID   string
	version     int64
	eventStatus string
	partner     config.Partner
}

// DecodeConfigChangeEvent validates the legacy compacted-topic key and the
// immutable payload. A nil update means the record belongs to another entity
// type (or is an untyped Kafka tombstone).
func DecodeConfigChangeEvent(key, payload []byte) (*partnerUpdate, error) {
	if payload == nil {
		return nil, nil
	}
	var event eventsv1.ConfigChangeEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ConfigChangeEvent: %w", err)
	}
	if event.GetEntityType() != commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER {
		return nil, nil
	}
	if event.GetEntityId() == "" || event.GetVersion() <= 0 {
		return nil, fmt.Errorf("PARTNER ConfigChangeEvent требует entity_id и version > 0")
	}
	if string(key) != event.GetEntityId() {
		return nil, fmt.Errorf("Kafka key %q не совпадает с PARTNER entity_id %q", string(key), event.GetEntityId())
	}

	update := &partnerUpdate{
		partnerID:   event.GetEntityId(),
		version:     event.GetVersion(),
		eventStatus: event.GetStatus(),
	}
	switch update.eventStatus {
	case "archived":
		return update, nil
	case "active":
		if len(event.GetPayloadJson()) == 0 {
			return nil, fmt.Errorf("PARTNER event status=active требует payload_json")
		}
		if err := json.Unmarshal(event.GetPayloadJson(), &update.partner); err != nil {
			return nil, fmt.Errorf("partner payload JSON: %w", err)
		}
		if update.partner.PartnerID != update.partnerID {
			return nil, fmt.Errorf("payload partner_id %q не совпадает с entity_id %q", update.partner.PartnerID, update.partnerID)
		}
		if int64(update.partner.Version) != update.version {
			return nil, fmt.Errorf("payload version %d не совпадает с event version %d", update.partner.Version, update.version)
		}
		if update.partner.Status != "active" && update.partner.Status != "suspended" && update.partner.Status != "archived" {
			return nil, fmt.Errorf("active config version содержит неизвестный payload status=%q", update.partner.Status)
		}
		return update, nil
	default:
		return nil, fmt.Errorf("неподдерживаемый PARTNER event status=%q", update.eventStatus)
	}
}

func applyConfigChangeRecord(store *config.Store, record *kgo.Record) error {
	update, err := DecodeConfigChangeEvent(record.Key, record.Value)
	if err != nil || update == nil {
		return err
	}

	var changed bool
	if update.eventStatus == "archived" {
		changed = store.ApplyArchive(update.version, update.partnerID)
	} else {
		changed = store.ApplyPartner(update.version, update.partner)
	}
	if changed {
		log.Printf("config.changes: partner_id=%s version=%d event_status=%s payload_status=%s применён к notification snapshot", update.partnerID, update.version, update.eventStatus, update.partner.Status)
	}
	return nil
}

// ConfigConsumer owns a broadcast/full-mirror consumer independent from the
// message.lifecycle consumer group. It reconnects from the beginning and uses
// Store version fencing for idempotency.
type ConfigConsumer struct {
	brokers []string
	store   *config.Store

	ready     chan struct{}
	readyOnce sync.Once
	healthy   atomic.Bool

	lastErrorMu sync.RWMutex
	lastError   error
}

func NewConfigConsumer(brokers []string, store *config.Store) *ConfigConsumer {
	return &ConfigConsumer{
		brokers: append([]string(nil), brokers...),
		store:   store,
		ready:   make(chan struct{}),
	}
}

func (c *ConfigConsumer) Healthy() bool { return c.healthy.Load() }

func (c *ConfigConsumer) setLastError(err error) {
	c.lastErrorMu.Lock()
	c.lastError = err
	c.lastErrorMu.Unlock()
}

func (c *ConfigConsumer) getLastError() error {
	c.lastErrorMu.RLock()
	defer c.lastErrorMu.RUnlock()
	return c.lastError
}

func (c *ConfigConsumer) AwaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("config.changes initial full replay: %w (last error: %v)", ctx.Err(), c.getLastError())
	}
}

func (c *ConfigConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		c.healthy.Store(false)
		err := c.consumeSession(ctx)
		c.healthy.Store(false)
		if ctx.Err() != nil {
			return
		}
		c.setLastError(err)
		log.Printf("config.changes full-mirror остановлен: %v; повтор через 2s, действует последний валидный snapshot", err)
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *ConfigConsumer) consumeSession(ctx context.Context) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ClientID("partner-notification-config-full-mirror"),
		kgo.ConsumeTopics(TopicConfigChanges),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return fmt.Errorf("new config consumer: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	starts, err := admin.ListStartOffsets(ctx, TopicConfigChanges)
	if err != nil {
		return fmt.Errorf("list config start offsets: %w", err)
	}
	ends, err := admin.ListEndOffsets(ctx, TopicConfigChanges)
	if err != nil {
		return fmt.Errorf("list config end offsets: %w", err)
	}

	positions, replayEnds, err := capturedReplayRange(starts, ends)
	if err != nil {
		return err
	}
	markReady := func() {
		if caughtUp(replayEnds, positions) {
			c.healthy.Store(true)
			c.setLastError(nil)
			c.readyOnce.Do(func() { close(c.ready) })
		}
	}
	markReady()
	lastBrokerCheck := time.Now()

	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := client.PollFetches(pollCtx)
		cancel()

		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			if fetchErr == nil {
				fetchErr = fmt.Errorf("fetch %s[%d]: %w", topic, partition, err)
			}
		})
		if fetchErr != nil {
			return fetchErr
		}
		if time.Since(lastBrokerCheck) >= 5*time.Second {
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			pingErr := client.Ping(pingCtx)
			pingCancel()
			if pingErr != nil {
				return fmt.Errorf("config.changes broker health check: %w", pingErr)
			}
			lastBrokerCheck = time.Now()
		}

		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if partition.Topic != TopicConfigChanges || partition.Err != nil {
				return
			}
			for _, record := range partition.Records {
				if err := applyConfigChangeRecord(c.store, record); err != nil {
					// Deterministic poison is skipped so a later corrected version
					// in the same partition can repair the snapshot.
					log.Printf("config.changes: отклонена запись partition=%d offset=%d: %v", record.Partition, record.Offset, err)
				}
				positions[record.Partition] = record.Offset + 1
			}
			if len(partition.Records) == 0 && partition.HighWatermark > positions[partition.Partition] {
				// Advances over offsets removed by compaction.
				positions[partition.Partition] = partition.HighWatermark
			}
		})
		markReady()
	}
	return ctx.Err()
}

func capturedReplayRange(starts, ends kadm.ListedOffsets) (map[int32]int64, map[int32]int64, error) {
	topicEnds := ends[TopicConfigChanges]
	if len(topicEnds) == 0 {
		return nil, nil, fmt.Errorf("topic %s не существует или не содержит партиций", TopicConfigChanges)
	}
	positions := make(map[int32]int64, len(topicEnds))
	replayEnds := make(map[int32]int64, len(topicEnds))
	for partition, end := range topicEnds {
		if end.Err != nil || end.Offset < 0 {
			return nil, nil, fmt.Errorf("end offset %s[%d]: offset=%d err=%v", TopicConfigChanges, partition, end.Offset, end.Err)
		}
		start, ok := starts.Lookup(TopicConfigChanges, partition)
		if !ok || start.Err != nil || start.Offset < 0 {
			return nil, nil, fmt.Errorf("start offset %s[%d] недоступен", TopicConfigChanges, partition)
		}
		positions[partition] = start.Offset
		replayEnds[partition] = end.Offset
	}
	return positions, replayEnds, nil
}

func caughtUp(ends, positions map[int32]int64) bool {
	if len(ends) == 0 {
		return false
	}
	for partition, end := range ends {
		if positions[partition] < end {
			return false
		}
	}
	return true
}
