package kafkaio

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeConfigChangeEventRoundTrips(t *testing.T) {
	original := &eventsv1.ConfigChangeEvent{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		Version:     1,
		PayloadJson: []byte(`{}`),
		Status:      "active",
	}
	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	got, err := DecodeConfigChangeEvent(payload)
	if err != nil {
		t.Fatalf("DecodeConfigChangeEvent failed: %v", err)
	}
	if got.GetEntityId() != "acme" || got.GetVersion() != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestDecodeConfigChangeEventRejectsMissingEntityID(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{Version: 1})
	_, err := DecodeConfigChangeEvent(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку для записи без entity_id")
	}
}

func TestDecodeConfigChangeEventRejectsGarbage(t *testing.T) {
	_, err := DecodeConfigChangeEvent([]byte{0xff, 0x00, 0xff, 0x01})
	if err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}

// CODE_REVIEW.md finding #1: subscriber_consent events aren't filtered out
// and get written into Configuration Redis. Fixed: DecodeConfigChangeEvent
// now returns (nil, nil) for SUBSCRIBER_CONSENT — same "not our path"
// signal that WriteProjection's caller in main.go already treats as a
// no-op, symmetric with consent-cache-projector's DecodeConsentEvent.
func TestDecodeConfigChangeEventFiltersOutSubscriberConsent(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
		EntityId:   "998901234567:CATEGORY:ADVERTISING:SMS",
	})
	event, err := DecodeConfigChangeEvent(payload)
	if err != nil {
		t.Fatalf("не должно быть ошибки на subscriber_consent (просто не наш путь): %v", err)
	}
	if event != nil {
		t.Fatalf("ожидали nil для subscriber_consent — config-cache-projector не должен проецировать consent-данные в Configuration Redis")
	}
}

// CODE_REVIEW.md finding #2: malformed/unset entity_type is silently
// accepted and projected under a bogus "unspecified" key.
func TestDecodeConfigChangeEventRejectsUnspecifiedEntityType(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		EntityId:   "x",
	})
	_, err := DecodeConfigChangeEvent(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку для entity_type=UNSPECIFIED вместо тихой проекции под бракованным ключом")
	}
}

func TestDecodeConfigChangeEventAcceptsAllKnownEntityTypesExceptConsent(t *testing.T) {
	known := []commonv1.ConfigEntityType{
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR,
	}
	for _, et := range known {
		payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{EntityType: et, EntityId: "x"})
		event, err := DecodeConfigChangeEvent(payload)
		if err != nil {
			t.Fatalf("entity_type %v должен приниматься без ошибки: %v", et, err)
		}
		if event == nil {
			t.Fatalf("entity_type %v не должен фильтроваться", et)
		}
	}
}

// CODE_REVIEW.md cross-cutting finding + test-quality note: "no test
// exercises Consumer.Run — only the pure DecodeConfigChangeEvent function
// is tested... untestable-in-principle against the current suite". These
// tests exercise processRecords, the orchestration logic extracted from
// Run() specifically to make this testable without a live Kafka broker.

func recordFor(t *testing.T, partition int32, offset int64, event *eventsv1.ConfigChangeEvent) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return &kgo.Record{Partition: partition, Offset: offset, Value: payload}
}

func TestProcessRecordsCommitsOnlySuccessfullyHandledRecords(t *testing.T) {
	r1 := recordFor(t, 0, 10, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "a"})
	r2 := recordFor(t, 0, 11, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "b"})

	var handled []string
	handle := func(e *eventsv1.ConfigChangeEvent) error {
		handled = append(handled, e.GetEntityId())
		if e.GetEntityId() == "b" {
			return fmt.Errorf("simulated Redis timeout")
		}
		return nil
	}

	toCommit := processRecords([]*kgo.Record{r1, r2}, handle, nil, nil)

	if len(toCommit) != 1 || toCommit[0] != r1 {
		t.Fatalf("ожидали закоммитить только r1 (offset=10), получили %d записей", len(toCommit))
	}
	if len(handled) != 2 {
		t.Fatalf("ожидали, что handle вызван для обеих записей, вызван для %d", len(handled))
	}
}

// CODE_REVIEW.md cross-cutting: "A failed WriteProjection is only logged —
// the record is never retried, and its offset commits on the next 5s tick
// anyway". This test is the direct regression test for that bug: a
// retriable failure must not end up in toCommit.
func TestProcessRecordsDoesNotCommitFailedRecordOrLaterRecordsInSamePartition(t *testing.T) {
	r1 := recordFor(t, 0, 1, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "fails"})
	r2 := recordFor(t, 0, 2, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "after-failure"})

	handle := func(e *eventsv1.ConfigChangeEvent) error {
		return fmt.Errorf("simulated Redis timeout")
	}

	var retried []*kgo.Record
	toCommit := processRecords([]*kgo.Record{r1, r2}, handle, nil, func(r *kgo.Record, err error) {
		retried = append(retried, r)
	})

	if len(toCommit) != 0 {
		t.Fatalf("ожидали 0 закоммиченных записей — offset не должен продвигаться мимо непереданного сообщения, получили %d", len(toCommit))
	}
	if len(retried) != 1 || retried[0] != r1 {
		t.Fatalf("ожидали onRetry вызванным ровно один раз для r1 (r2 не должен даже обрабатываться), получили %d вызовов", len(retried))
	}
}

func TestProcessRecordsSkipsAndCommitsPoisonMessage(t *testing.T) {
	poison := &kgo.Record{Partition: 0, Offset: 1, Value: []byte{0xff, 0x00, 0xff}}
	good := recordFor(t, 0, 2, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "a"})

	var handledCount int
	handle := func(e *eventsv1.ConfigChangeEvent) error {
		handledCount++
		return nil
	}
	var poisoned []*kgo.Record
	toCommit := processRecords([]*kgo.Record{poison, good}, handle, func(r *kgo.Record, err error) {
		poisoned = append(poisoned, r)
	}, nil)

	if len(toCommit) != 2 {
		t.Fatalf("poison message должен коммититься (bounded, не блокировать партицию навечно), а не только good — получили %d", len(toCommit))
	}
	if len(poisoned) != 1 || poisoned[0] != poison {
		t.Fatalf("ожидали onPoison вызванным ровно один раз для poison-записи")
	}
	if handledCount != 1 {
		t.Fatalf("handle должен вызываться только для валидной записи, вызван %d раз", handledCount)
	}
}

func TestProcessRecordsSkipsAndCommitsSubscriberConsentEvent(t *testing.T) {
	consentRecord := recordFor(t, 0, 1, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
		EntityId:   "998901234567:CATEGORY:ADVERTISING:SMS",
	})

	var handledCount int
	handle := func(e *eventsv1.ConfigChangeEvent) error {
		handledCount++
		return nil
	}
	toCommit := processRecords([]*kgo.Record{consentRecord}, handle, nil, nil)

	if len(toCommit) != 1 {
		t.Fatalf("subscriber_consent запись должна коммититься (не наш путь, не ошибка), получили %d", len(toCommit))
	}
	if handledCount != 0 {
		t.Fatalf("handle не должен вызываться для subscriber_consent")
	}
}

// Партиции обрабатываются независимо: retriable-ошибка на партиции 0 не
// должна блокировать прогресс по партиции 1.
func TestProcessRecordsPartitionsAreIndependent(t *testing.T) {
	failing := recordFor(t, 0, 1, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "p0"})
	okOtherPartition := recordFor(t, 1, 1, &eventsv1.ConfigChangeEvent{EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "p1"})

	handle := func(e *eventsv1.ConfigChangeEvent) error {
		if e.GetEntityId() == "p0" {
			return fmt.Errorf("simulated failure on partition 0")
		}
		return nil
	}

	toCommit := processRecords([]*kgo.Record{failing, okOtherPartition}, handle, nil, nil)

	if len(toCommit) != 1 || toCommit[0] != okOtherPartition {
		t.Fatalf("ожидали закоммитить только запись с партиции 1, получили %d записей", len(toCommit))
	}
}
