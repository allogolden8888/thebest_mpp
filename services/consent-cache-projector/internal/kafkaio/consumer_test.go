package kafkaio

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeConsentEventReturnsNilForOtherEntityTypes(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
	})
	event, err := DecodeConsentEvent(payload)
	if err != nil {
		t.Fatalf("не должно быть ошибки на другом entity_type: %v", err)
	}
	if event != nil {
		t.Fatalf("ожидали nil для entity_type != subscriber_consent")
	}
}

func TestDecodeConsentEventReturnsEventForSubscriberConsent(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
		EntityId:   "998901234567:CATEGORY:ADVERTISING:SMS",
	})
	event, err := DecodeConsentEvent(payload)
	if err != nil {
		t.Fatalf("DecodeConsentEvent failed: %v", err)
	}
	if event == nil {
		t.Fatalf("ожидали непустое событие для subscriber_consent")
	}
}

func TestDecodeConsentEventRejectsMissingEntityID(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
	})
	_, err := DecodeConsentEvent(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку без entity_id")
	}
}

func TestDecodeConsentEventRejectsGarbage(t *testing.T) {
	_, err := DecodeConsentEvent([]byte{0xff, 0x00, 0xff, 0x01})
	if err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}

// CODE_REVIEW.md cross-cutting finding + test-quality note: "no test
// exercises the actual Kafka consume/produce loop... only pure
// decode/build helper functions are unit-tested". These tests exercise
// processRecords, the orchestration logic extracted from Run() specifically
// to make this testable without a live Kafka broker — same shape as
// config-cache-projector's equivalent tests.

func recordFor(t *testing.T, partition int32, offset int64, event *eventsv1.ConfigChangeEvent) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return &kgo.Record{Partition: partition, Offset: offset, Value: payload}
}

func consentEvent(entityID, status string) *eventsv1.ConfigChangeEvent {
	return &eventsv1.ConfigChangeEvent{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
		EntityId:    entityID,
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
		Status:      status,
	}
}

func TestProcessRecordsCommitsOnlySuccessfullyHandledRecords(t *testing.T) {
	r1 := recordFor(t, 0, 10, consentEvent("a", "active"))
	r2 := recordFor(t, 0, 11, consentEvent("b", "active"))

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
}

// CODE_REVIEW.md cross-cutting: "A failed ApplyConsentChange is only
// logged — the record is never retried, and its offset commits on the
// next 5s tick anyway. If no later update ever arrives for that entity,
// the projected cache is permanently stale/missing" — the direct
// regression test.
func TestProcessRecordsDoesNotCommitFailedRecordOrLaterRecordsInSamePartition(t *testing.T) {
	r1 := recordFor(t, 0, 1, consentEvent("fails", "active"))
	r2 := recordFor(t, 0, 2, consentEvent("after-failure", "active"))

	handle := func(e *eventsv1.ConfigChangeEvent) error {
		return fmt.Errorf("simulated Redis timeout")
	}

	var retried []*kgo.Record
	toCommit := processRecords([]*kgo.Record{r1, r2}, handle, nil, func(r *kgo.Record, err error) {
		retried = append(retried, r)
	})

	if len(toCommit) != 0 {
		t.Fatalf("ожидали 0 закоммиченных записей, получили %d", len(toCommit))
	}
	if len(retried) != 1 || retried[0] != r1 {
		t.Fatalf("ожидали onRetry ровно один раз для r1 (r2 не должен даже обрабатываться), получили %d вызовов", len(retried))
	}
}

func TestProcessRecordsSkipsAndCommitsPoisonMessage(t *testing.T) {
	poison := &kgo.Record{Partition: 0, Offset: 1, Value: []byte{0xff, 0x00, 0xff}}
	good := recordFor(t, 0, 2, consentEvent("a", "active"))

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
		t.Fatalf("poison message должен коммититься (bounded), получили %d", len(toCommit))
	}
	if len(poisoned) != 1 {
		t.Fatalf("ожидали onPoison ровно один раз")
	}
	if handledCount != 1 {
		t.Fatalf("handle должен вызываться только для валидной записи, вызван %d раз", handledCount)
	}
}

func TestProcessRecordsSkipsAndCommitsOtherEntityTypes(t *testing.T) {
	other := recordFor(t, 0, 1, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
	})

	var handledCount int
	handle := func(e *eventsv1.ConfigChangeEvent) error {
		handledCount++
		return nil
	}
	toCommit := processRecords([]*kgo.Record{other}, handle, nil, nil)

	if len(toCommit) != 1 {
		t.Fatalf("записи не-subscriber_consent должны коммититься (не наш путь), получили %d", len(toCommit))
	}
	if handledCount != 0 {
		t.Fatalf("handle не должен вызываться для entity_type != subscriber_consent")
	}
}