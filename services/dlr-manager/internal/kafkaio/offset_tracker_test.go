package kafkaio

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Тот же паттерн, что уже тестируется в delivery-service/billing-service
// (Java, KafkaIoTest) — применён здесь для того же класса бага.

func rec(topic string, partition int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Offset: offset}
}

func TestOffsetTrackerAllSuccessfulCommitsAllRecords(t *testing.T) {
	tracker := NewOffsetTracker()
	tracker.RecordSuccess(rec("operator.dlr", 0, 10))
	tracker.RecordSuccess(rec("operator.dlr", 0, 11))
	tracker.RecordSuccess(rec("operator.dlr", 0, 12))

	committable := tracker.CommittableRecords()
	if len(committable) != 1 || committable[0].Offset != 12 {
		t.Fatalf("ожидалась одна запись с offset=12, получили %+v", committable)
	}
}

func TestOffsetTrackerFailureSuspendsPartitionAndSkipsLaterRecords(t *testing.T) {
	tp := PartitionKey{Topic: "operator.dlr", Partition: 0}
	tracker := NewOffsetTracker()
	tracker.RecordSuccess(rec("operator.dlr", 0, 10))
	tracker.RecordFailure(tp)

	if !tracker.IsSuspended(tp) {
		t.Fatal("партиция должна быть приостановлена после ошибки")
	}
	committable := tracker.CommittableRecords()
	if len(committable) != 1 || committable[0].Offset != 10 {
		t.Fatalf("должна коммититься только запись ДО ошибки (offset=10), получили %+v", committable)
	}
}

func TestOffsetTrackerIndependentPartitionsTrackedSeparately(t *testing.T) {
	tp0 := PartitionKey{Topic: "operator.dlr", Partition: 0}
	tp1 := PartitionKey{Topic: "operator.dlr", Partition: 1}
	tracker := NewOffsetTracker()

	tracker.RecordSuccess(rec("operator.dlr", 0, 5))
	tracker.RecordFailure(tp1)
	tracker.RecordSuccess(rec("operator.dlr", 0, 6))

	if tracker.IsSuspended(tp0) {
		t.Fatal("партиция 0 не должна быть приостановлена")
	}
	if !tracker.IsSuspended(tp1) {
		t.Fatal("партиция 1 должна быть приостановлена")
	}
	committable := tracker.CommittableRecords()
	if len(committable) != 1 || committable[0].Offset != 6 || committable[0].Partition != 0 {
		t.Fatalf("должна коммититься только запись партиции 0 (offset=6), получили %+v", committable)
	}
}

func TestOffsetTrackerNoSuccessMeansNothingToCommit(t *testing.T) {
	tp := PartitionKey{Topic: "operator.dlr", Partition: 0}
	tracker := NewOffsetTracker()
	tracker.RecordFailure(tp)

	if len(tracker.CommittableRecords()) != 0 {
		t.Fatalf("без единого успеха коммитить нечего, получили %+v", tracker.CommittableRecords())
	}
}
