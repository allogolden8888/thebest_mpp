package kafkaio

import "github.com/twmb/franz-go/pkg/kgo"

// PartitionKey — идентифицирует партицию для целей OffsetTracker (topic,
// т.к. этот consumer подписан сразу на два топика в одной группе, partition
// одна и та же не значит одна и та же партиция между ними).
type PartitionKey struct {
	Topic     string
	Partition int32
}

// OffsetTracker — MEDIUM находка кодревью (PART 2, dlr-manager #2):
// раньше каждая запись коммитилась индивидуально сразу после handleRecord;
// если запись N не декодировалась (или иначе падала обработка),
// handleRecord возвращал ошибку без коммита, но у Kafka коммит per-partition
// один — если запись N+1 на той же партиции успешно обрабатывалась и
// коммитилась following запись, это коммитило позицию МИМО ещё не
// подтверждённой N, и она терялась молча и навсегда (README ошибочно
// утверждал, что оффсет "не продвигается" в этом случае).
//
// Тот же паттерн, что уже используется в delivery-service/billing-service
// (Java, `KafkaIo.OffsetTracker`) — создаётся заново на каждый poll,
// приостанавливает партицию при первой ошибке в рамках этого poll'а
// (последующие успешные записи той же партиции в ЭТОМ poll'е не
// обрабатываются и не коммитятся), коммитит только оффсеты партиций, где
// все записи этого poll'а до точки остановки обработаны успешно. При
// перезапуске/переподключении Kafka передоставит с последнего
// закоммиченного оффсета — та же самая проблемная запись будет
// переобработана, партиция реально "застревает" на ней (видимо через
// растущий consumer lag), как и заявляет README, а не молча теряется.
type OffsetTracker struct {
	committable map[PartitionKey]*kgo.Record
	suspended   map[PartitionKey]bool
}

func NewOffsetTracker() *OffsetTracker {
	return &OffsetTracker{
		committable: make(map[PartitionKey]*kgo.Record),
		suspended:   make(map[PartitionKey]bool),
	}
}

func (t *OffsetTracker) IsSuspended(key PartitionKey) bool {
	return t.suspended[key]
}

func (t *OffsetTracker) RecordSuccess(rec *kgo.Record) {
	t.committable[PartitionKey{Topic: rec.Topic, Partition: rec.Partition}] = rec
}

func (t *OffsetTracker) RecordFailure(key PartitionKey) {
	t.suspended[key] = true
}

// CommittableRecords — по одной записи на партицию (самая свежая успешная
// ДО первой ошибки этого poll'а, если она была) — то, что нужно передать в
// Consumer.CommitRecords.
func (t *OffsetTracker) CommittableRecords() []*kgo.Record {
	records := make([]*kgo.Record, 0, len(t.committable))
	for _, rec := range t.committable {
		records = append(records, rec)
	}
	return records
}
