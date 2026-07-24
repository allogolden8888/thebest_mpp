// Package writer — batch_buffer/flush_batch (service_internal_methods.md
// §4.1): накопление CorrelationRecord в буфере, сброс в PostgreSQL по
// таймеру или размеру.
package writer

import (
	"sync"
	"time"
)

// CorrelationRecord — то, что в итоге пишется в dlr.dlr_correlation
// (migrations/V009__dlr_correlation.sql), построено из OperatorSubmitAccepted.
type CorrelationRecord struct {
	OperatorID       string
	SmscMessageID    string
	SegmentID        int32
	MessageID        string
	StageExecutionID string
	SubmittedAt      time.Time
	ExpiresAt        time.Time
}

// BufferedRecord — CorrelationRecord + позиция в Kafka, откуда оно взято.
// Оффсет коммитится только после успешного flush в PostgreSQL (см.
// internal/kafkaio/consumer.go) — тот же принцип "коммит после успешной
// записи", что применялся во всех Kafka-consumer'ах этой сессии, здесь —
// для батча, а не отдельной записи.
type BufferedRecord struct {
	Correlation CorrelationRecord
	Partition   int32
	Offset      int64
}

// BatchBuffer — чистая, потокобезопасная структура, тестируется без
// Kafka/PostgreSQL.
type BatchBuffer struct {
	mu      sync.Mutex
	records []BufferedRecord
	maxSize int
}

func NewBatchBuffer(maxSize int) *BatchBuffer {
	return &BatchBuffer{maxSize: maxSize}
}

// Add возвращает true, если буфер достиг maxSize и должен быть сброшен
// немедленно (не дожидаясь следующего тика таймера) — "по таймеру ИЛИ
// размеру" (service_internal_methods.md §4.1).
func (b *BatchBuffer) Add(rec BufferedRecord) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.records = append(b.records, rec)
	return len(b.records) >= b.maxSize
}

// Snapshot — копия текущего содержимого буфера, БЕЗ очистки. Вызывающая
// сторона обязана сама вызвать Clear() только после того, как записи
// реально успешно записаны в PostgreSQL — если снапшот сразу же удалять
// здесь, неудачный flush навсегда терял бы буферизованные записи (тот же
// класс ошибки, что offset-commit-до-подтверждения в Kafka-консьюмерах
// этой сессии, только на уровне буфера, а не одной записи).
func (b *BatchBuffer) Snapshot() []BufferedRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]BufferedRecord, len(b.records))
	copy(out, b.records)
	return out
}

func (b *BatchBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

func (b *BatchBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.records = nil
}

// MaxOffsets — последний (наибольший) оффсет на партицию в снапшоте, +1
// (Kafka commit семантика — "committed offset" это оффсет СЛЕДУЮЩЕЙ записи
// для чтения, не последней обработанной).
func MaxOffsets(records []BufferedRecord) map[int32]int64 {
	out := make(map[int32]int64)
	for _, r := range records {
		if current, ok := out[r.Partition]; !ok || r.Offset+1 > current {
			out[r.Partition] = r.Offset + 1
		}
	}
	return out
}
