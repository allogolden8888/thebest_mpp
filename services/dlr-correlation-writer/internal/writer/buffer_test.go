package writer

import (
	"testing"
	"time"
)

func rec(partition int32, offset int64) BufferedRecord {
	return BufferedRecord{
		Correlation: CorrelationRecord{
			OperatorID:       "beeline",
			SmscMessageID:    "smsc-1",
			SegmentID:        1,
			MessageID:        "m1",
			StageExecutionID: "se1",
			SubmittedAt:      time.Now(),
			ExpiresAt:        time.Now().Add(48 * time.Hour),
		},
		Partition: partition,
		Offset:    offset,
	}
}

func TestAddReturnsFalseBelowMaxSize(t *testing.T) {
	buf := NewBatchBuffer(3)
	if buf.Add(rec(0, 1)) {
		t.Fatal("не должно сигнализировать flush ниже maxSize")
	}
	if buf.Add(rec(0, 2)) {
		t.Fatal("не должно сигнализировать flush ниже maxSize")
	}
}

func TestAddReturnsTrueAtMaxSize(t *testing.T) {
	buf := NewBatchBuffer(2)
	buf.Add(rec(0, 1))
	if !buf.Add(rec(0, 2)) {
		t.Fatal("должно сигнализировать flush при достижении maxSize")
	}
}

func TestSnapshotDoesNotClearBuffer(t *testing.T) {
	buf := NewBatchBuffer(10)
	buf.Add(rec(0, 1))
	snapshot := buf.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("ожидали 1 запись в снапшоте, получили %d", len(snapshot))
	}
	if buf.Len() != 1 {
		t.Fatalf("Snapshot не должен очищать буфер, Len() = %d", buf.Len())
	}
}

func TestClearEmptiesBuffer(t *testing.T) {
	buf := NewBatchBuffer(10)
	buf.Add(rec(0, 1))
	buf.Clear()
	if buf.Len() != 0 {
		t.Fatalf("ожидали пустой буфер после Clear(), Len() = %d", buf.Len())
	}
}

func TestFailedFlushLeavesBufferIntact(t *testing.T) {
	// Прямая проверка задокументированного инварианта: Snapshot() не чистит
	// буфер именно чтобы неудачный flush не терял навсегда буферизованные
	// записи — вызывающая сторона решает, когда Clear(), не эта структура.
	buf := NewBatchBuffer(10)
	buf.Add(rec(0, 1))
	buf.Add(rec(0, 2))
	_ = buf.Snapshot() // имитация неудачной попытки flush — снапшот взят, но не применён
	if buf.Len() != 2 {
		t.Fatalf("буфер должен остаться нетронутым после Snapshot() без Clear(), Len() = %d", buf.Len())
	}
}

func TestMaxOffsetsIsPerPartitionExclusiveUpperBound(t *testing.T) {
	records := []BufferedRecord{rec(0, 5), rec(0, 7), rec(1, 2)}
	offsets := MaxOffsets(records)
	if offsets[0] != 8 {
		t.Fatalf("partition 0: ожидали 8 (max offset 7 + 1), получили %d", offsets[0])
	}
	if offsets[1] != 3 {
		t.Fatalf("partition 1: ожидали 3 (max offset 2 + 1), получили %d", offsets[1])
	}
}

func TestMaxOffsetsEmptyForEmptyInput(t *testing.T) {
	offsets := MaxOffsets(nil)
	if len(offsets) != 0 {
		t.Fatalf("ожидали пустую карту, получили %v", offsets)
	}
}
