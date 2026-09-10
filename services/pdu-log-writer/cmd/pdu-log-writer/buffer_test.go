package main

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/pdu-log-writer/internal/core"
)

// TestBufferRestorePreservesDataOnFailedWrite — тот же CODE_REVIEW.md класс
// исправления, что analytics-writer: буфер не должен терять данные при
// сбое записи, drain()+restore() — контракт, которым пользуется flush().
func TestBufferRestorePreservesDataOnFailedWrite(t *testing.T) {
	buf := &buffer{}
	buf.append(core.PduLogRecord{MessageID: "m1"}, &kgo.Record{Topic: "operator.pdu.log"})

	rows, records := buf.drain()
	if buf.len() != 0 {
		t.Fatalf("после drain буфер должен быть пуст")
	}

	buf.restore(rows, records)

	if buf.len() != 1 {
		t.Fatalf("после restore ожидали 1 строку, получили %d", buf.len())
	}
	if buf.rows[0].MessageID != "m1" {
		t.Fatalf("данные не восстановились корректно: %+v", buf.rows)
	}
	if len(buf.records) != 1 {
		t.Fatalf("records (для будущего commit) не восстановились корректно")
	}
}

func TestBufferRestorePrependsBeforeNewlyAccumulatedData(t *testing.T) {
	buf := &buffer{}
	buf.append(core.PduLogRecord{MessageID: "old"}, &kgo.Record{})

	rows, records := buf.drain()

	// Новые данные пришли, пока "запись" была в процессе.
	buf.append(core.PduLogRecord{MessageID: "new"}, &kgo.Record{})

	buf.restore(rows, records)

	if buf.len() != 2 {
		t.Fatalf("ожидали 2 строки после restore+new, получили %d", buf.len())
	}
	if buf.rows[0].MessageID != "old" || buf.rows[1].MessageID != "new" {
		t.Fatalf("неверный порядок после restore: %+v", buf.rows)
	}
}

func TestBufferEmptyInitially(t *testing.T) {
	buf := &buffer{}
	if buf.len() != 0 {
		t.Fatalf("новый буфер должен быть пуст")
	}
}
