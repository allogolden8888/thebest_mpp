package main

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/lifecycle-writer/internal/core"
)

// TestBufferRestorePreservesDataOnFailedWrite — CODE_REVIEW.md HIGH
// finding: раньше буфер очищался ДО подтверждения успешной записи в
// Postgres, поэтому сбой записи означал безвозвратную потерю уже
// накопленных строк. drain()+restore() — контракт, которым flush()
// пользуется для "не терять данные при ошибке": drain снимает текущее
// содержимое, restore возвращает его обратно, если запись не удалась.
func TestBufferRestorePreservesDataOnFailedWrite(t *testing.T) {
	buf := &buffer{}
	buf.inserts = append(buf.inserts, core.ReadModelRow{MessageID: "m1"})
	buf.history = append(buf.history, core.LifecycleHistoryRow{MessageID: "m1"})
	buf.records = append(buf.records, &kgo.Record{Topic: "incoming.messages"})

	inserts, updates, history, dlq, records := buf.drain()
	if buf.empty() != true {
		t.Fatalf("после drain буфер должен быть пуст")
	}

	// Симулируем сбой записи в Postgres — restore должен вернуть всё как
	// было, ничего не потеряв.
	buf.restore(inserts, updates, history, dlq, records)

	if buf.empty() {
		t.Fatalf("после restore буфер не должен быть пуст — данные должны были вернуться")
	}
	if len(buf.inserts) != 1 || buf.inserts[0].MessageID != "m1" {
		t.Fatalf("inserts не восстановились корректно: %+v", buf.inserts)
	}
	if len(buf.history) != 1 {
		t.Fatalf("history не восстановилась корректно: %+v", buf.history)
	}
	if len(buf.records) != 1 {
		t.Fatalf("records (для будущего commit) не восстановились корректно: %+v", buf.records)
	}
}

// TestBufferRestorePrependsBeforeNewlyAccumulatedData — данные,
// накопленные ПОКА шла (неудачная) попытка записи, не должны быть
// потеряны или переупорядочены так, чтобы восстановленные данные
// оказались позже новых.
func TestBufferRestorePrependsBeforeNewlyAccumulatedData(t *testing.T) {
	buf := &buffer{}
	buf.inserts = append(buf.inserts, core.ReadModelRow{MessageID: "old"})

	inserts, updates, history, dlq, records := buf.drain()

	// Новые данные пришли, пока "запись" была в процессе.
	buf.inserts = append(buf.inserts, core.ReadModelRow{MessageID: "new"})

	buf.restore(inserts, updates, history, dlq, records)

	if len(buf.inserts) != 2 {
		t.Fatalf("ожидали 2 строки после restore+new, получили %d", len(buf.inserts))
	}
	if buf.inserts[0].MessageID != "old" || buf.inserts[1].MessageID != "new" {
		t.Fatalf("неверный порядок после restore: %+v", buf.inserts)
	}
}

func TestBufferEmptyInitially(t *testing.T) {
	buf := &buffer{}
	if !buf.empty() {
		t.Fatalf("новый буфер должен быть пуст")
	}
}
