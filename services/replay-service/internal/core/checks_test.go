package core

import (
	"testing"
	"time"
)

func TestCheckTTLValidBeforeExpiry(t *testing.T) {
	now := time.Now()
	record := DlqRecord{MessageTTL: now.Add(time.Hour)}
	if CheckTTL(record, now) != TTLValid {
		t.Fatalf("ожидали Valid до истечения message_ttl")
	}
}

func TestCheckTTLExpiredAfterExpiry(t *testing.T) {
	now := time.Now()
	record := DlqRecord{MessageTTL: now.Add(-time.Hour)}
	if CheckTTL(record, now) != TTLExpired {
		t.Fatalf("ожидали Expired после истечения message_ttl")
	}
}

func TestCheckTTLValidWhenTTLUnset(t *testing.T) {
	record := DlqRecord{}
	if CheckTTL(record, time.Now()) != TTLValid {
		t.Fatalf("отсутствующий message_ttl не должен блокировать replay")
	}
}

func TestCheckIdempotencySafeWhenPending(t *testing.T) {
	record := DlqRecord{ReplayStatus: "pending"}
	if CheckIdempotency(record) != IdempotencySafe {
		t.Fatalf("ожидали Safe для pending записи")
	}
}

func TestCheckIdempotencyUnsafeWhenAlreadyReplayed(t *testing.T) {
	record := DlqRecord{ReplayStatus: "replayed"}
	if CheckIdempotency(record) != IdempotencyUnsafe {
		t.Fatalf("ожидали Unsafe для уже реплеенной записи")
	}
}

func TestCheckBillingSideEffectSafeForNonBillingStage(t *testing.T) {
	record := DlqRecord{StageName: "DELIVERY"}
	if CheckBillingSideEffect(record, true) != Safe {
		t.Fatalf("проверка billing side effect не должна применяться к не-Billing стадии")
	}
}

func TestCheckBillingSideEffectUnsafeWhenChargeExists(t *testing.T) {
	record := DlqRecord{StageName: "BILLING"}
	if CheckBillingSideEffect(record, true) != Unsafe {
		t.Fatalf("ожидали Unsafe, если charge_id уже в ledger")
	}
}

func TestCheckBillingSideEffectSafeWhenNoChargeYet(t *testing.T) {
	record := DlqRecord{StageName: "BILLING"}
	if CheckBillingSideEffect(record, false) != Safe {
		t.Fatalf("ожидали Safe, если charge_id ещё не в ledger")
	}
}

func TestCheckDeliveryAmbiguitySafeForNonDeliveryStage(t *testing.T) {
	record := DlqRecord{StageName: "BILLING"}
	if CheckDeliveryAmbiguity(record, true) != Safe {
		t.Fatalf("проверка delivery ambiguity не должна применяться к не-Delivery стадии")
	}
}

func TestCheckDeliveryAmbiguityUnsafeWhenCorrelationExists(t *testing.T) {
	record := DlqRecord{StageName: "DELIVERY"}
	if CheckDeliveryAmbiguity(record, true) != Unsafe {
		t.Fatalf("ожидали Unsafe, если submit уже дошёл до оператора (dlr_correlation существует)")
	}
}

func TestCheckDeliveryAmbiguitySafeWhenNoCorrelation(t *testing.T) {
	record := DlqRecord{StageName: "DELIVERY"}
	if CheckDeliveryAmbiguity(record, false) != Safe {
		t.Fatalf("ожидали Safe, если корреляции ещё нет")
	}
}