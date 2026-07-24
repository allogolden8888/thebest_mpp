// Package dlr — on_raw_dlr / normalize_operator_status
// (service_internal_methods.md §4.2). Чистые функции, тестируются без сети.
package dlr

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// DecodeOperatorDlr — разбор operator.dlr / operator.dlr.dlq (тот же
// формат, `platform_contracts.md`: "operator.dlr / operator.dlr.unresolved
// / operator.dlr.dlq → mpp.events.v1.OperatorDlr"). smsc_message_id
// обязателен здесь (в отличие от OperatorSubmitAccepted, где он
// опционален) — без него DLR физически нельзя сопоставить ни с чем.
func DecodeOperatorDlr(payload []byte) (*eventsv1.OperatorDlr, error) {
	var event eventsv1.OperatorDlr
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal OperatorDlr: %w", err)
	}
	if event.GetOperatorId() == "" {
		return nil, fmt.Errorf("OperatorDlr без operator_id")
	}
	if event.GetSmscMessageId() == "" {
		return nil, fmt.Errorf("OperatorDlr без smsc_message_id")
	}
	return &event, nil
}

// normalizedStatusByRawSMPPStat — стандартный SMPP v3.4 `stat` словарь DLR
// (§4.7.3 спецификации, не выдуманный список — те же 7 кодов используются
// буквально любым SMPP-стеком: DELIVRD/EXPIRED/DELETED/UNDELIV/ACCEPTD/
// REJECTD/UNKNOWN). Точное per-operator расширение этого словаря явно вне
// scope контракта (`operator_events.proto`'s комментарий у raw_status:
// "точный список операторских→платформенных маппингов вне scope этого
// контракта (per-operator LLD)") — здесь только базовый, протокольно
// определённый уровень, HTTP-операторы (Operator HTTP Gateway) обязаны
// нормализовать свои webhook-коды в этот же словарь до публикации в
// operator.dlr (`services_specifictaion.md` §2.3a).
var normalizedStatusByRawSMPPStat = map[string]string{
	"DELIVRD": "DELIVERED",
	"EXPIRED": "UNDELIVERABLE",
	"DELETED": "UNDELIVERABLE",
	"UNDELIV": "UNDELIVERABLE",
	"REJECTD": "UNDELIVERABLE",
	// ACCEPTD/UNKNOWN — не терминальные исходы по SMPP spec (сообщение всё
	// ещё в процессе или статус неизвестен оператору) — намеренно НЕ
	// маппятся ни в DELIVERED, ни в UNDELIVERABLE; publish_delivery_status
	// не должен получить один из этих кодов как окончательный статус.
}

// NormalizeStatus — normalize_operator_status. `recognized=false` — код не
// входит в известный словарь (ACCEPTD/UNKNOWN, либо оператор-специфичный
// код, ещё не добавленный) — вызывающая сторона решает, что делать
// (сегодня: не публиковать delivery.status с мусорным статусом).
func NormalizeStatus(rawStatus string) (normalized string, recognized bool) {
	normalized, recognized = normalizedStatusByRawSMPPStat[rawStatus]
	return normalized, recognized
}

// DeriveEventID — детерминированный, не случайный (!) идентификатор для
// кэша в internal/pending. Найдено при проектировании, не тривиально:
// `OperatorDlr` не несёт собственного event_id (сырое событие от
// оператора) — если бы этот id генерировался случайно (uuid.New()) на
// каждую обработку, at-least-once редоставка одного и того же
// operator.dlr (та же гарантия Kafka, что везде в этой сессии) породила
// бы ВТОРУЮ, независимую цепочку retry для одного и того же DLR — тот же
// класс дублирования, что offset-commit-до-подтверждения баг в других
// сервисах, только на уровне бизнес-логики, не Kafka-оффсета. Детерминированный
// хэш по (operator_id, smsc_message_id, segment_id, raw_status, received_at)
// делает повторную обработку идемпотентной: тот же DLR — тот же event_id —
// тот же ключ в pending-кэше, перезапись, не дублирование.
func DeriveEventID(d *eventsv1.OperatorDlr) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%d|%s|%d", d.GetOperatorId(), d.GetSmscMessageId(), d.GetSegmentId(), d.GetRawStatus(), d.GetReceivedAt().AsTime().UnixNano())
	return hex.EncodeToString(h.Sum(nil))[:32]
}
