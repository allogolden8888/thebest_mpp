// Package grpcserver реализует mpp.grpc.v1.ReplayService.RequestReplay —
// оркестрация claim_for_replay -> check_ttl -> check_billing_side_effect ->
// check_delivery_ambiguity -> check_execution_control -> republish ->
// write_audit (service_internal_methods.md §7.4, HLD §20). Вызывается
// Backoffice API (handle_replay_request).
//
// **Авторизация — CODE_REVIEW.md CRITICAL finding #1, что реально
// исправлено и что нет.** Раньше здесь не было вообще никакой проверки —
// ни требования непустого requested_by, ни какой-либо связи между
// вызывающим и правом на replay. requested_by теперь обязателен (пустой —
// отказ до всех остальных шагов, см. RequestReplay). Транспорт: сервис
// зарегистрирован в k8s/generate_manifests.py и деплоится в namespace
// `mpp`, целиком помеченный `istio-injection: enabled` под STRICT
// `PeerAuthentication` (infra/istio/peer-authentication-strict.yaml) —
// то есть mTLS на уровне транспорта реально обеспечен service mesh'ем, не
// кодом приложения (тот же принцип, что и в других gRPC-серверах этой
// сессии). НО: mTLS сам по себе доказывает только "вызывающий — валидный
// под в мешe", не "вызывающий имеет право звать RequestReplay" — в
// infra/istio/ нет ни одного AuthorizationPolicy, ограничивающего, чьи
// identity могут звать replay-service (только Backoffice API должен
// иметь право). Это реальный, непокрытый остаток finding #1 — но
// исправление лежит в infra/istio/ (Istio AuthorizationPolicy,
// scoped на ServiceAccount backoffice-api), не в этом коде, а infra/ —
// не в периметре ответственности этой сессии (development_plan.md
// "Координация"). Задокументировано честно, не изобретаем
// приложенческий credential-механизм, конкурирующий с mesh и нигде не
// специфицированный.
package grpcserver

import (
	"context"
	"fmt"
	"log"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/replay-service/internal/core"
	"mpp/replay-service/internal/kafkaio"
	"mpp/replay-service/internal/store"
)

// DlqClaimer — минимальный интерфейс от store.Store, покрывающий атомарный
// claim_for_replay (CODE_REVIEW.md CRITICAL finding #2 — TOCTOU-гонка
// двойного replay) и связанные переходы статуса.
type DlqClaimer interface {
	ClaimForReplay(ctx context.Context, stageExecutionID string) (record core.DlqRecord, claimed bool, err error)
	ReleaseClaim(ctx context.Context, stageExecutionID string) error
	MarkReplayed(ctx context.Context, stageExecutionID string) error
	MarkExpired(ctx context.Context, stageExecutionID string) error
}

type LedgerChecker interface {
	ChargeExistsInLedger(ctx context.Context, chargeID string) (bool, error)
}

type CorrelationChecker interface {
	DlrCorrelationExists(ctx context.Context, stageExecutionID string) (bool, error)
}

type AuditWriter interface {
	WriteAudit(ctx context.Context, stageExecutionID, requestedBy string, checks store.ChecksPassed, outcome, targetTopic string) error
}

// ControlChecker — check_execution_control (CODE_REVIEW.md HIGH finding
// #4): раньше Republish публиковал прошедшую все прочие проверки команду
// напрямую на любой из шести stage.* топиков без единой проверки
// execution.control, полностью игнорируя GLOBAL/PARTNER_STAGE-паузы,
// поставленные именно для остановки этой активности во время инцидента.
type ControlChecker interface {
	IsPaused(stageName string) bool
}

// Republisher — минимальный интерфейс от kafkaio.Publisher (тестируемость
// без реального Kafka-брокера — раньше поле было конкретным типом
// *kafkaio.Publisher, поэтому server.go orchestration был вообще
// нетестируем без live-брокера, CODE_REVIEW.md test-quality finding).
type Republisher interface {
	Republish(ctx context.Context, cmd *commonv1.StageExecuteCommand) error
}

type Server struct {
	grpcv1.UnimplementedReplayServiceServer

	dlq       DlqClaimer
	ledger    LedgerChecker
	dlr       CorrelationChecker
	audit     AuditWriter
	control   ControlChecker
	publisher Republisher
}

func New(dlq DlqClaimer, ledger LedgerChecker, dlr CorrelationChecker, audit AuditWriter, control ControlChecker, publisher Republisher) *Server {
	return &Server{dlq: dlq, ledger: ledger, dlr: dlr, audit: audit, control: control, publisher: publisher}
}

func (s *Server) RequestReplay(ctx context.Context, req *grpcv1.RequestReplayRequest) (*grpcv1.RequestReplayResponse, error) {
	stageExecutionID := req.GetStageExecutionId()
	requestedBy := req.GetRequestedBy()

	// CODE_REVIEW.md CRITICAL finding #1 (частично, см. package doc) —
	// requested_by раньше не валидировался вообще, свободная строка,
	// используемая только как метка в аудите. Минимум, который можно
	// требовать в коде без выдумывания несуществующего auth-механизма:
	// не пустая строка, иначе отказ до любых DB/Kafka операций.
	if requestedBy == "" {
		return reject("REQUESTED_BY_REQUIRED"), nil
	}

	record, claimed, err := s.dlq.ClaimForReplay(ctx, stageExecutionID)
	if err != nil {
		return reject("DLQ_RECORD_NOT_FOUND: " + err.Error()), nil
	}
	if !claimed {
		reason := claimRejectionReason(record.ReplayStatus)
		s.writeAudit(ctx, stageExecutionID, requestedBy, false, false, false, false, reason, "")
		return reject(reason), nil
	}

	ttlOK := core.CheckTTL(record, time.Now()) == core.TTLValid
	if !ttlOK {
		_ = s.dlq.MarkExpired(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, requestedBy, false, false, false, false, "EXPIRED", "")
		return reject("TTL_EXPIRED"), nil
	}

	chargeExists := false
	if record.StageName == "BILLING" {
		chargeExists, err = s.ledger.ChargeExistsInLedger(ctx, stageExecutionID)
		if err != nil {
			s.releaseClaim(ctx, stageExecutionID)
			return reject("BILLING_CHECK_FAILED: " + err.Error()), nil
		}
	}
	billingOK := core.CheckBillingSideEffect(record, chargeExists) == core.Safe
	if !billingOK {
		s.releaseClaim(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, requestedBy, true, true, false, false, "BILLING_SIDE_EFFECT_UNSAFE", "")
		return reject("BILLING_SIDE_EFFECT_UNSAFE"), nil
	}

	correlationExists := false
	if record.StageName == "DELIVERY" {
		correlationExists, err = s.dlr.DlrCorrelationExists(ctx, stageExecutionID)
		if err != nil {
			s.releaseClaim(ctx, stageExecutionID)
			return reject("DELIVERY_CHECK_FAILED: " + err.Error()), nil
		}
	}
	deliveryOK := core.CheckDeliveryAmbiguity(record, correlationExists) == core.Safe
	if !deliveryOK {
		s.releaseClaim(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, requestedBy, true, true, true, false, "DELIVERY_AMBIGUITY_UNSAFE", "")
		return reject("DELIVERY_AMBIGUITY_UNSAFE"), nil
	}

	if s.control != nil && s.control.IsPaused(record.StageName) {
		s.releaseClaim(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, requestedBy, true, true, true, false, "STAGE_PAUSED", "")
		return reject("STAGE_PAUSED"), nil
	}

	cmd, err := kafkaio.DecodeOriginalCommand(record.OriginalCommand)
	if err != nil {
		s.releaseClaim(ctx, stageExecutionID)
		return reject("DECODE_ORIGINAL_COMMAND_FAILED: " + err.Error()), nil
	}
	topic, err := kafkaio.StageTopic(cmd.GetStageName())
	if err != nil {
		s.releaseClaim(ctx, stageExecutionID)
		return reject("UNKNOWN_STAGE: " + err.Error()), nil
	}

	if err := s.publisher.Republish(ctx, cmd); err != nil {
		s.releaseClaim(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, requestedBy, true, true, true, true, "REPUBLISH_FAILED", topic)
		return reject("REPUBLISH_FAILED: " + err.Error()), nil
	}

	// CODE_REVIEW.md HIGH finding #3: раньше эта ошибка отбрасывалась
	// (`_ = s.dlq.MarkReplayed(...)`), и записи, застрявшей в 'pending',
	// молча открывался повторный replay без единой гонки. Теперь запись
	// уже 'in_progress' (claim выше), так что при сбое MarkReplayed она
	// остаётся 'in_progress' навсегда — fail-safe (требует ручного
	// вмешательства), не fail-open (тихий повторный replay). Сообщение
	// уже реально ушло в Kafka, поэтому вызывающему всё равно возвращаем
	// Accepted=true — врать об обратном было бы хуже.
	outcome := "REPUBLISHED"
	if err := s.dlq.MarkReplayed(ctx, stageExecutionID); err != nil {
		outcome = "REPUBLISHED_BUT_MARK_FAILED"
		log.Printf("mark_replayed failed after successful republish for %s: %v — запись останется in_progress, требуется ручная проверка", stageExecutionID, err)
	}
	s.writeAudit(ctx, stageExecutionID, requestedBy, true, true, true, true, outcome, topic)

	return &grpcv1.RequestReplayResponse{Accepted: true}, nil
}

// claimRejectionReason — сопоставляет текущий (не-pending) replay_status с
// понятной причиной отказа для вызывающего.
func claimRejectionReason(currentStatus string) string {
	switch currentStatus {
	case "replayed":
		return "ALREADY_REPLAYED"
	case "expired":
		return "TTL_EXPIRED"
	case "in_progress":
		return "REPLAY_IN_PROGRESS"
	default:
		return fmt.Sprintf("NOT_PENDING: %s", currentStatus)
	}
}

func (s *Server) releaseClaim(ctx context.Context, stageExecutionID string) {
	if err := s.dlq.ReleaseClaim(ctx, stageExecutionID); err != nil {
		log.Printf("release_claim failed for %s: %v — запись может остаться зависшей в in_progress до ручного вмешательства", stageExecutionID, err)
	}
}

func (s *Server) writeAudit(ctx context.Context, stageExecutionID, requestedBy string, ttl, idempotency, billing, delivery bool, outcome, topic string) {
	_ = s.audit.WriteAudit(ctx, stageExecutionID, requestedBy, store.ChecksPassed{
		TTL: ttl, Idempotency: idempotency, BillingSideEffect: billing, DeliveryAmbiguity: delivery,
	}, outcome, topic)
}

func reject(reason string) *grpcv1.RequestReplayResponse {
	return &grpcv1.RequestReplayResponse{Accepted: false, RejectionReason: reason}
}
