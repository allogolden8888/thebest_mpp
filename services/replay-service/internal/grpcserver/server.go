// Package grpcserver реализует mpp.grpc.v1.ReplayService.RequestReplay —
// оркестрация load_dlq_record -> check_ttl -> check_idempotency ->
// check_billing_side_effect -> check_delivery_ambiguity -> republish ->
// write_audit (service_internal_methods.md §7.4, HLD §20). Вызывается
// Backoffice API (handle_replay_request).
package grpcserver

import (
	"context"
	"time"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/replay-service/internal/core"
	"mpp/replay-service/internal/kafkaio"
	"mpp/replay-service/internal/store"
)

// DlqLoader/LedgerChecker/CorrelationChecker/Publisher/AuditWriter —
// минимальные интерфейсы от store.Store/kafkaio.Publisher (тестируемость без реального Postgres/Kafka).
type DlqLoader interface {
	LoadDlqRecord(ctx context.Context, stageExecutionID string) (core.DlqRecord, error)
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

type Server struct {
	grpcv1.UnimplementedReplayServiceServer

	dlq       DlqLoader
	ledger    LedgerChecker
	dlr       CorrelationChecker
	audit     AuditWriter
	publisher *kafkaio.Publisher
}

func New(dlq DlqLoader, ledger LedgerChecker, dlr CorrelationChecker, audit AuditWriter, publisher *kafkaio.Publisher) *Server {
	return &Server{dlq: dlq, ledger: ledger, dlr: dlr, audit: audit, publisher: publisher}
}

func (s *Server) RequestReplay(ctx context.Context, req *grpcv1.RequestReplayRequest) (*grpcv1.RequestReplayResponse, error) {
	stageExecutionID := req.GetStageExecutionId()

	record, err := s.dlq.LoadDlqRecord(ctx, stageExecutionID)
	if err != nil {
		return reject("DLQ_RECORD_NOT_FOUND: " + err.Error()), nil
	}

	ttlOK := core.CheckTTL(record, time.Now()) == core.TTLValid
	if !ttlOK {
		_ = s.dlq.MarkExpired(ctx, stageExecutionID)
		s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), false, false, false, false, "EXPIRED", "")
		return reject("TTL_EXPIRED"), nil
	}

	idempotencyOK := core.CheckIdempotency(record) == core.IdempotencySafe
	if !idempotencyOK {
		s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), true, false, false, false, "ALREADY_REPLAYED", "")
		return reject("ALREADY_REPLAYED"), nil
	}

	chargeExists := false
	if record.StageName == "BILLING" {
		chargeExists, err = s.ledger.ChargeExistsInLedger(ctx, stageExecutionID)
		if err != nil {
			return reject("BILLING_CHECK_FAILED: " + err.Error()), nil
		}
	}
	billingOK := core.CheckBillingSideEffect(record, chargeExists) == core.Safe
	if !billingOK {
		s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), true, true, false, false, "BILLING_SIDE_EFFECT_UNSAFE", "")
		return reject("BILLING_SIDE_EFFECT_UNSAFE"), nil
	}

	correlationExists := false
	if record.StageName == "DELIVERY" {
		correlationExists, err = s.dlr.DlrCorrelationExists(ctx, stageExecutionID)
		if err != nil {
			return reject("DELIVERY_CHECK_FAILED: " + err.Error()), nil
		}
	}
	deliveryOK := core.CheckDeliveryAmbiguity(record, correlationExists) == core.Safe
	if !deliveryOK {
		s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), true, true, true, false, "DELIVERY_AMBIGUITY_UNSAFE", "")
		return reject("DELIVERY_AMBIGUITY_UNSAFE"), nil
	}

	cmd, err := kafkaio.DecodeOriginalCommand(record.OriginalCommand)
	if err != nil {
		return reject("DECODE_ORIGINAL_COMMAND_FAILED: " + err.Error()), nil
	}
	topic, err := kafkaio.StageTopic(cmd.GetStageName())
	if err != nil {
		return reject("UNKNOWN_STAGE: " + err.Error()), nil
	}

	if err := s.publisher.Republish(ctx, cmd); err != nil {
		s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), true, true, true, true, "REPUBLISH_FAILED", topic)
		return reject("REPUBLISH_FAILED: " + err.Error()), nil
	}

	_ = s.dlq.MarkReplayed(ctx, stageExecutionID)
	s.writeAudit(ctx, stageExecutionID, req.GetRequestedBy(), true, true, true, true, "REPUBLISHED", topic)

	return &grpcv1.RequestReplayResponse{Accepted: true}, nil
}

func (s *Server) writeAudit(ctx context.Context, stageExecutionID, requestedBy string, ttl, idempotency, billing, delivery bool, outcome, topic string) {
	_ = s.audit.WriteAudit(ctx, stageExecutionID, requestedBy, store.ChecksPassed{
		TTL: ttl, Idempotency: idempotency, BillingSideEffect: billing, DeliveryAmbiguity: delivery,
	}, outcome, topic)
}

func reject(reason string) *grpcv1.RequestReplayResponse {
	return &grpcv1.RequestReplayResponse{Accepted: false, RejectionReason: reason}
}