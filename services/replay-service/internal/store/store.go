// Package store — load_dlq_record + вспомогательные проверки + write_audit
// (service_internal_methods.md §7.4), pgx против PostgreSQL
// (migrations/V006__dlq_record.sql, V007__replay_audit.sql,
// V008__billing_ledger.sql, V009__dlr_correlation.sql).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/replay-service/internal/core"

	commonv1 "mpp/platformcontracts/common/v1"
	"google.golang.org/protobuf/proto"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// LoadDlqRecord — load_dlq_record: stage_execution_id -> DlqRecord (+ message_ttl из original_command).
// Только для read-only просмотра — сам replay-путь использует
// ClaimForReplay (см. ниже), не этот метод (CODE_REVIEW.md CRITICAL
// finding #2 — TOCTOU-гонка).
func (s *Store) LoadDlqRecord(ctx context.Context, stageExecutionID string) (core.DlqRecord, error) {
	var (
		messageID, stageName, reasonCode, replayStatus string
		errorDetail                                     *string // nullable, migrations/V006__dlq_record.sql
		attempt                                          int32
		originalCommand                                  []byte
		createdAt                                        time.Time
	)
	err := s.pool.QueryRow(ctx, `
		SELECT message_id, stage_name, attempt, original_command, reason_code, error_detail, created_at, replay_status
		FROM messaging.dlq_record
		WHERE stage_execution_id = $1
	`, stageExecutionID).Scan(&messageID, &stageName, &attempt, &originalCommand, &reasonCode, &errorDetail, &createdAt, &replayStatus)
	if err != nil {
		return core.DlqRecord{}, fmt.Errorf("load_dlq_record: %w", err)
	}

	record := core.DlqRecord{
		StageExecutionID: stageExecutionID,
		MessageID:        messageID,
		StageName:        stageName,
		Attempt:          attempt,
		OriginalCommand:  originalCommand,
		ReasonCode:       reasonCode,
		CreatedAt:        createdAt,
		ReplayStatus:     replayStatus,
	}
	if errorDetail != nil {
		record.ErrorDetail = *errorDetail
	}

	var cmd commonv1.StageExecuteCommand
	if err := proto.Unmarshal(originalCommand, &cmd); err == nil && cmd.GetMessageTtl() != nil {
		record.MessageTTL = cmd.GetMessageTtl().AsTime()
	}
	return record, nil
}

// ClaimForReplay — CODE_REVIEW.md CRITICAL finding #2: раньше
// load_dlq_record (SELECT) и MarkReplayed (UPDATE, только после успешного
// republish) были два отдельных шага без единого атомарного "claim" между
// ними. Два конкурентных RequestReplay для одного stage_execution_id
// (двойной клик в Backoffice UI, gRPC-ретрай после таймаута, два Backoffice
// API пода, проксирующих одно действие) оба читали replay_status='pending',
// оба проходили check_idempotency (единственную защиту от повторной
// обработки), и оба республиковали — DELIVERY-стадийная запись могла уйти
// абоненту физически дважды, BILLING-стадийная — списаться дважды.
//
// Атомарный UPDATE ... WHERE replay_status='pending' ... RETURNING —
// только ОДИН конкурентный вызов может успешно перевести запись
// pending -> in_progress; остальные видят 0 обновлённых строк и получают
// claimed=false с текущим (уже не pending) статусом, из которого
// вызывающий формирует точную причину отказа (уже реплеена/просрочена/
// реплеится прямо сейчас), не проходя дальше ни к каким safety-проверкам.
func (s *Store) ClaimForReplay(ctx context.Context, stageExecutionID string) (record core.DlqRecord, claimed bool, err error) {
	var (
		messageID, stageName, reasonCode string
		errorDetail                       *string
		attempt                           int32
		originalCommand                   []byte
		createdAt                         time.Time
	)
	scanErr := s.pool.QueryRow(ctx, `
		UPDATE messaging.dlq_record
		SET replay_status = 'in_progress'
		WHERE stage_execution_id = $1 AND replay_status = 'pending'
		RETURNING message_id, stage_name, attempt, original_command, reason_code, error_detail, created_at
	`, stageExecutionID).Scan(&messageID, &stageName, &attempt, &originalCommand, &reasonCode, &errorDetail, &createdAt)

	if scanErr == nil {
		record = core.DlqRecord{
			StageExecutionID: stageExecutionID,
			MessageID:        messageID,
			StageName:        stageName,
			Attempt:          attempt,
			OriginalCommand:  originalCommand,
			ReasonCode:       reasonCode,
			CreatedAt:        createdAt,
			ReplayStatus:     "in_progress",
		}
		if errorDetail != nil {
			record.ErrorDetail = *errorDetail
		}
		var cmd commonv1.StageExecuteCommand
		if err := proto.Unmarshal(originalCommand, &cmd); err == nil && cmd.GetMessageTtl() != nil {
			record.MessageTTL = cmd.GetMessageTtl().AsTime()
		}
		return record, true, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return core.DlqRecord{}, false, fmt.Errorf("claim_for_replay: %w", scanErr)
	}

	// 0 строк обновлено — либо записи не существует, либо она не в
	// 'pending' (уже in_progress/replayed/expired). Читаем текущий статус
	// отдельно, чтобы вызывающий мог вернуть точную причину, а не общий
	// NOT_FOUND для обоих случаев.
	var currentStatus string
	lookupErr := s.pool.QueryRow(ctx, `SELECT replay_status FROM messaging.dlq_record WHERE stage_execution_id = $1`, stageExecutionID).Scan(&currentStatus)
	if lookupErr != nil {
		return core.DlqRecord{}, false, fmt.Errorf("claim_for_replay: запись не найдена: %w", lookupErr)
	}
	return core.DlqRecord{StageExecutionID: stageExecutionID, ReplayStatus: currentStatus}, false, nil
}

// ReleaseClaim — откатывает in_progress обратно в pending: используется,
// когда safety-проверка (billing/delivery) провалилась ПОСЛЕ claim — та же
// семантика, что и раньше (запись остаётся pending, доступна для будущей
// повторной попытки, если условие перестанет быть unsafe), не "успешно
// обработана".
func (s *Store) ReleaseClaim(ctx context.Context, stageExecutionID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE messaging.dlq_record SET replay_status = 'pending' WHERE stage_execution_id = $1 AND replay_status = 'in_progress'`, stageExecutionID)
	if err != nil {
		return fmt.Errorf("release_claim: %w", err)
	}
	return nil
}

// ChargeExistsInLedger — вход check_billing_side_effect.
func (s *Store) ChargeExistsInLedger(ctx context.Context, chargeID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM billing.billing_ledger WHERE charge_id = $1)`, chargeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check billing ledger: %w", err)
	}
	return exists, nil
}

// DlrCorrelationExists — вход check_delivery_ambiguity.
func (s *Store) DlrCorrelationExists(ctx context.Context, stageExecutionID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM dlr.dlr_correlation WHERE stage_execution_id = $1)`, stageExecutionID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check dlr_correlation: %w", err)
	}
	return exists, nil
}

// MarkReplayed — обновление replay_status после успешного republish.
func (s *Store) MarkReplayed(ctx context.Context, stageExecutionID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE messaging.dlq_record SET replay_status = 'replayed' WHERE stage_execution_id = $1`, stageExecutionID)
	if err != nil {
		return fmt.Errorf("mark_replayed: %w", err)
	}
	return nil
}

// MarkExpired — обновление replay_status при check_ttl=Expired.
func (s *Store) MarkExpired(ctx context.Context, stageExecutionID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE messaging.dlq_record SET replay_status = 'expired' WHERE stage_execution_id = $1`, stageExecutionID)
	if err != nil {
		return fmt.Errorf("mark_expired: %w", err)
	}
	return nil
}

// ChecksPassed — итог всех 4 проверок для write_audit (JSONB checks_passed, migrations/V007).
type ChecksPassed struct {
	TTL                bool `json:"ttl"`
	Idempotency        bool `json:"idempotency"`
	BillingSideEffect  bool `json:"billing_side_effect"`
	DeliveryAmbiguity  bool `json:"delivery_ambiguity"`
}

// WriteAudit — write_audit: итог операции -> messaging.replay_audit.
func (s *Store) WriteAudit(ctx context.Context, stageExecutionID, requestedBy string, checks ChecksPassed, outcome, targetTopic string) error {
	checksJSON, err := json.Marshal(checks)
	if err != nil {
		return fmt.Errorf("marshal checks_passed: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO messaging.replay_audit (stage_execution_id, requested_by, checks_passed, outcome, target_topic)
		VALUES ($1, $2, $3, $4, $5)
	`, stageExecutionID, requestedBy, checksJSON, outcome, targetTopic)
	if err != nil {
		return fmt.Errorf("write_audit: %w", err)
	}
	return nil
}