// Package store — batch_buffer + flush_batch (service_internal_methods.md
// §6.1): batch UPSERT/INSERT (COPY) в PostgreSQL.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/lifecycle-writer/internal/core"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InsertReadModel — первая строка read model (INSERT, ON CONFLICT DO NOTHING
// — IncomingMessage не должен переопределять уже существующую строку, если
// consumer перечитывает после рестарта). Оставлен для read-only/одиночных
// вызовов (например тестов) — реальный consume-путь использует
// BatchInsertReadModel, см. ниже.
func (s *Store) InsertReadModel(ctx context.Context, row core.ReadModelRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO messaging.message_read_model
			(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at, sandbox)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)
		ON CONFLICT (message_id) DO NOTHING
	`, row.MessageID, row.PartnerID, row.ApplicationID, row.TraceID, row.PipelineID, row.PipelineVersion,
		row.CurrentStatus, row.Terminal, row.Timestamp, row.Sandbox)
	if err != nil {
		return fmt.Errorf("insert message_read_model: %w", err)
	}
	return nil
}

// BatchInsertReadModel — batch-версия InsertReadModel (CODE_REVIEW.md
// MEDIUM finding: read model писался синхронно по одной строке за раз,
// не батчем вместе с history/dlq, как описывает service_internal_methods.md
// §6.1 — риск отставания consumer'а под нагрузкой).
func (s *Store) BatchInsertReadModel(ctx context.Context, rows []core.ReadModelRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.message_read_model
				(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at, sandbox)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)
			ON CONFLICT (message_id) DO NOTHING
		`, row.MessageID, row.PartnerID, row.ApplicationID, row.TraceID, row.PipelineID, row.PipelineVersion,
			row.CurrentStatus, row.Terminal, row.Timestamp, row.Sandbox)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert message_read_model: %w", err)
		}
	}
	return nil
}

// UpdateReadModel — обновление current_status/terminal по message.lifecycle.
// Оставлен для read-only/одиночных вызовов — реальный consume-путь
// использует BatchUpdateReadModel, см. ниже.
//
// lifecycle_version — CODE_REVIEW.md MEDIUM finding: раньше обновление
// было безусловным, без защиты от переупорядоченной/повторной доставки —
// партиционный rebalance, редоставивший старый SUBMITTED уже ПОСЛЕ того,
// как был применён более новый DELIVERED, откатывал партнёр-facing read
// model назад. `AND lifecycle_version < $5` — обновление применяется,
// только если оно новее уже применённого (миграция V019).
func (s *Store) UpdateReadModel(ctx context.Context, update core.ReadModelUpdate) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE messaging.message_read_model
		SET current_status = $2, terminal = $3, updated_at = $4, lifecycle_version = $5
		WHERE message_id = $1 AND lifecycle_version < $5
	`, update.MessageID, update.CurrentStatus, update.Terminal, update.UpdatedAt, update.LifecycleVersion)
	if err != nil {
		return fmt.Errorf("update message_read_model: %w", err)
	}
	return nil
}

// BatchUpdateReadModel — batch-версия UpdateReadModel, та же
// lifecycle_version-защита от out-of-order/дубликатов.
func (s *Store) BatchUpdateReadModel(ctx context.Context, updates []core.ReadModelUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, update := range updates {
		batch.Queue(`
			UPDATE messaging.message_read_model
			SET current_status = $2, terminal = $3, updated_at = $4, lifecycle_version = $5
			WHERE message_id = $1 AND lifecycle_version < $5
		`, update.MessageID, update.CurrentStatus, update.Terminal, update.UpdatedAt, update.LifecycleVersion)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range updates {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch update message_read_model: %w", err)
		}
	}
	return nil
}

// BatchInsertLifecycleHistory — COPY-стиль batch insert через pgx.Batch.
func (s *Store) BatchInsertLifecycleHistory(ctx context.Context, rows []core.LifecycleHistoryRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.message_lifecycle_history
				(message_id, lifecycle_version, status, event_id, occurred_at, source)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING
		`, row.MessageID, row.LifecycleVersion, row.Status, row.EventID, row.OccurredAt, row.Source)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert message_lifecycle_history: %w", err)
		}
	}
	return nil
}

// BatchInsertDlq — batch insert dlq_record.
func (s *Store) BatchInsertDlq(ctx context.Context, rows []core.DlqRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.dlq_record
				(stage_execution_id, message_id, stage_name, attempt, original_command, reason_code, error_detail, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (stage_execution_id) DO NOTHING
		`, row.StageExecutionID, row.MessageID, row.StageName, row.Attempt, row.OriginalCommand,
			row.ReasonCode, row.ErrorDetail, row.CreatedAt)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert dlq_record: %w", err)
		}
	}
	return nil
}