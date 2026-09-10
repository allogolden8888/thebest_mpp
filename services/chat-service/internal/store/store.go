// Package store — Postgres-доступ chat-service: собственная схема
// support.chat_messages (migrations/V034__chat_messages.sql). Единственный
// писатель в эту таблицу — этот сервис (в отличие от, например,
// incident-service, который дополнительно ЧИТАЕТ чужую control.
// execution_control_audit напрямую, здесь никакого cross-schema доступа
// нет — support.chat_messages принадлежит целиком chat-service).
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ChatMessage struct {
	ID         int64
	PartnerID  string
	SenderType string
	SenderID   string
	Body       string
	CreatedAt  time.Time
	ReadAt     *time.Time
}

type ChatThread struct {
	PartnerID     string
	LastBody      string
	LastMessageAt time.Time
	UnreadCount   int64
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// Ping — используется /readyz, не запросами приложения.
func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// SendMessage — простая вставка, read_at остаётся NULL (непрочитано) до
// первого ListMessages противоположной стороны. Валидация sender_type
// (partner/admin) и непустоты body — на уровне grpcserver (см. package doc
// там), CHECK-ограничение в БД (migrations/V034) — второй, defense-in-depth
// слой, не единственный.
func (p *Postgres) SendMessage(ctx context.Context, partnerID, senderType, senderID, body string) (ChatMessage, error) {
	var m ChatMessage
	err := p.pool.QueryRow(ctx, `
		INSERT INTO support.chat_messages (partner_id, sender_type, sender_id, body)
		VALUES ($1, $2, $3, $4)
		RETURNING id, partner_id, sender_type, sender_id, body, created_at, read_at`,
		partnerID, senderType, senderID, body).
		Scan(&m.ID, &m.PartnerID, &m.SenderType, &m.SenderID, &m.Body, &m.CreatedAt, &m.ReadAt)
	if err != nil {
		return ChatMessage{}, fmt.Errorf("SendMessage: %w", err)
	}
	return m, nil
}

// oppositeSenderType — чьи сообщения помечаются read_at=now() как побочный
// эффект ListMessages: admin читает -> партнёрские сообщения становятся
// прочитанными, и наоборот. Валидация viewerType (admin/partner) — на
// уровне grpcserver, симметрично SendMessage.
func oppositeSenderType(viewerType string) string {
	if viewerType == "admin" {
		return "partner"
	}
	return "admin"
}

// ListMessages — сообщения треда партнёра с created_at > since (since ==
// nil -> весь тред с начала), ORDER BY created_at ASC, id ASC (стабильный
// tie-break на случай нескольких сообщений с тем же микросекундным
// created_at — TIMESTAMPTZ здесь микросекундной точности, коллизия
// практически невозможна при последовательных INSERT, но ORDER BY только
// по created_at был бы недетерминирован при равенстве).
//
// Побочный эффект В ТОЙ ЖЕ транзакции: все сообщения этого partner_id с
// sender_type = oppositeSenderType(viewerType) и read_at IS NULL
// помечаются read_at=now() ДО select — единственный источник данных для
// ListThreads.unread_count (см. package doc chat.proto ChatService.
// ListMessages за полным обоснованием, почему нет отдельного mark-as-read
// RPC).
func (p *Postgres) ListMessages(ctx context.Context, partnerID string, since *time.Time, viewerType string) ([]ChatMessage, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("ListMessages: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE support.chat_messages
		SET read_at = now()
		WHERE partner_id = $1 AND sender_type = $2 AND read_at IS NULL`,
		partnerID, oppositeSenderType(viewerType)); err != nil {
		return nil, fmt.Errorf("ListMessages: mark read: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT id, partner_id, sender_type, sender_id, body, created_at, read_at
		FROM support.chat_messages
		WHERE partner_id = $1 AND ($2::timestamptz IS NULL OR created_at > $2)
		ORDER BY created_at ASC, id ASC`,
		partnerID, since)
	if err != nil {
		return nil, fmt.Errorf("ListMessages: select: %w", err)
	}
	defer rows.Close()

	var messages []ChatMessage
	for rows.Next() {
		var m ChatMessage
		if err := rows.Scan(&m.ID, &m.PartnerID, &m.SenderType, &m.SenderID, &m.Body, &m.CreatedAt, &m.ReadAt); err != nil {
			return nil, fmt.Errorf("ListMessages: scan: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListMessages: rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("ListMessages: commit tx: %w", err)
	}
	return messages, nil
}

// ListThreads — один партнёр = одна строка (последнее сообщение + счётчик
// непрочитанных сообщений ОТ ПАРТНЁРА, с точки зрения админа — design-
// референс "payme_uz 2 новых" в сайдбаре backoffice-ui). DISTINCT ON
// (partner_id) — последнее сообщение каждого треда, LEFT JOIN — счётчик
// непрочитанных (обслуживается частичным индексом chat_messages_unread_idx,
// migrations/V034). Партнёры без непрочитанных сообщений от партнёра
// (COALESCE) получают unread_count=0, не отсутствуют из LEFT JOIN.
func (p *Postgres) ListThreads(ctx context.Context) ([]ChatThread, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT t.partner_id, t.body, t.created_at, COALESCE(u.unread_count, 0)
		FROM (
			SELECT DISTINCT ON (partner_id) partner_id, body, created_at
			FROM support.chat_messages
			ORDER BY partner_id, created_at DESC
		) t
		LEFT JOIN (
			SELECT partner_id, COUNT(*) AS unread_count
			FROM support.chat_messages
			WHERE sender_type = 'partner' AND read_at IS NULL
			GROUP BY partner_id
		) u ON u.partner_id = t.partner_id
		ORDER BY t.created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("ListThreads: %w", err)
	}
	defer rows.Close()

	var threads []ChatThread
	for rows.Next() {
		var t ChatThread
		if err := rows.Scan(&t.PartnerID, &t.LastBody, &t.LastMessageAt, &t.UnreadCount); err != nil {
			return nil, fmt.Errorf("ListThreads: scan: %w", err)
		}
		threads = append(threads, t)
	}
	return threads, rows.Err()
}
