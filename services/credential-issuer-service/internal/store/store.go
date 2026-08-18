// Package store — Postgres-доступ credential-issuer-service: собственная
// схема credentials.* (migrations/V027__credentials.sql) плюс ПРЯМОЕ
// чтение config.config_versions (владелец — configuration-service) ради
// одного lookup'а — тот же паттерн прямого cross-schema чтения, что
// backoffice-api уже использует для DLQ/reconciliation/audit browse
// (backoffice-api/internal/store/postgres.go package doc), не новая
// gRPC-зависимость: ConfigService.GetActiveVersion (internal_control.proto)
// не несёт payload_json в ответе вообще (только entity_type/entity_id/
// version/status/created_at) — реальное содержимое живёт только в Postgres.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrPartnerNotFound — нет активной версии PARTNER-конфига с этим entity_id.
	ErrPartnerNotFound = errors.New("активный PARTNER-конфиг не найден")
	// ErrApplicationNotFound — партнёр есть, но у него нет такого application_id.
	ErrApplicationNotFound = errors.New("application_id не найден в PARTNER-конфиге этого партнёра")
)

// partnerApplication — минимальный срез partner.schema.json's
// applications[] элемента, нужный здесь (id + credential_ref), не полная
// структура — этому сервису не нужны ip_allowlist/rate_limit_tps/etc.
type partnerPayload struct {
	Applications []struct {
		ApplicationID string `json:"application_id"`
		Auth          struct {
			CredentialRef string `json:"credential_ref"`
		} `json:"auth"`
	} `json:"applications"`
}

type IssuedSecret struct {
	ID             int64
	PartnerID      string
	ApplicationID  string
	CredentialRef  string
	SecretVersion  int32
	Status         string
	IssuedAt       time.Time
	IssuedBy       string
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

// LookupCredentialRef — читает АКТИВНУЮ версию config.config_versions
// (entity_type='partner', entity_id=partnerID), разбирает JSON, находит
// application_id. Не создаёт/не изменяет ничего — чистый lookup перед
// RotateCredential.
func (p *Postgres) LookupCredentialRef(ctx context.Context, partnerID, applicationID string) (string, error) {
	var raw []byte
	err := p.pool.QueryRow(ctx, `
		SELECT payload FROM config.config_versions
		WHERE entity_type = 'partner' AND entity_id = $1 AND status = 'active'
		ORDER BY version DESC LIMIT 1`, partnerID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrPartnerNotFound
		}
		return "", fmt.Errorf("LookupCredentialRef: чтение config_versions: %w", err)
	}

	var payload partnerPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", fmt.Errorf("LookupCredentialRef: разбор PARTNER payload: %w", err)
	}
	for _, app := range payload.Applications {
		if app.ApplicationID == applicationID {
			return app.Auth.CredentialRef, nil
		}
	}
	return "", ErrApplicationNotFound
}

// RecordRotation — атомарно (одна транзакция): отзывает предыдущую
// активную версию (если есть — первый выпуск для application'а её не
// имеет, это не ошибка), вставляет новую строку credentials.issued_secrets
// со следующим secret_version, пишет credentials.rotation_audit. Тот же
// принцип "мутация и аудит в одной Postgres-транзакции", что
// iam-service.AssignStaffRole (README iam-service — "гонка между registry
// и audit здесь структурно невозможна").
func (p *Postgres) RecordRotation(ctx context.Context, partnerID, applicationID, credentialRef, issuedBy string) (IssuedSecret, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return IssuedSecret{}, fmt.Errorf("RecordRotation: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var previousVersion int32
	err = tx.QueryRow(ctx, `
		SELECT secret_version FROM credentials.issued_secrets
		WHERE partner_id = $1 AND application_id = $2 AND status = 'active'
		FOR UPDATE`, partnerID, applicationID).Scan(&previousVersion)
	switch {
	case err == nil:
		if _, err := tx.Exec(ctx, `
			UPDATE credentials.issued_secrets SET status = 'revoked', revoked_at = now()
			WHERE partner_id = $1 AND application_id = $2 AND status = 'active'`, partnerID, applicationID); err != nil {
			return IssuedSecret{}, fmt.Errorf("RecordRotation: revoke previous: %w", err)
		}
	case errors.Is(err, pgx.ErrNoRows):
		previousVersion = 0 // первый выпуск для этого application'а — "ISSUED", не "ROTATED".
	default:
		return IssuedSecret{}, fmt.Errorf("RecordRotation: lookup previous: %w", err)
	}

	newVersion := previousVersion + 1
	var s IssuedSecret
	err = tx.QueryRow(ctx, `
		INSERT INTO credentials.issued_secrets (partner_id, application_id, credential_ref, secret_version, status, issued_by)
		VALUES ($1, $2, $3, $4, 'active', $5)
		RETURNING id, issued_at`, partnerID, applicationID, credentialRef, newVersion, issuedBy).Scan(&s.ID, &s.IssuedAt)
	if err != nil {
		return IssuedSecret{}, fmt.Errorf("RecordRotation: insert: %w", err)
	}
	s.PartnerID = partnerID
	s.ApplicationID = applicationID
	s.CredentialRef = credentialRef
	s.SecretVersion = newVersion
	s.Status = "active"
	s.IssuedBy = issuedBy

	action := "ROTATED"
	if previousVersion == 0 {
		action = "ISSUED"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO credentials.rotation_audit (actor, action, target) VALUES ($1, $2, $3)`,
		issuedBy, action, partnerID+":"+applicationID); err != nil {
		return IssuedSecret{}, fmt.Errorf("RecordRotation: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return IssuedSecret{}, fmt.Errorf("RecordRotation: commit: %w", err)
	}
	return s, nil
}

// ListIssuedSecrets — метаданные (НЕ секреты — те только в Vault) по
// партнёру, все версии (активные и отозванные), новейшие первыми.
func (p *Postgres) ListIssuedSecrets(ctx context.Context, partnerID string) ([]IssuedSecret, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, partner_id, application_id, credential_ref, secret_version, status, issued_at, issued_by
		FROM credentials.issued_secrets
		WHERE partner_id = $1
		ORDER BY application_id, secret_version DESC`, partnerID)
	if err != nil {
		return nil, fmt.Errorf("ListIssuedSecrets: %w", err)
	}
	defer rows.Close()

	var secrets []IssuedSecret
	for rows.Next() {
		var s IssuedSecret
		if err := rows.Scan(&s.ID, &s.PartnerID, &s.ApplicationID, &s.CredentialRef, &s.SecretVersion, &s.Status, &s.IssuedAt, &s.IssuedBy); err != nil {
			return nil, fmt.Errorf("ListIssuedSecrets: scan: %w", err)
		}
		secrets = append(secrets, s)
	}
	return secrets, rows.Err()
}
