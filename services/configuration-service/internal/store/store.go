// Package store — create_immutable_version + write_config_and_outbox
// (service_internal_methods.md §3.2): одна PostgreSQL-транзакция,
// config.config_versions + config.config_outbox
// (migrations/V002__config_versions.sql, V003__config_outbox.sql).
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/configuration-service/internal/validate"
)

// policyTemplateOrConsent — эти два entity_type не пишут в config_versions
// (собственные таблицы policy.policy_template / policy.subscriber_consent,
// см. migrations/V002 комментарий) — outbox всё равно пишется, с
// config_version_id=NULL.
func skipsConfigVersionsTable(e validate.EntityType) bool {
	return e == validate.EntityPolicyTemplate || e == validate.EntitySubscriberConsent
}

type ConfigVersion struct {
	ID        int64
	EntityType validate.EntityType
	EntityID  string
	Version   int32
	Status    string
	CreatedAt time.Time
	CreatedBy string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateImmutableVersionAndOutbox — create_immutable_version +
// write_config_and_outbox в одной транзакции. version = max(version)+1 для
// (entity_type, entity_id), вычислено внутри той же транзакции
// (SELECT ... FOR UPDATE — сериализация конкурентных CRUD на одну и ту же
// сущность).
func (s *Store) CreateImmutableVersionAndOutbox(ctx context.Context, entityType validate.EntityType, entityID string, payloadJSON []byte, createdBy string) (ConfigVersion, error) {
	var result ConfigVersion

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if skipsConfigVersionsTable(entityType) {
			// Нет строки в config_versions — outbox пишется напрямую,
			// config_version_id=NULL (V003 комментарий).
			_, err := tx.Exec(ctx, `
				INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload)
				VALUES (NULL, $1, $2, $3)
			`, string(entityType), entityID, payloadJSON)
			if err != nil {
				return fmt.Errorf("insert outbox (no config_versions row): %w", err)
			}
			result = ConfigVersion{EntityType: entityType, EntityID: entityID, Status: "active", CreatedBy: createdBy}
			return nil
		}

		// pg_advisory_xact_lock сериализует конкурентные CreateImmutableVersion
		// для одной и той же (entity_type, entity_id) — включая самую первую
		// версию, когда строк ещё нет и обычный "SELECT ... FOR UPDATE" не может
		// заблокировать ничего (PostgreSQL к тому же вообще не разрешает FOR
		// UPDATE вместе с агрегатной функцией MAX() в одном запросе —
		// предыдущая версия этого кода не компилировалась в рантайме запроса).
		// Транзакционная advisory-блокировка снимается автоматически на
		// commit/rollback.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, string(entityType)+":"+entityID); err != nil {
			return fmt.Errorf("acquire version lock: %w", err)
		}

		var nextVersion int32
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(version), 0) + 1
			FROM config.config_versions
			WHERE entity_type = $1 AND entity_id = $2
		`, string(entityType), entityID).Scan(&nextVersion)
		if err != nil {
			return fmt.Errorf("compute next version: %w", err)
		}

		var id int64
		var createdAt time.Time
		err = tx.QueryRow(ctx, `
			INSERT INTO config.config_versions (entity_type, entity_id, version, payload, status, created_by)
			VALUES ($1, $2, $3, $4, 'active', $5)
			RETURNING id, created_at
		`, string(entityType), entityID, nextVersion, payloadJSON, createdBy).Scan(&id, &createdAt)
		if err != nil {
			return fmt.Errorf("insert config_versions: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload)
			VALUES ($1, $2, $3, $4)
		`, id, string(entityType), entityID, payloadJSON)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}

		result = ConfigVersion{
			ID: id, EntityType: entityType, EntityID: entityID, Version: nextVersion,
			Status: "active", CreatedAt: createdAt, CreatedBy: createdBy,
		}
		return nil
	})
	if err != nil {
		return ConfigVersion{}, err
	}
	return result, nil
}

// GetActiveVersion — handle_crud_request (GetActiveVersion RPC).
func (s *Store) GetActiveVersion(ctx context.Context, entityType validate.EntityType, entityID string) (ConfigVersion, error) {
	var v ConfigVersion
	err := s.pool.QueryRow(ctx, `
		SELECT id, entity_type, entity_id, version, status, created_at, created_by
		FROM config.config_versions
		WHERE entity_type = $1 AND entity_id = $2 AND status = 'active'
		ORDER BY version DESC LIMIT 1
	`, string(entityType), entityID).Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy)
	if err != nil {
		return ConfigVersion{}, fmt.Errorf("get active version: %w", err)
	}
	return v, nil
}

// ListVersions — handle_crud_request (ListVersions RPC), простая пагинация
// по id (page_token = последний увиденный id, "" = с начала).
func (s *Store) ListVersions(ctx context.Context, entityType validate.EntityType, entityID string, pageSize int32, pageToken string) ([]ConfigVersion, string, error) {
	afterID := int64(0)
	if pageToken != "" {
		if _, err := fmt.Sscanf(pageToken, "%d", &afterID); err != nil {
			return nil, "", fmt.Errorf("invalid page_token: %w", err)
		}
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, entity_type, entity_id, version, status, created_at, created_by
		FROM config.config_versions
		WHERE entity_type = $1 AND entity_id = $2 AND id > $3
		ORDER BY id ASC LIMIT $4
	`, string(entityType), entityID, afterID, pageSize)
	if err != nil {
		return nil, "", fmt.Errorf("list versions: %w", err)
	}
	defer rows.Close()

	var out []ConfigVersion
	for rows.Next() {
		var v ConfigVersion
		if err := rows.Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy); err != nil {
			return nil, "", fmt.Errorf("scan version row: %w", err)
		}
		out = append(out, v)
	}

	nextToken := ""
	if len(out) == int(pageSize) {
		nextToken = fmt.Sprintf("%d", out[len(out)-1].ID)
	}
	return out, nextToken, rows.Err()
}

// ArchiveVersion — handle_crud_request (ArchiveVersion RPC). Hard delete не
// выполняется (HLD §16.1, migrations/V002 комментарий) — только
// status=archived.
func (s *Store) ArchiveVersion(ctx context.Context, entityType validate.EntityType, entityID string, version int32) (ConfigVersion, error) {
	var v ConfigVersion
	err := s.pool.QueryRow(ctx, `
		UPDATE config.config_versions
		SET status = 'archived'
		WHERE entity_type = $1 AND entity_id = $2 AND version = $3
		RETURNING id, entity_type, entity_id, version, status, created_at, created_by
	`, string(entityType), entityID, version).Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy)
	if err != nil {
		return ConfigVersion{}, fmt.Errorf("archive version: %w", err)
	}
	return v, nil
}
