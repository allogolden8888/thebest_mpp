// Package store — create_immutable_version + write_config_and_outbox
// (service_internal_methods.md §3.2): одна PostgreSQL-транзакция,
// config.config_versions + config.config_outbox
// (migrations/V002__config_versions.sql, V003__config_outbox.sql).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/configuration-service/internal/validate"
)

// ErrVersionConflict — compare-and-swap провалился: expectedVersion,
// переданный CreateImmutableVersionAndOutbox, разошёлся с реальной текущей
// версией (entity_type, entity_id) на момент записи (BACKOFFICE_ROADMAP.md,
// Production Readiness Review, P1 "Конкурентные изменения"). Отдельный
// sentinel, не текстовая ошибка — grpcserver/server.go должен отличить этот
// случай от прочих ошибок транзакции и вернуть codes.Aborted, а не
// codes.Internal.
var ErrVersionConflict = errors.New("version conflict: текущая версия не совпадает с expected_version")

// policyTemplateOrConsent — эти два entity_type не пишут в config_versions
// (собственные таблицы policy.policy_template / policy.subscriber_consent,
// см. migrations/V002 комментарий) — outbox всё равно пишется, с
// config_version_id=NULL.
func skipsConfigVersionsTable(e validate.EntityType) bool {
	return e == validate.EntityPolicyTemplate || e == validate.EntitySubscriberConsent
}

type ConfigVersion struct {
	ID         int64
	EntityType validate.EntityType
	EntityID   string
	Version    int32
	Status     string
	CreatedAt  time.Time
	CreatedBy  string
	// Payload — найдено при реализации partner-self-service-api (Фаза 3
	// плана): до этого поля ни один читающий метод не возвращал содержимое
	// документа, что делало read-modify-write невозможным ни для одного
	// клиента ConfigService. См. platform-contracts/grpc/internal_control.proto
	// ConfigVersionResponse.payload_json за тем же обоснованием на уровне
	// контракта.
	Payload []byte
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
//
// expectedVersion — оптимистическая блокировка (BACKOFFICE_ROADMAP.md,
// Production Readiness Review, P1 "Конкурентные изменения"): 0 = без
// проверки (текущее поведение для всех вызывающих, которые ещё не приняли
// CAS — backoffice-api/config.go, compliance-api/consent.go). Ненулевое
// значение сверяется с реальной текущей версией СРАЗУ ПОСЛЕ
// pg_advisory_xact_lock, то есть внутри того же критического участка, что
// уже сериализует конкурентные записи на (entity_type, entity_id) — без
// этой проверки advisory-lock лишь гарантирует, что версии не потеряются
// на уровне номеров (N+1 всегда достаётся ровно одному писателю), но не
// мешает второму писателю молча перезаписать поля, изменённые первым,
// собственной устаревшей копией документа.
func (s *Store) CreateImmutableVersionAndOutbox(ctx context.Context, entityType validate.EntityType, entityID string, payloadJSON []byte, createdBy string, expectedVersion int64) (ConfigVersion, error) {
	var result ConfigVersion

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if skipsConfigVersionsTable(entityType) {
			// Эти entity_type не ведут историю версий вообще (см.
			// skipsConfigVersionsTable) — CAS здесь структурно неприменим,
			// expectedVersion игнорируется (ни один сегодняшний вызывающий
			// этих entity_type его не заполняет).
			_, err := tx.Exec(ctx, `
				INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload)
				VALUES (NULL, $1, $2, $3)
			`, string(entityType), entityID, payloadJSON)
			if err != nil {
				return fmt.Errorf("insert outbox (no config_versions row): %w", err)
			}
			result = ConfigVersion{EntityType: entityType, EntityID: entityID, Status: "active", CreatedBy: createdBy, Payload: payloadJSON}
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

		if expectedVersion != 0 {
			// currentVersion=0 означает "версий ещё нет" — тот же COALESCE(...,
			// 0), что и ниже при вычислении nextVersion, читается ВНУТРИ уже
			// взятого advisory-lock, поэтому не может устареть между этой
			// проверкой и INSERT ниже: второй конкурентный вызов заблокирован
			// на tx.Exec(pg_advisory_xact_lock) выше до commit/rollback первого.
			var currentVersion int32
			if err := tx.QueryRow(ctx, `
				SELECT COALESCE(MAX(version), 0)
				FROM config.config_versions
				WHERE entity_type = $1 AND entity_id = $2
			`, string(entityType), entityID).Scan(&currentVersion); err != nil {
				return fmt.Errorf("check expected_version: %w", err)
			}
			if int64(currentVersion) != expectedVersion {
				return ErrVersionConflict
			}
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
			Status: "active", CreatedAt: createdAt, CreatedBy: createdBy, Payload: payloadJSON,
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
		SELECT id, entity_type, entity_id, version, status, created_at, created_by, payload
		FROM config.config_versions
		WHERE entity_type = $1 AND entity_id = $2 AND status = 'active'
		ORDER BY version DESC LIMIT 1
	`, string(entityType), entityID).Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy, &v.Payload)
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
		SELECT id, entity_type, entity_id, version, status, created_at, created_by, payload
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
		if err := rows.Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy, &v.Payload); err != nil {
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

// ListActiveVersionsByType — BACKOFFICE_DESIGN_SPEC.md Экраны 32/23
// (Categories/CTN): ListVersions выше требует уже известный entity_id
// (история версий ОДНОЙ сущности) — для "список всех категорий" нужен
// противоположный срез, одна (последняя активная) строка на каждый
// entity_id этого entity_type. DISTINCT ON (entity_id) ... ORDER BY
// entity_id, id DESC — если у entity_id несколько версий, берёт
// максимальный id среди статуса 'active' (обычно ровно один активный на
// entity_id, но не гарантировано уникальным индексом на уровне схемы —
// не падает, если вдруг окажется больше одного).
func (s *Store) ListActiveVersionsByType(ctx context.Context, entityType validate.EntityType) ([]ConfigVersion, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (entity_id) id, entity_type, entity_id, version, status, created_at, created_by, payload
		FROM config.config_versions
		WHERE entity_type = $1 AND status = 'active'
		ORDER BY entity_id, id DESC
	`, string(entityType))
	if err != nil {
		return nil, fmt.Errorf("list active versions by type: %w", err)
	}
	defer rows.Close()

	var out []ConfigVersion
	for rows.Next() {
		var v ConfigVersion
		if err := rows.Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy, &v.Payload); err != nil {
			return nil, fmt.Errorf("list active versions by type: scan: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// GetVersionByNumber — luminous-hugging-charm.md Фаза 10 (DiffVersions).
// В отличие от GetActiveVersion, здесь конкретный номер версии, не только
// текущая активная — diff по определению сравнивает ДВЕ версии, обычно
// хотя бы одна из них уже archived. Возвращает payload — единственный
// store-метод в этом файле, который это делает (остальные RPC
// сознательно не несут payload_json в ответе, см.
// services/backoffice-api/README.md "Rotate credential" за тем же
// наблюдением о ConfigVersionResponse — здесь это МЕНЯЕТСЯ намеренно,
// DiffVersions ради самого diff обязан вернуть содержимое).
func (s *Store) GetVersionByNumber(ctx context.Context, entityType validate.EntityType, entityID string, version int32) ([]byte, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT payload FROM config.config_versions
		WHERE entity_type = $1 AND entity_id = $2 AND version = $3
	`, string(entityType), entityID, version).Scan(&payload)
	if err != nil {
		return nil, fmt.Errorf("get version %d: %w", version, err)
	}
	return payload, nil
}

// ArchiveVersion — handle_crud_request (ArchiveVersion RPC). Hard delete не
// выполняется (HLD §16.1). The terminal transition and a new
// outbox row are committed atomically; otherwise full-mirror consumers never
// learn that the active version was archived.
func (s *Store) ArchiveVersion(ctx context.Context, entityType validate.EntityType, entityID string, version int32) (ConfigVersion, error) {
	var v ConfigVersion
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE config.config_versions
			SET status = 'archived'
			WHERE entity_type = $1 AND entity_id = $2 AND version = $3 AND status = 'active'
			RETURNING id, entity_type, entity_id, version, status, created_at, created_by, payload
		`, string(entityType), entityID, version).Scan(&v.ID, &v.EntityType, &v.EntityID, &v.Version, &v.Status, &v.CreatedAt, &v.CreatedBy, &v.Payload)
		if err != nil {
			return fmt.Errorf("update config version: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload)
			VALUES ($1, $2, $3, $4)
		`, v.ID, string(entityType), entityID, v.Payload); err != nil {
			return fmt.Errorf("insert archive outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return ConfigVersion{}, fmt.Errorf("archive version: %w", err)
	}
	return v, nil
}
