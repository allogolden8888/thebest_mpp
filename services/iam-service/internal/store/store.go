// Package store — Postgres-доступ к схеме iam.* (migrations/V025__iam.sql).
// CheckPermission — самый частый запрос (вызывается на каждый мутирующий
// запрос backoffice-api через gRPC), остальные — административные CRUD для
// экрана backoffice-ui "Access Control".
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type Role struct {
	ID          int64
	Name        string
	Description string
	Permissions []string
}

type StaffAssignment struct {
	ID         int64
	ExternalID string
	Role       string
	GrantedBy  string
	GrantedAt  time.Time
}

// bcryptCost — luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экран
// 33. Первый credential store в этой кодовой базе (grep bcrypt/argon2/
// scrypt по всему репозиторию — ноль совпадений до этой фичи), нет
// существующего прецедента, cost 12 — стандартный, не занижен ради
// скорости dev-окружения (это было бы неверным выбором именно потому, что
// потом легко забыть поднять его для прода).
const bcryptCost = 12

// dummyHash — luminous-hugging-charm.md Экран 33: посчитан один раз при
// старте, используется VerifyStaffCredentials, когда username не найден,
// чтобы всё равно выполнить bcrypt.CompareHashAndPassword той же
// стоимости — иначе "неизвестный username" отвечал бы заметно быстрее
// "неверный пароль", раскрывая существование аккаунта по времени ответа.
var dummyHash = func() []byte {
	h, err := bcrypt.GenerateFromPassword([]byte("timing-safety-dummy-password"), bcryptCost)
	if err != nil {
		panic(fmt.Sprintf("precompute dummy bcrypt hash: %v", err))
	}
	return h
}()

var (
	// ErrRoleNotFound — имя роли не существует в iam.roles.
	ErrRoleNotFound = errors.New("роль не найдена")
	// ErrUsernameTaken — CreateStaffAccount с уже занятым username
	// (уникальный индекс iam.staff_accounts.username).
	ErrUsernameTaken = errors.New("username уже занят")
	// ErrAlreadyAssigned — (external_id, role) уже активно назначены
	// (уникальный частичный индекс staff_role_assignments_active_unique).
	ErrAlreadyAssigned = errors.New("роль уже назначена этому пользователю")
	// ErrInvalidPartnerPortalRole — role не входит в CHECK-ограниченный
	// набор iam.partner_portal_role_assignments (в отличие от Staff*, role
	// здесь не FK на открытый каталог iam.roles).
	ErrInvalidPartnerPortalRole = errors.New("role должен быть partner-admin или partner-viewer")
	// ErrPartnerPortalUserNotFound — external_id не существует в
	// iam.partner_portal_users (FK на неё, в отличие от Staff*, где
	// external_id — свободная строка).
	ErrPartnerPortalUserNotFound = errors.New("партнёрский пользователь не найден")
)

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

// CheckPermission — allowed=true, если у external_id есть хотя бы одна
// НЕ отозванная роль, несущая указанное право. roles — имена всех таких
// ролей (для аудита/отладки на стороне вызывающего, см. iam.proto).
func (p *Postgres) CheckPermission(ctx context.Context, externalID, permission string) (bool, []string, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT DISTINCT r.name
		FROM iam.staff_role_assignments sra
		JOIN iam.roles r ON r.id = sra.role_id
		JOIN iam.role_permissions rp ON rp.role_id = r.id
		JOIN iam.permissions perm ON perm.id = rp.permission_id
		WHERE sra.external_id = $1 AND sra.revoked_at IS NULL AND perm.name = $2
		ORDER BY r.name`, externalID, permission)
	if err != nil {
		return false, nil, fmt.Errorf("CheckPermission: %w", err)
	}
	defer rows.Close()

	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return false, nil, fmt.Errorf("CheckPermission: scan: %w", err)
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return false, nil, fmt.Errorf("CheckPermission: %w", err)
	}
	return len(roles) > 0, roles, nil
}

// ListRoles — все роли с их правами, для role-picker'а backoffice-ui.
func (p *Postgres) ListRoles(ctx context.Context) ([]Role, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT r.id, r.name, r.description,
		       COALESCE(array_agg(perm.name ORDER BY perm.name) FILTER (WHERE perm.name IS NOT NULL), '{}')
		FROM iam.roles r
		LEFT JOIN iam.role_permissions rp ON rp.role_id = r.id
		LEFT JOIN iam.permissions perm ON perm.id = rp.permission_id
		GROUP BY r.id, r.name, r.description
		ORDER BY r.name`)
	if err != nil {
		return nil, fmt.Errorf("ListRoles: %w", err)
	}
	defer rows.Close()

	var roles []Role
	for rows.Next() {
		var role Role
		if err := rows.Scan(&role.ID, &role.Name, &role.Description, &role.Permissions); err != nil {
			return nil, fmt.Errorf("ListRoles: scan: %w", err)
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

// ListStaffAssignments — активные (revoked_at IS NULL) назначения.
// externalID == "" -> все; иначе фильтр по конкретному пользователю
// (backoffice-ui "у кого какой доступ").
func (p *Postgres) ListStaffAssignments(ctx context.Context, externalID string) ([]StaffAssignment, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT sra.id, sra.external_id, r.name, sra.granted_by, sra.granted_at
		FROM iam.staff_role_assignments sra
		JOIN iam.roles r ON r.id = sra.role_id
		WHERE sra.revoked_at IS NULL AND ($1 = '' OR sra.external_id = $1)
		ORDER BY sra.external_id, r.name`, externalID)
	if err != nil {
		return nil, fmt.Errorf("ListStaffAssignments: %w", err)
	}
	defer rows.Close()

	var assignments []StaffAssignment
	for rows.Next() {
		var a StaffAssignment
		if err := rows.Scan(&a.ID, &a.ExternalID, &a.Role, &a.GrantedBy, &a.GrantedAt); err != nil {
			return nil, fmt.Errorf("ListStaffAssignments: scan: %w", err)
		}
		assignments = append(assignments, a)
	}
	return assignments, rows.Err()
}

// AssignStaffRole — INSERT + identity_audit в одной транзакции: audit
// пишется ДО commit, не best-effort после (тот же принцип, что
// registry mutation -> audit в execution-control-service, но здесь
// порядок наоборот безопасен, потому что оба пишутся в один Postgres —
// либо оба применяются, либо ни один, реальная гонка "аудит потерян, но
// мутация применилась" здесь структурно невозможна благодаря транзакции,
// в отличие от registry (in-memory) + audit (Postgres) в другом сервисе).
func (p *Postgres) AssignStaffRole(ctx context.Context, externalID, role, grantedBy string) (StaffAssignment, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return StaffAssignment{}, fmt.Errorf("AssignStaffRole: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var roleID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM iam.roles WHERE name = $1`, role).Scan(&roleID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StaffAssignment{}, ErrRoleNotFound
		}
		return StaffAssignment{}, fmt.Errorf("AssignStaffRole: lookup role: %w", err)
	}

	var a StaffAssignment
	err = tx.QueryRow(ctx, `
		INSERT INTO iam.staff_role_assignments (external_id, role_id, granted_by)
		VALUES ($1, $2, $3)
		RETURNING id, granted_at`, externalID, roleID, grantedBy).Scan(&a.ID, &a.GrantedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return StaffAssignment{}, ErrAlreadyAssigned
		}
		return StaffAssignment{}, fmt.Errorf("AssignStaffRole: insert: %w", err)
	}
	a.ExternalID = externalID
	a.Role = role
	a.GrantedBy = grantedBy

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'ROLE_GRANTED', $2)`,
		grantedBy, externalID+":"+role); err != nil {
		return StaffAssignment{}, fmt.Errorf("AssignStaffRole: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return StaffAssignment{}, fmt.Errorf("AssignStaffRole: commit: %w", err)
	}
	return a, nil
}

// RevokeStaffRole — revoked=false, если не нашлось активного назначения
// (уже отозвано или никогда не выдавалось) — не ошибка, идемпотентно.
func (p *Postgres) RevokeStaffRole(ctx context.Context, externalID, role, revokedBy string) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("RevokeStaffRole: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE iam.staff_role_assignments SET revoked_by = $1, revoked_at = now()
		WHERE external_id = $2 AND revoked_at IS NULL
		  AND role_id = (SELECT id FROM iam.roles WHERE name = $3)`, revokedBy, externalID, role)
	if err != nil {
		return false, fmt.Errorf("RevokeStaffRole: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'ROLE_REVOKED', $2)`,
		revokedBy, externalID+":"+role); err != nil {
		return false, fmt.Errorf("RevokeStaffRole: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("RevokeStaffRole: commit: %w", err)
	}
	return true, nil
}

func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}

func isForeignKeyViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23503"
	}
	return false
}

type PartnerPortalAssignment struct {
	ID         int64
	ExternalID string
	Role       string
	GrantedBy  string
	GrantedAt  time.Time
}

var validPartnerPortalRoles = map[string]bool{"partner-admin": true, "partner-viewer": true}

// ListPartnerPortalAssignments — активные (revoked_at IS NULL) назначения.
// externalID == "" -> все; иначе фильтр по конкретному партнёрскому
// пользователю. role — inline CHECK-литерал (см. package doc), не JOIN на
// открытый каталог, как у ListStaffAssignments.
func (p *Postgres) ListPartnerPortalAssignments(ctx context.Context, externalID string) ([]PartnerPortalAssignment, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, external_id, role, granted_by, granted_at
		FROM iam.partner_portal_role_assignments
		WHERE revoked_at IS NULL AND ($1 = '' OR external_id = $1)
		ORDER BY external_id, role`, externalID)
	if err != nil {
		return nil, fmt.Errorf("ListPartnerPortalAssignments: %w", err)
	}
	defer rows.Close()

	var assignments []PartnerPortalAssignment
	for rows.Next() {
		var a PartnerPortalAssignment
		if err := rows.Scan(&a.ID, &a.ExternalID, &a.Role, &a.GrantedBy, &a.GrantedAt); err != nil {
			return nil, fmt.Errorf("ListPartnerPortalAssignments: scan: %w", err)
		}
		assignments = append(assignments, a)
	}
	return assignments, rows.Err()
}

// AssignPartnerPortalRole — тот же транзакционный insert+audit паттерн, что
// AssignStaffRole. Нет партиционированного unique-индекса на "активное
// назначение" для этой таблицы (в отличие от
// staff_role_assignments_active_unique) — сознательный, задокументированный
// объём этой задачи (BACKOFFICE_DESIGN_SPEC.md Экран 35): дубликат активного
// (external_id, role) сегодня возможен на уровне БД, если понадобится
// защита — отдельная миграция с частичным уникальным индексом, не часть
// этого маленького CRUD-добавления.
func (p *Postgres) AssignPartnerPortalRole(ctx context.Context, externalID, role, grantedBy string) (PartnerPortalAssignment, error) {
	if !validPartnerPortalRoles[role] {
		return PartnerPortalAssignment{}, ErrInvalidPartnerPortalRole
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return PartnerPortalAssignment{}, fmt.Errorf("AssignPartnerPortalRole: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var a PartnerPortalAssignment
	err = tx.QueryRow(ctx, `
		INSERT INTO iam.partner_portal_role_assignments (external_id, role, granted_by)
		VALUES ($1, $2, $3)
		RETURNING id, granted_at`, externalID, role, grantedBy).Scan(&a.ID, &a.GrantedAt)
	if err != nil {
		if isForeignKeyViolation(err) {
			return PartnerPortalAssignment{}, ErrPartnerPortalUserNotFound
		}
		return PartnerPortalAssignment{}, fmt.Errorf("AssignPartnerPortalRole: insert: %w", err)
	}
	a.ExternalID = externalID
	a.Role = role
	a.GrantedBy = grantedBy

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'PARTNER_PORTAL_ROLE_GRANTED', $2)`,
		grantedBy, externalID+":"+role); err != nil {
		return PartnerPortalAssignment{}, fmt.Errorf("AssignPartnerPortalRole: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return PartnerPortalAssignment{}, fmt.Errorf("AssignPartnerPortalRole: commit: %w", err)
	}
	return a, nil
}

// RevokePartnerPortalRole — тот же идемпотентный паттерн, что RevokeStaffRole.
func (p *Postgres) RevokePartnerPortalRole(ctx context.Context, externalID, role, revokedBy string) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("RevokePartnerPortalRole: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE iam.partner_portal_role_assignments SET revoked_by = $1, revoked_at = now()
		WHERE external_id = $2 AND role = $3 AND revoked_at IS NULL`, revokedBy, externalID, role)
	if err != nil {
		return false, fmt.Errorf("RevokePartnerPortalRole: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'PARTNER_PORTAL_ROLE_REVOKED', $2)`,
		revokedBy, externalID+":"+role); err != nil {
		return false, fmt.Errorf("RevokePartnerPortalRole: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("RevokePartnerPortalRole: commit: %w", err)
	}
	return true, nil
}

type StaffAccount struct {
	ExternalID  string
	Username    string
	DisplayName string
	Active      bool
	CreatedAt   time.Time
}

// CreateStaffAccount — external_id = username (V031 комментарий: нет
// Keycloak sub, взять неоткуда до реального Keycloak). Пароль хешируется
// здесь, никогда не покидает этот процесс как plaintext дальше вызова.
func (p *Postgres) CreateStaffAccount(ctx context.Context, username, password, displayName, createdBy string) (StaffAccount, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return StaffAccount{}, fmt.Errorf("CreateStaffAccount: hash password: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return StaffAccount{}, fmt.Errorf("CreateStaffAccount: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var a StaffAccount
	err = tx.QueryRow(ctx, `
		INSERT INTO iam.staff_accounts (external_id, username, password_hash, display_name, created_by)
		VALUES ($1, $1, $2, $3, $4)
		RETURNING external_id, username, display_name, active, created_at`,
		username, string(hash), displayName, createdBy).
		Scan(&a.ExternalID, &a.Username, &a.DisplayName, &a.Active, &a.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return StaffAccount{}, ErrUsernameTaken
		}
		return StaffAccount{}, fmt.Errorf("CreateStaffAccount: insert: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'STAFF_ACCOUNT_CREATED', $2)`,
		createdBy, username); err != nil {
		return StaffAccount{}, fmt.Errorf("CreateStaffAccount: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return StaffAccount{}, fmt.Errorf("CreateStaffAccount: commit: %w", err)
	}
	return a, nil
}

func (p *Postgres) ListStaffAccounts(ctx context.Context, activeOnly bool) ([]StaffAccount, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT external_id, username, display_name, active, created_at
		FROM iam.staff_accounts
		WHERE ($1 = false OR active = true)
		ORDER BY username`, activeOnly)
	if err != nil {
		return nil, fmt.Errorf("ListStaffAccounts: %w", err)
	}
	defer rows.Close()

	var out []StaffAccount
	for rows.Next() {
		var a StaffAccount
		if err := rows.Scan(&a.ExternalID, &a.Username, &a.DisplayName, &a.Active, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("ListStaffAccounts: scan: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeactivateStaffAccount — deactivated=false, если аккаунт уже неактивен
// или не существует — не ошибка, идемпотентно (тот же контракт, что
// RevokeStaffRole).
func (p *Postgres) DeactivateStaffAccount(ctx context.Context, externalID, actor string) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("DeactivateStaffAccount: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE iam.staff_accounts SET active = false
		WHERE external_id = $1 AND active = true`, externalID)
	if err != nil {
		return false, fmt.Errorf("DeactivateStaffAccount: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target) VALUES ($1, 'STAFF_ACCOUNT_DEACTIVATED', $2)`,
		actor, externalID); err != nil {
		return false, fmt.Errorf("DeactivateStaffAccount: audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("DeactivateStaffAccount: commit: %w", err)
	}
	return true, nil
}

// VerifyStaffCredentials — bcrypt-сравнение целиком здесь, хеш никогда не
// покидает store/gRPC-границу IamService наружу (внешний ответ — только
// external_id/ok, см. VerifyStaffCredentialsResponse). Неизвестный
// username и неактивный аккаунт — оба ok=false без различимой ошибки для
// вызывающего (backoffice-api/frontend видят одинаковый отказ), но ВСЕГДА
// выполняют bcrypt.CompareHashAndPassword одной и той же стоимости
// (dummyHash при неизвестном username) — не раскрываем существование
// аккаунта по времени ответа.
func (p *Postgres) VerifyStaffCredentials(ctx context.Context, username, password string) (string, bool, error) {
	var externalID, hash string
	var active bool
	err := p.pool.QueryRow(ctx, `
		SELECT external_id, password_hash, active FROM iam.staff_accounts WHERE username = $1`, username).
		Scan(&externalID, &hash, &active)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", false, fmt.Errorf("VerifyStaffCredentials: lookup: %w", err)
		}
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
		return "", false, nil
	}

	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return "", false, nil
	}
	if !active {
		return "", false, nil
	}
	return externalID, true, nil
}
