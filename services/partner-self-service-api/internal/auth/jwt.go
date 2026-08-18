// Package auth — JWT/Keycloak validation, тот же паттерн, что
// services/partner-api/internal/auth/jwt.go (aud/iss обязательны,
// partner_id из claim, не из тела/пути запроса — партнёр не должен
// получить доступ к чужому конфигу через подмену параметра). Отличие от
// partner-api: этот сервис МУТИРУЕТ (заявки на изменение applications/
// senders, ротация credentials), поэтому дополнительно несёт роль
// (realm_access.roles, тот же claim-формат, что Keycloak уже отдаёт в
// backoffice-realm) — RequireAdmin гейтит деструктивные операции, чтение
// открыто любой валидной partner_id-роли.
//
// Фаза 3 плана (/Users/Alisher/.claude/plans/luminous-hugging-charm.md):
// новый Keycloak realm/client для партнёрских людей-пользователей —
// координация вне этого репозитория (см. допущение в начале плана).
// Валидатор ниже не завязан на конкретный realm, только на aud/iss/
// public key, настраиваемые деплоем — заработает, как только конкретный
// realm появится, без изменений кода.
//
// IamService.CheckPermission НЕ используется здесь — найдено при
// реализации: platform-contracts/grpc/iam.proto проверяет только
// iam.staff_role_assignments (сотрудники backoffice), под
// iam.partner_portal_role_assignments (Ф0, migrations/V025__iam.sql)
// никакого RPC не заведено. Роль здесь проверяется по JWT claim'у
// напрямую — тот же уровень доверия, что был у backoffice-api ДО Фазы 0
// (RequireRole на claim'е, не через отдельный сервис) — осознанное
// упрощение для этого прохода, не незамеченный откат к дореформенной
// схеме; заводить новый iam-service RPC под один этот сервис — отдельная,
// не обязательная для MVP self-service работа.
package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const AdminRole = "partner-admin"

type realmAccess struct {
	Roles []string `json:"roles"`
}

type Claims struct {
	PartnerID   string      `json:"partner_id"`
	RealmAccess realmAccess `json:"realm_access"`
	jwt.RegisteredClaims
}

func (c *Claims) HasRole(role string) bool {
	for _, r := range c.RealmAccess.Roles {
		if r == role {
			return true
		}
	}
	return false
}

func (c *Claims) IsAdmin() bool { return c.HasRole(AdminRole) }

type Validator struct {
	publicKey *rsa.PublicKey
	audience  string
	issuer    string
}

func NewValidator(publicKey *rsa.PublicKey, audience, issuer string) *Validator {
	return &Validator{publicKey: publicKey, audience: audience, issuer: issuer}
}

var (
	ErrMissingBearer    = errors.New("отсутствует Authorization: Bearer <token>")
	ErrMissingPartnerID = errors.New("токен не содержит claim partner_id")
)

func (v *Validator) ParseBearer(header string) (*Claims, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return nil, ErrMissingBearer
	}
	tokenString := strings.TrimPrefix(header, prefix)

	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("неожидаемый метод подписи: %v", t.Method.Alg())
		}
		return v.publicKey, nil
	}, jwt.WithExpirationRequired(), jwt.WithAudience(v.audience), jwt.WithIssuer(v.issuer))
	if err != nil {
		return nil, fmt.Errorf("проверка токена не пройдена: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("токен невалиден")
	}
	if claims.PartnerID == "" {
		return nil, ErrMissingPartnerID
	}
	return claims, nil
}

type contextKey string

const claimsContextKey contextKey = "auth.claims"

func (v *Validator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := v.ParseBearer(r.Header.Get("Authorization"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := contextWithClaims(r.Context(), claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func contextWithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*Claims)
	return claims, ok
}

// RequireAdmin — mux-уровневый gate поверх Middleware: 403, если
// вызывающий partner-токен не несёт роль partner-admin. Используется для
// деструктивных операций (изменение applications/senders, ротация
// credentials) — partner-viewer может читать, не мутировать.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте (RequireAdmin смонтирован до Middleware?)", http.StatusInternalServerError)
			return
		}
		if !claims.IsAdmin() {
			http.Error(w, "требуется роль partner-admin", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
