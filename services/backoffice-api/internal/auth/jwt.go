// Package auth — Keycloak OIDC validation (services_specifictaion.md §8.3
// "Стек"). Проверяет подпись RS256 и извлекает `sub` (Keycloak subject) как
// идентификатор оператора — используется как `requested_by` в каждой
// аудируемой мутирующей операции (config CRUD, execution control override,
// force scheduler command, replay request — HLD §9.1/§20).
//
// **RBAC** (CODE_REVIEW.md CRITICAL finding, luminous-hugging-charm.md
// Фаза 0): до Фазы 0 middleware проверял только подлинность токена (кто
// вызывает), не авторизацию (что вызывающему разрешено) — любой валидный
// токен любого пользователя realm'а, включая read-only support-аккаунт, мог
// поставить платформу на паузу через execution-control override.
// Промежуточный барьер (единственная бинарная роль `backoffice-admin` из
// `realm_access.roles`, `RequireRole`/`AdminRole`) закрывал это "вообще без
// проверки прав" состояние, но не давал гранулярности — Фаза 0 добавила
// `iam-service` (реальная RBAC-модель, `migrations/V025__iam.sql`) и
// `RequirePermission` (`permission.go`), проверяющий КОНКРЕТНОЕ право через
// `IamService.CheckPermission` на каждый мутирующий запрос вместо разбора
// claim'а на месте. `RequireRole`/`AdminRole` удалены как мёртвый код после
// того, как router.go перешёл на `RequirePermission` — обратная
// совместимость с токенами `backoffice-admin` обеспечена на уровне
// `iam.roles` (суперроль с полным набором прав, см. iam-service/README.md),
// не здесь.
package auth

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type realmAccess struct {
	Roles []string `json:"roles"`
}

// RealmAccess — сохранён на Claims для обратной совместимости разбора
// токена (Keycloak всегда несёт этот claim), но больше не используется для
// авторизационных решений здесь — см. package doc: гранулярные права теперь
// проверяются через IamService.CheckPermission (permission.go), не разбором
// realm_access.roles на месте.
type Claims struct {
	jwt.RegisteredClaims
	RealmAccess realmAccess `json:"realm_access"`
}

type Validator struct {
	publicKey *rsa.PublicKey
}

func NewValidator(publicKey *rsa.PublicKey) *Validator {
	return &Validator{publicKey: publicKey}
}

var (
	ErrMissingBearer  = errors.New("отсутствует Authorization: Bearer <token>")
	ErrMissingSubject = errors.New("токен не содержит claim sub")
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
	})
	if err != nil {
		return nil, fmt.Errorf("проверка токена не пройдена: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("токен невалиден")
	}
	if claims.Subject == "" {
		return nil, ErrMissingSubject
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
