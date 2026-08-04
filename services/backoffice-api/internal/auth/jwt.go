// Package auth — Keycloak OIDC validation (services_specifictaion.md §8.3
// "Стек"). Проверяет подпись RS256 и извлекает `sub` (Keycloak subject) как
// идентификатор оператора — используется как `requested_by` в каждой
// аудируемой мутирующей операции (config CRUD, execution control override,
// force scheduler command, replay request — HLD §9.1/§20).
//
// **RBAC** (CODE_REVIEW.md CRITICAL finding): `migrations/V017__backoffice_stub.sql`
// оставляет полную RBAC-модель (роли/права/SSO-интеграция, привязанные к
// backoffice.users) отдельному будущему LLD Backoffice API — этого документа
// в репозитории нет. До прошлого прохода ревью middleware проверял только
// подлинность токена (кто вызывает), не авторизацию (что вызывающему
// разрешено): любой валидный токен любого пользователя realm'а — включая
// read-only support-аккаунт — мог поставить платформу на паузу через
// execution-control override. hld.md §25 явно требует "RBAC в Backoffice";
// полную ролевую модель без LLD изобретать нельзя, но состояние "вообще без
// проверки прав" для сервиса, который может остановить обработку всего
// трафика платформы, недопустимо само по себе. Минимальный, не
// изобретающий лишнего барьер: разбор стандартного Keycloak-claim
// `realm_access.roles` (JWT спецификацией не описан, но это стандартная
// структура токенов Keycloak, единственного описанного здесь IdP,
// services_specifictaion.md §8.3) и требование роли `backoffice-admin` для
// всех деструктивных операций (config CRUD/archive, execution-control
// override/clear, force-scheduler-command, replay); read-only browse/report
// эндпоинты по-прежнему доступны любому валидному токену realm'а. Полная
// модель (гранулярные permission, привязка к backoffice.users, UI ролей) —
// по-прежнему отдельная задача, ждущая LLD.
package auth

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// AdminRole — единственная роль, которую этот срез умеет проверять;
// требуется для любого HTTP-маршрута, вызывающего деструктивную операцию
// (см. package doc). Имя согласовано с той же ролью в других сервисах
// Backoffice-контура (backoffice-ui читает её из того же токена).
const AdminRole = "backoffice-admin"

type realmAccess struct {
	Roles []string `json:"roles"`
}

type Claims struct {
	jwt.RegisteredClaims
	RealmAccess realmAccess `json:"realm_access"`
}

// HasRole — true, если токен несёт указанную Keycloak realm-роль.
func (c *Claims) HasRole(role string) bool {
	for _, r := range c.RealmAccess.Roles {
		if r == role {
			return true
		}
	}
	return false
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

// RequireRole — mux-уровневый gate поверх Middleware: 403, если аутентифи-
// цированный вызывающий не несёт указанную realm-роль. Должен монтироваться
// только на маршруты, уже прошедшие Middleware (полагается на claims в
// контексте — 500, если её там нет, что означает ошибку монтирования
// роутера, а не рантайм-состояние вызывающего).
func RequireRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFromContext(r.Context())
			if !ok {
				http.Error(w, "нет claims в контексте (RequireRole смонтирован до Middleware?)", http.StatusInternalServerError)
				return
			}
			if !claims.HasRole(role) {
				http.Error(w, fmt.Sprintf("требуется роль %q", role), http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
