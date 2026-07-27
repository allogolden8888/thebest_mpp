// Package auth — Keycloak OIDC validation (services_specifictaion.md §8.3
// "Стек"). Проверяет подпись RS256 и извлекает `sub` (Keycloak subject) как
// идентификатор оператора — используется как `requested_by` в каждой
// аудируемой мутирующей операции (config CRUD, execution control override,
// force scheduler command, replay request — HLD §9.1/§20).
//
// **Открытый вопрос**: `migrations/V017__backoffice_stub.sql` явно
// оставляет полную RBAC-модель (роли/права) отдельному LLD Backoffice API —
// "не специфицируется здесь". Middleware здесь проверяет только подлинность
// токена (кто вызывает), не авторизацию (что вызывающему разрешено) — нет
// ни одного документа, специфицирующего роли/права, которые можно было бы
// проверить.
package auth

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	jwt.RegisteredClaims
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
