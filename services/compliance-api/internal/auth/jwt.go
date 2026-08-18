// Package auth — Keycloak OIDC validation, тот же паттерн, что
// services/backoffice-api/internal/auth/jwt.go. Проверяет подпись RS256 и
// извлекает `sub` (Keycloak subject) — используется и как identity для
// IamService.CheckPermission (permission.go), и как actor в
// policy.subscriber_consent-мутациях (requested_by на CreateVersion). Новый
// сервис (в отличие от backoffice-api до Фазы 0) сразу строится вокруг
// RequirePermission — нет legacy realm_access.roles-разбора, который нужно
// было бы держать ради обратной совместимости.
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

func contextWithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*Claims)
	return claims, ok
}
