// Package auth — JWT/Keycloak validation, тот же паттерн, что
// services/partner-api/internal/auth/jwt.go (RS256, обязательные aud/iss,
// partner_id из claim — не из query/path). Тонкий read-only сервис (Фаза 5
// плана закрытия API-пробелов) — нет ролей/RequireAdmin, каждый запрос
// просто ограничен partner_id вызывающего.
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
	PartnerID string `json:"partner_id"`
	jwt.RegisteredClaims
}

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
