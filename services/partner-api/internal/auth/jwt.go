// Package auth — JWT / Keycloak validation (services_specifictaion.md §8.2
// "Стек"). Проверяет подпись RS256 (стандартная схема Keycloak) и извлекает
// claim `partner_id`, которым дальше scoped-ится каждый PostgreSQL/ClickHouse
// запрос — партнёр не должен получить доступ к чужим сообщениям через
// подмену query-параметра.
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
	PartnerID string `json:"partner_id"`
	jwt.RegisteredClaims
}

type Validator struct {
	publicKey *rsa.PublicKey
}

func NewValidator(publicKey *rsa.PublicKey) *Validator {
	return &Validator{publicKey: publicKey}
}

var (
	ErrMissingBearer = errors.New("отсутствует Authorization: Bearer <token>")
	ErrMissingPartnerID = errors.New("токен не содержит claim partner_id")
)

// ParseBearer — извлекает и валидирует JWT из заголовка Authorization.
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
	if claims.PartnerID == "" {
		return nil, ErrMissingPartnerID
	}
	return claims, nil
}

type contextKey string

const claimsContextKey contextKey = "auth.claims"

// Middleware — chi/net-http middleware: 401 без валидного токена, иначе
// кладёт *Claims в контекст запроса (ClaimsFromContext).
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