// Package auth — issuer.go: luminous-hugging-charm.md, BACKOFFICE_DESIGN_
// SPEC.md Экран 33 "Admin users". До этой фичи backoffice-api только
// ВАЛИДИРОВАЛ JWT (jwt.go) — ни разу не подписывал ни один сам (реальный
// Keycloak не развёрнут нигде в этом репозитории, LoginView.vue принимал
// вставленный JWT в textarea, HTTP-раунд-трипа при логине не было).
// TokenIssuer использует ОТДЕЛЬНЫЙ RSA-keypair, не общий dev-keypair
// остальных self-service API (partner-self-service-api/billing-self-
// service-api/compliance-api) — независимые env-блоки в docker-compose.yml,
// смена значения здесь не трогает остальные три (см. CODE_REVIEW.md про
// риск token-confusion при переиспользовании одного ключа несколькими
// сервисами — новый ключ здесь устраняет этот риск для нового пути,
// а не воспроизводит его).
package auth

import (
	"crypto/rsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// tokenTTL — 8 часов, обычная рабочая смена; локальный логин — временное
// решение до реального Keycloak (BACKOFFICE_ROADMAP.md "Admin users"), не
// повод усложнять refresh-token flow сейчас.
const tokenTTL = 8 * time.Hour

type TokenIssuer struct {
	privateKey *rsa.PrivateKey
}

func NewTokenIssuer(privateKey *rsa.PrivateKey) *TokenIssuer {
	return &TokenIssuer{privateKey: privateKey}
}

// Issue — sub=externalID (staff_accounts.external_id = username, см.
// migrations/V031__staff_accounts.sql), пустой realm_access.roles: Claims
// (jwt.go) парсит это поле только для обратной совместимости, реальная
// авторизация всегда идёт через IamService.CheckPermission по sub
// (permission.go), не через этот claim.
func (i *TokenIssuer) Issue(externalID string) (token string, expiresAt time.Time, err error) {
	now := time.Now()
	expiresAt = now.Add(tokenTTL)

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   externalID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(i.privateKey)
	if err != nil {
		return "", time.Time{}, err
	}
	return signed, expiresAt, nil
}
