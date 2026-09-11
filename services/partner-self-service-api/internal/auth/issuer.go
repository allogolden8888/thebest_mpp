// Package auth — issuer.go: BACKOFFICE_ROADMAP.md Production Readiness
// Review P0#5 ("Identity-контур не производственного класса"). До этого
// файла partner-self-service-api только ВАЛИДИРОВАЛ JWT (jwt.go) — партнёр
// не мог реально войти, partner-portal-ui's LoginView.vue принимал
// вставленный вручную JWT в textarea (см. git history), никакого HTTP-
// раунд-трипа при логине не было и ни одной живой строки в
// iam.partner_portal_users с паролем не существовало (см. package doc
// platform-contracts/grpc/iam.proto).
//
// Отдельный RSA-keypair, НЕ общий dev-keypair, который сегодня разделяют
// partner-self-service-api/billing-self-service-api/compliance-api в
// infra/docker/docker-compose.yml — тот же вывод, что уже сделан для
// backoffice-api (internal/auth/issuer.go там, BACKOFFICE_DESIGN_SPEC.md
// Экран 33 "Admin users"): сервис, который сам ПОДПИСЫВАЕТ токены, несёт
// принципиально другое доверие, чем сервис, который их только проверяет.
// Общий dev-keypair трёх self-service API был безопасен, пока все трое были
// чистыми ВАЛИДАТОРАМИ чужих (гипотетических Keycloak) токенов — теперь,
// когда partner-self-service-api сам выпускает токены, компрометация или
// ошибка в этом коде создавала бы токены, которые billing-self-service-api/
// compliance-api тоже приняли бы как валидные (тот же public key), хотя они
// никогда не собирались доверять именно этому issuer'у. Отдельный ключ
// устраняет этот риск для нового пути, не переделывая два остальных сервиса
// сейчас (они остаются чистыми валидаторами).
package auth

import (
	"crypto/rsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// tokenTTL — 8 часов, тот же выбор и то же обоснование, что backoffice-api
// (internal/auth/issuer.go там): временное решение до реального Keycloak
// realm/client для партнёрских людей-пользователей (см. package doc jwt.go
// "Фаза 3 плана"), не повод усложнять refresh-token flow сейчас.
const tokenTTL = 8 * time.Hour

// TokenIssuer — в отличие от backoffice-api, Validator этого сервиса
// (jwt.go) ТРЕБУЕТ aud/iss (готовился и для реального будущего Keycloak
// realm'а, не только для локального логина) и не делает kid-based key
// rotation (один статический ключ, см. package doc — JWKS/kid rotation для
// ЭТОГО сервиса осознанно не в этом заходе, отдельная будущая работа). Issue
// поэтому обязан сам проставить те же audience/issuer, которые Validator
// будет требовать при проверке — TokenIssuer и Validator конфигурируются
// одними и теми же env var'ами в main.go (PARTNER_SELF_SERVICE_API_JWT_
// AUDIENCE/_ISSUER), не независимо друг от друга.
type TokenIssuer struct {
	privateKey *rsa.PrivateKey
	audience   string
	issuer     string
}

func NewTokenIssuer(privateKey *rsa.PrivateKey, audience, issuer string) *TokenIssuer {
	return &TokenIssuer{privateKey: privateKey, audience: audience, issuer: issuer}
}

// Issue — sub=externalID (iam.partner_portal_users.external_id = username,
// см. migrations/V035__partner_portal_credentials.sql), partner_id/roles —
// снимок на момент логина, ТОЛЬКО для немедленного отображения в
// partner-portal-ui сразу после входа (jwtRoles.ts decodeRealmRoles/
// decodePartnerID — чисто клиентский UX-декодинг, не проверяет подпись, см.
// комментарий там). Реальная авторизация на КАЖДЫЙ последующий запрос
// проходит через ResolveLiveAccess (resolve.go), который перезаписывает оба
// поля живым результатом IamService.ResolvePartnerPortalAccess — то, что
// здесь записано в сам JWT, никогда не читается сервером повторно и не
// является источником истины после первого запроса. Если роль отозвана
// посреди 8-часовой сессии, follow-up запросы всё равно корректно
// получат 403/401 через ResolveLiveAccess — только nav в UI до следующего
// логина будет визуально устаревшим, тот же класс уже принятого компромисса,
// что и в backoffice-ui (там ADMIN_ROLE/isAdmin() тоже читается из JWT
// исключительно как UX-слой, см. jwtRoles.ts doc-комментарий).
func (i *TokenIssuer) Issue(externalID, partnerID string, roles []string) (token string, expiresAt time.Time, err error) {
	now := time.Now()
	expiresAt = now.Add(tokenTTL)

	claims := &Claims{
		PartnerID:   partnerID,
		RealmAccess: realmAccess{Roles: roles},
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   externalID,
			Audience:  jwt.ClaimStrings{i.audience},
			Issuer:    i.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}

	jwtToken := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := jwtToken.SignedString(i.privateKey)
	if err != nil {
		return "", time.Time{}, err
	}
	return signed, expiresAt, nil
}
