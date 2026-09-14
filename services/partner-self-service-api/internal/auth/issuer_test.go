package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"
)

func testKeypair(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация RSA-ключа не удалась: %v", err)
	}
	return key
}

// TestTokenIssuerIssuesTokenValidatorAccepts — the exact round-trip that a
// mismatched JWT_PUBLIC_KEY_PEM/JWT_PRIVATE_KEY_PEM pair in
// docker-compose.yml broke in practice (found live: every self-issued login
// token failed "crypto/rsa: verification error" on the very next request,
// because the Validator there was still wired to the old shared dev
// keypair instead of this issuer's own public half). This test would have
// caught that class of misconfiguration if it existed at the unit level —
// it only catches the issuer/validator code pairing correctly with each
// other, not the deployment config, but it's the cheapest guard against
// regressing the code side of that bug.
func TestTokenIssuerIssuesTokenValidatorAccepts(t *testing.T) {
	key := testKeypair(t)
	const audience = "partner-self-service-api"
	const issuer = "https://keycloak.mpp.svc/realms/mpp"
	tokenIssuer := NewTokenIssuer(key, audience, issuer)
	validator := NewValidator(&key.PublicKey, audience, issuer)

	before := time.Now()
	token, expiresAt, err := tokenIssuer.Issue("click_uz_portal", "click_uz", []string{"partner-admin"})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if expiresAt.Before(before.Add(tokenTTL - time.Minute)) || expiresAt.After(before.Add(tokenTTL+time.Minute)) {
		t.Errorf("expiresAt=%v не в пределах ожидаемого TTL (%v) от %v", expiresAt, tokenTTL, before)
	}

	claims, err := validator.ParseBearer("Bearer " + token)
	if err != nil {
		t.Fatalf("ParseBearer не принял токен, выпущенный Issue: %v", err)
	}
	if claims.Subject != "click_uz_portal" {
		t.Errorf("ожидали sub=click_uz_portal, получили %q", claims.Subject)
	}
	if claims.PartnerID != "click_uz" {
		t.Errorf("ожидали partner_id=click_uz, получили %q", claims.PartnerID)
	}
	if !claims.IsAdmin() {
		t.Errorf("ожидали роль partner-admin в токене")
	}
}

func TestTokenIssuerWrongKeyIsRejectedByValidator(t *testing.T) {
	issuerKey := testKeypair(t)
	otherKey := testKeypair(t)
	const audience = "partner-self-service-api"
	const issuer = "https://keycloak.mpp.svc/realms/mpp"
	tokenIssuer := NewTokenIssuer(issuerKey, audience, issuer)
	// Валидатор с ЧУЖИМ публичным ключом — ровно тот сценарий, который
	// реально произошёл в docker-compose.yml до фикса (JWT_PUBLIC_KEY_PEM
	// там был публичной половиной другого, общего dev-keypair).
	validator := NewValidator(&otherKey.PublicKey, audience, issuer)

	token, _, err := tokenIssuer.Issue("click_uz_portal", "click_uz", nil)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := validator.ParseBearer("Bearer " + token); err == nil {
		t.Fatalf("ожидали отказ валидации токена, подписанного другим ключом")
	}
}

func TestTokenIssuerWrongAudienceIsRejectedByValidator(t *testing.T) {
	key := testKeypair(t)
	tokenIssuer := NewTokenIssuer(key, "some-other-audience", "https://keycloak.mpp.svc/realms/mpp")
	validator := NewValidator(&key.PublicKey, "partner-self-service-api", "https://keycloak.mpp.svc/realms/mpp")

	token, _, err := tokenIssuer.Issue("click_uz_portal", "click_uz", nil)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := validator.ParseBearer("Bearer " + token); err == nil {
		t.Fatalf("ожидали отказ валидации токена с чужим audience")
	}
}
