package auth

import (
	"testing"
	"time"
)

func TestTokenIssuerIssuesTokenValidatorAccepts(t *testing.T) {
	key := testKeypair(t)
	issuer := NewTokenIssuer(key)
	validator := NewValidator(&key.PublicKey)

	before := time.Now()
	token, expiresAt, err := issuer.Issue("alice")
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
	if claims.Subject != "alice" {
		t.Errorf("ожидали sub=alice, получили %q", claims.Subject)
	}
}

func TestTokenIssuerWrongKeyIsRejectedByValidator(t *testing.T) {
	issuerKey := testKeypair(t)
	otherKey := testKeypair(t)
	issuer := NewTokenIssuer(issuerKey)
	// Валидатор с ЧУЖИМ публичным ключом — тот самый сценарий, который
	// internal/auth/issuer.go package doc называет причиной завести
	// отдельный keypair для backoffice-api, а не переиспользовать общий
	// dev-keypair остальных self-service API.
	validator := NewValidator(&otherKey.PublicKey)

	token, _, err := issuer.Issue("alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := validator.ParseBearer("Bearer " + token); err == nil {
		t.Fatalf("ожидали отказ валидации токена, подписанного другим ключом")
	}
}
