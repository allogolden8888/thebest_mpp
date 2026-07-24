package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func testKeypair(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация RSA-ключа не удалась: %v", err)
	}
	return key
}

func signToken(t *testing.T, key *rsa.PrivateKey, partnerID string, expiry time.Time) string {
	t.Helper()
	claims := Claims{
		PartnerID: partnerID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiry),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись токена не удалась: %v", err)
	}
	return signed
}

func TestParseBearerAcceptsValidToken(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(time.Hour))

	claims, err := v.ParseBearer("Bearer " + signed)
	if err != nil {
		t.Fatalf("ParseBearer failed: %v", err)
	}
	if claims.PartnerID != "acme" {
		t.Fatalf("partner_id = %q, want acme", claims.PartnerID)
	}
}

func TestParseBearerRejectsMissingPrefix(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer(signed); err != ErrMissingBearer {
		t.Fatalf("ожидали ErrMissingBearer, получили %v", err)
	}
}

func TestParseBearerRejectsExpiredToken(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(-time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для истёкшего токена")
	}
}

func TestParseBearerRejectsWrongSigningKey(t *testing.T) {
	key := testKeypair(t)
	otherKey := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, otherKey, "acme", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена, подписанного чужим ключом")
	}
}

func TestParseBearerRejectsMissingPartnerID(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err != ErrMissingPartnerID {
		t.Fatalf("ожидали ErrMissingPartnerID, получили %v", err)
	}
}

func TestMiddlewareRejectsWithout401(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handlerCalled = true })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	v.Middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ожидали 401, получили %d", rec.Code)
	}
	if handlerCalled {
		t.Fatalf("next не должен вызываться без валидного токена")
	}
}

func TestMiddlewarePassesClaimsToContext(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(time.Hour))

	var gotPartnerID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := ClaimsFromContext(r.Context())
		if !ok {
			t.Fatalf("claims отсутствуют в контексте")
		}
		gotPartnerID = claims.PartnerID
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	v.Middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", rec.Code)
	}
	if gotPartnerID != "acme" {
		t.Fatalf("partner_id в контексте = %q, want acme", gotPartnerID)
	}
}