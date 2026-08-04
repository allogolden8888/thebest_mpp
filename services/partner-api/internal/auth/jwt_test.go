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

const (
	testAudience = "partner-api"
	testIssuer   = "https://keycloak.mpp.svc/realms/mpp"
)

func testKeypair(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация RSA-ключа не удалась: %v", err)
	}
	return key
}

func newTestValidator(pubKey *rsa.PublicKey) *Validator {
	return NewValidator(pubKey, testAudience, testIssuer)
}

func signToken(t *testing.T, key *rsa.PrivateKey, partnerID string, expiry time.Time) string {
	t.Helper()
	return signTokenWithClaims(t, key, partnerID, expiry, testAudience, testIssuer, true)
}

func signTokenWithClaims(t *testing.T, key *rsa.PrivateKey, partnerID string, expiry time.Time, audience, issuer string, includeExpiry bool) string {
	t.Helper()
	registered := jwt.RegisteredClaims{
		Issuer: issuer,
	}
	if audience != "" {
		registered.Audience = jwt.ClaimStrings{audience}
	}
	if includeExpiry {
		registered.ExpiresAt = jwt.NewNumericDate(expiry)
	}
	claims := Claims{
		PartnerID:        partnerID,
		RegisteredClaims: registered,
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
	v := newTestValidator(&key.PublicKey)
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
	v := newTestValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer(signed); err != ErrMissingBearer {
		t.Fatalf("ожидали ErrMissingBearer, получили %v", err)
	}
}

func TestParseBearerRejectsExpiredToken(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signToken(t, key, "acme", time.Now().Add(-time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для истёкшего токена")
	}
}

func TestParseBearerRejectsWrongSigningKey(t *testing.T) {
	key := testKeypair(t)
	otherKey := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signToken(t, otherKey, "acme", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена, подписанного чужим ключом")
	}
}

func TestParseBearerRejectsMissingPartnerID(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signToken(t, key, "", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err != ErrMissingPartnerID {
		t.Fatalf("ожидали ErrMissingPartnerID, получили %v", err)
	}
}

// TestParseBearerRejectsWrongAudience — CODE_REVIEW.md HIGH finding: токен,
// подписанный тем же ключом (тот же Keycloak realm), но выпущенный для
// другого клиента (aud), раньше принимался здесь без проверки.
func TestParseBearerRejectsWrongAudience(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signTokenWithClaims(t, key, "acme", time.Now().Add(time.Hour), "some-other-client", testIssuer, true)

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена с чужим aud")
	}
}

// TestParseBearerRejectsWrongIssuer — тот же класс риска, что и aud выше.
func TestParseBearerRejectsWrongIssuer(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signTokenWithClaims(t, key, "acme", time.Now().Add(time.Hour), testAudience, "https://keycloak.mpp.svc/realms/other", true)

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена с чужим iss")
	}
}

// TestParseBearerRejectsMissingExpiry — CODE_REVIEW.md Low finding:
// jwt.WithExpirationRequired() раньше не использовался, токен без claim exp
// принимался как никогда не истекающий.
func TestParseBearerRejectsMissingExpiry(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)
	signed := signTokenWithClaims(t, key, "acme", time.Time{}, testAudience, testIssuer, false)

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена без claim exp")
	}
}

func TestMiddlewareRejectsWithout401(t *testing.T) {
	key := testKeypair(t)
	v := newTestValidator(&key.PublicKey)

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
	v := newTestValidator(&key.PublicKey)
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
