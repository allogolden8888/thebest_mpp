package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
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

// signToken — kid проставляется автоматически из ключа подписи (KeyID),
// ровно та же логика, что реальный TokenIssuer.Issue (issuer.go) — тесты
// ниже верифицируют поведение Validator, а не то, что он как-то особо
// снисходителен к токенам без kid.
func signToken(t *testing.T, key *rsa.PrivateKey, subject string, expiry time.Time) string {
	t.Helper()
	return signTokenWithKid(t, key, KeyID(&key.PublicKey), subject, expiry)
}

// signTokenWithKid — тот же signToken, но kid можно задать явно (или
// пустым/отсутствующим, оставив header без kid), для тестов ротации/
// граничных случаев (TestParseBearer*Kid*), которым signToken не хватает.
func signTokenWithKid(t *testing.T, key *rsa.PrivateKey, kid, subject string, expiry time.Time) string {
	t.Helper()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			ExpiresAt: jwt.NewNumericDate(expiry),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись токена не удалась: %v", err)
	}
	return signed
}

func TestParseBearerAcceptsValidToken(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "ops@mpp", time.Now().Add(time.Hour))

	claims, err := v.ParseBearer("Bearer " + signed)
	if err != nil {
		t.Fatalf("ParseBearer failed: %v", err)
	}
	if claims.Subject != "ops@mpp" {
		t.Fatalf("sub = %q, want ops@mpp", claims.Subject)
	}
}

func TestParseBearerRejectsMissingPrefix(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "ops@mpp", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer(signed); err != ErrMissingBearer {
		t.Fatalf("ожидали ErrMissingBearer, получили %v", err)
	}
}

func TestParseBearerRejectsExpiredToken(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "ops@mpp", time.Now().Add(-time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для истёкшего токена")
	}
}

func TestParseBearerRejectsWrongSigningKey(t *testing.T) {
	key := testKeypair(t)
	otherKey := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, otherKey, "ops@mpp", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err == nil {
		t.Fatalf("ожидали ошибку для токена, подписанного чужим ключом")
	}
}

func TestParseBearerRejectsMissingSubject(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signToken(t, key, "", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); err != ErrMissingSubject {
		t.Fatalf("ожидали ErrMissingSubject, получили %v", err)
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
	signed := signToken(t, key, "ops@mpp", time.Now().Add(time.Hour))

	var gotSubject string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := ClaimsFromContext(r.Context())
		if !ok {
			t.Fatalf("claims отсутствуют в контексте")
		}
		gotSubject = claims.Subject
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	v.Middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", rec.Code)
	}
	if gotSubject != "ops@mpp" {
		t.Fatalf("sub в контексте = %q, want ops@mpp", gotSubject)
	}
}

// ---- JWKS/kid rotation (keys.go, BACKOFFICE_ROADMAP.md P0 "секреты") ----

func TestParseBearerRejectsTokenWithoutKid(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	signed := signTokenWithKid(t, key, "", "ops@mpp", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); !errors.Is(err, ErrMissingKid) {
		t.Fatalf("ожидали ErrMissingKid, получили %v", err)
	}
}

func TestParseBearerRejectsUnknownKid(t *testing.T) {
	key := testKeypair(t)
	v := NewValidator(&key.PublicKey)
	// Подписано ТЕМ ЖЕ ключом, что знает валидатор, но с посторонним kid —
	// доказывает, что выбор ключа идёт по kid, а не "раз ключ один, kid не
	// важен": даже правильный ключ с чужим kid должен быть отвергнут.
	signed := signTokenWithKid(t, key, "not-a-real-kid", "ops@mpp", time.Now().Add(time.Hour))

	if _, err := v.ParseBearer("Bearer " + signed); !errors.Is(err, ErrUnknownKid) {
		t.Fatalf("ожидали ErrUnknownKid, получили %v", err)
	}
}

// TestKeyRotationGraceWindow — сценарий из BACKOFFICE_ROADMAP.md/задачи
// целиком: подписываем ключом A, валидируем (ok) -> подписываем ключом B
// (другой kid), А ещё в наборе -> ОБА валидируются -> A убирается из
// набора (ротация завершена) -> токены A больше не проходят, токены B
// по-прежнему проходят.
func TestKeyRotationGraceWindow(t *testing.T) {
	keyA := testKeypair(t)
	keyB := testKeypair(t)

	tokenA := signToken(t, keyA, "alice", time.Now().Add(time.Hour))
	tokenB := signToken(t, keyB, "bob", time.Now().Add(time.Hour))

	// Grace window: и текущий (B), и предыдущий (A) ключ действительны одновременно.
	v := NewValidatorFromKeys(&keyB.PublicKey, &keyA.PublicKey)

	claimsA, err := v.ParseBearer("Bearer " + tokenA)
	if err != nil {
		t.Fatalf("токен A должен проходить внутри grace window: %v", err)
	}
	if claimsA.Subject != "alice" {
		t.Errorf("sub = %q, want alice", claimsA.Subject)
	}

	claimsB, err := v.ParseBearer("Bearer " + tokenB)
	if err != nil {
		t.Fatalf("токен B (текущий ключ) должен проходить: %v", err)
	}
	if claimsB.Subject != "bob" {
		t.Errorf("sub = %q, want bob", claimsB.Subject)
	}

	// Ротация завершена: A выведен из набора (был единственным previous).
	vAfterRotation := NewValidator(&keyB.PublicKey)
	if _, err := vAfterRotation.ParseBearer("Bearer " + tokenA); !errors.Is(err, ErrUnknownKid) {
		t.Fatalf("после удаления A из набора токен A должен отвергаться с ErrUnknownKid, получили %v", err)
	}
	if _, err := vAfterRotation.ParseBearer("Bearer " + tokenB); err != nil {
		t.Fatalf("токен B должен по-прежнему проходить после ротации: %v", err)
	}
}

func TestValidatorJWKSContainsAllKeys(t *testing.T) {
	current := testKeypair(t)
	previous := testKeypair(t)
	v := NewValidatorFromKeys(&current.PublicKey, &previous.PublicKey)

	set := v.JWKS()
	if len(set.Keys) != 2 {
		t.Fatalf("ожидали 2 ключа в JWKS, получили %d", len(set.Keys))
	}

	wantKids := map[string]bool{
		KeyID(&current.PublicKey):  false,
		KeyID(&previous.PublicKey): false,
	}
	for _, jwk := range set.Keys {
		if jwk.Kty != "RSA" || jwk.Use != "sig" || jwk.Alg != "RS256" {
			t.Errorf("неожиданные метаданные JWK: %+v", jwk)
		}
		if jwk.N == "" || jwk.E == "" {
			t.Errorf("JWK без n/e: %+v", jwk)
		}
		if _, ok := wantKids[jwk.Kid]; !ok {
			t.Errorf("неожиданный kid в JWKS: %s", jwk.Kid)
		}
		wantKids[jwk.Kid] = true
	}
	for kid, seen := range wantKids {
		if !seen {
			t.Errorf("kid %s отсутствует в JWKS", kid)
		}
	}
}