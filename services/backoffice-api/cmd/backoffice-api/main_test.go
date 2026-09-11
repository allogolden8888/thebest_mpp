package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
)

func encodePublicKeyPEM(t *testing.T, pub *rsa.PublicKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func genKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return key
}

func TestLoadJWTPreviousPublicKeysEmptyReturnsNilNoError(t *testing.T) {
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_PEM", "")
	keys, err := loadJWTPreviousPublicKeys()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keys != nil {
		t.Fatalf("ожидали nil для пустого env var, получили %d ключей", len(keys))
	}
}

func TestLoadJWTPreviousPublicKeysSingleBlock(t *testing.T) {
	key := genKey(t)
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_PEM", encodePublicKeyPEM(t, &key.PublicKey))

	keys, err := loadJWTPreviousPublicKeys()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("ожидали 1 ключ, получили %d", len(keys))
	}
	if !keys[0].Equal(&key.PublicKey) {
		t.Fatalf("разобранный ключ не совпадает с исходным")
	}
}

// TestLoadJWTPreviousPublicKeysMultipleConcatenatedBlocks — ротация с
// несколькими предыдущими ключами одновременно: один env var, несколько
// конкатенированных PEM-блоков подряд (см. package doc loadJWTPreviousPublicKeys
// про то, почему не JWT_PREVIOUS_PUBLIC_KEY_PEM_1/_2/...).
func TestLoadJWTPreviousPublicKeysMultipleConcatenatedBlocks(t *testing.T) {
	keyA := genKey(t)
	keyB := genKey(t)
	combined := encodePublicKeyPEM(t, &keyA.PublicKey) + encodePublicKeyPEM(t, &keyB.PublicKey)
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_PEM", combined)

	keys, err := loadJWTPreviousPublicKeys()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ожидали 2 ключа, получили %d", len(keys))
	}
	if !keys[0].Equal(&keyA.PublicKey) || !keys[1].Equal(&keyB.PublicKey) {
		t.Fatalf("разобранные ключи не совпадают с исходными в порядке следования")
	}
}

func TestLoadJWTPreviousPublicKeysGarbageReturnsError(t *testing.T) {
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_PEM", "not a pem block at all")
	if _, err := loadJWTPreviousPublicKeys(); err == nil {
		t.Fatalf("ожидали ошибку для мусорного значения без единого разборного PEM-блока")
	}
}

func TestLoadJWTPreviousPublicKeysNonRSABlockReturnsError(t *testing.T) {
	// Валидный PEM-блок, но не PKIX/не RSA-ключ (произвольные байты внутри
	// корректного PEM-конверта) — x509.ParsePKIXPublicKey должен упасть, не
	// x509-парсер тихо съесть мусор.
	block := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("not-a-real-der-payload")})
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_PEM", string(block))
	if _, err := loadJWTPreviousPublicKeys(); err == nil {
		t.Fatalf("ожидали ошибку для PEM-блока с невалидным DER внутри")
	}
	if _, err := loadJWTPreviousPublicKeys(); err != nil && !strings.Contains(err.Error(), "JWT_PREVIOUS_PUBLIC_KEYS_PEM") {
		t.Fatalf("сообщение об ошибке должно называть переменную окружения, получили: %v", err)
	}
}
