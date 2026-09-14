package kafkaio

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя
// HLD требует ACL": dlr-correlation-writer — один из трёх пилотных клиентов
// нового SASL_SCRAM+TLS листенера (см. consumer.go SaslConfig doc comment).
// Эти тесты покрывают ТОЛЬКО построение конфигурации (env parsing + PEM
// разбор) — без живого брокера, которого в CI нет; live round-trip против
// SASL-листенера этим не проверяется (см. BACKOFFICE_ROADMAP.md за честной
// формулировкой того, что реально проверено).

func clearSaslEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"KAFKA_SASL_BOOTSTRAP_SERVERS",
		"KAFKA_SASL_USERNAME",
		"KAFKA_SASL_PASSWORD",
		"KAFKA_SASL_MECHANISM",
		"KAFKA_TLS_CA_PATH",
	} {
		old, existed := os.LookupEnv(key)
		os.Unsetenv(key)
		if existed {
			t.Cleanup(func() { os.Setenv(key, old) })
		}
	}
}

func TestSaslConfigFromEnvIsNilWhenNoVarsAreSet(t *testing.T) {
	clearSaslEnv(t)
	if cfg := SaslConfigFromEnv(); cfg != nil {
		t.Fatalf("ожидали nil (сервис остаётся на plaintext), получили %+v", cfg)
	}
}

func TestSaslConfigFromEnvIsNilWhenOnlySomeVarsAreSet(t *testing.T) {
	// Частичная конфигурация (например, опечатка в rollout, потерявшая одну
	// переменную) не должна тихо наполовину аутентифицироваться — это
	// известный fail-safe, не полнота случайного стечения.
	clearSaslEnv(t)
	t.Setenv("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094")
	t.Setenv("KAFKA_SASL_USERNAME", "dlr-correlation-writer-kafka-user")
	// password и CA path намеренно не заданы.

	if cfg := SaslConfigFromEnv(); cfg != nil {
		t.Fatalf("ожидали nil при неполном наборе переменных, получили %+v", cfg)
	}
}

func TestSaslConfigFromEnvDefaultsMechanismToScramSha512(t *testing.T) {
	clearSaslEnv(t)
	t.Setenv("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094")
	t.Setenv("KAFKA_SASL_USERNAME", "dlr-correlation-writer-kafka-user")
	t.Setenv("KAFKA_SASL_PASSWORD", "s3cr3t")
	t.Setenv("KAFKA_TLS_CA_PATH", "/etc/mpp/kafka-tls/ca.crt")

	cfg := SaslConfigFromEnv()
	if cfg == nil {
		t.Fatal("ожидали непустой конфиг при полном наборе переменных")
	}
	if cfg.Mechanism != "SCRAM-SHA-512" {
		t.Fatalf("Mechanism = %q, want SCRAM-SHA-512 (дефолт при отсутствии KAFKA_SASL_MECHANISM)", cfg.Mechanism)
	}
	if cfg.BootstrapServers != "sasl-broker:9094" || cfg.Username != "dlr-correlation-writer-kafka-user" || cfg.Password != "s3cr3t" {
		t.Fatalf("поля конфига не совпадают с заданными env: %+v", cfg)
	}
}

// selfSignedCAPEM — минимальный валидный самоподписанный сертификат,
// сгенерированный in-memory: реальный Strimzi cluster CA (ca.crt) —
// такой же PEM-encoded X.509, просто выпущенный Strimzi CA, а не тестом.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("сгенерировать ключ: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-mpp-kafka-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("создать сертификат: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestSaslClientOptsSucceedsWithValidCAFile(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, selfSignedCAPEM(t), 0o600); err != nil {
		t.Fatalf("записать тестовый CA-файл: %v", err)
	}

	cfg := &SaslConfig{
		BootstrapServers: "sasl-broker:9094",
		Username:         "dlr-correlation-writer-kafka-user",
		Password:         "s3cr3t",
		Mechanism:        "SCRAM-SHA-512",
		CAPath:           caPath,
	}
	opts, err := saslClientOpts(cfg)
	if err != nil {
		t.Fatalf("saslClientOpts: %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("ожидали ровно 2 kgo.Opt (SASL + TLS dialer), получили %d", len(opts))
	}
}

func TestSaslClientOptsRejectsMissingCAFile(t *testing.T) {
	cfg := &SaslConfig{
		BootstrapServers: "sasl-broker:9094",
		Username:         "dlr-correlation-writer-kafka-user",
		Password:         "s3cr3t",
		Mechanism:        "SCRAM-SHA-512",
		CAPath:           filepath.Join(t.TempDir(), "does-not-exist.crt"),
	}
	if _, err := saslClientOpts(cfg); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий CA-файл")
	}
}

func TestSaslClientOptsRejectsInvalidPEM(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, []byte("this is not a PEM certificate"), 0o600); err != nil {
		t.Fatalf("записать тестовый CA-файл: %v", err)
	}
	cfg := &SaslConfig{
		BootstrapServers: "sasl-broker:9094",
		Username:         "dlr-correlation-writer-kafka-user",
		Password:         "s3cr3t",
		Mechanism:        "SCRAM-SHA-512",
		CAPath:           caPath,
	}
	if _, err := saslClientOpts(cfg); err == nil {
		t.Fatal("ожидали ошибку на невалидный PEM")
	}
}

func TestSaslClientOptsRejectsUnsupportedMechanism(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, selfSignedCAPEM(t), 0o600); err != nil {
		t.Fatalf("записать тестовый CA-файл: %v", err)
	}
	cfg := &SaslConfig{
		BootstrapServers: "sasl-broker:9094",
		Username:         "dlr-correlation-writer-kafka-user",
		Password:         "s3cr3t",
		Mechanism:        "PLAIN",
		CAPath:           caPath,
	}
	if _, err := saslClientOpts(cfg); err == nil {
		t.Fatal("ожидали ошибку на неподдерживаемый механизм (сегодня поддержан только SCRAM-SHA-512)")
	}
}
