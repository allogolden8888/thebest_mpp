package webhookauth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConfigReader struct {
	refs   map[string]string
	errFor map[string]error
	calls  atomic.Int64
}

func (f *fakeConfigReader) WebhookCredentialRef(_ context.Context, operatorID string) (string, bool, error) {
	f.calls.Add(1)
	if err, ok := f.errFor[operatorID]; ok {
		return "", false, err
	}
	ref, ok := f.refs[operatorID]
	return ref, ok, nil
}

type fakeVaultReader struct {
	values map[string]string // kvPath+"/"+property -> value
	errFor map[string]error
	calls  atomic.Int64
}

func (f *fakeVaultReader) ReadKV2Property(_ context.Context, kvPath, property string) (string, error) {
	f.calls.Add(1)
	key := kvPath + "/" + property
	if err, ok := f.errFor[key]; ok {
		return "", err
	}
	value, ok := f.values[key]
	if !ok {
		return "", errors.New("not found")
	}
	return value, nil
}

func TestExpectedTokenResolvesThroughConfigAndVault(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{"beeline_uz": "vault://operators/beeline_uz/webhook_bearer"}}
	vaultReader := &fakeVaultReader{values: map[string]string{"operators/beeline_uz/webhook_bearer": "real-secret"}}
	lookup := New(config, vaultReader)

	token, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || token != "real-secret" {
		t.Fatalf("found=%v token=%q, want found=true token=real-secret", found, token)
	}
}

func TestExpectedTokenNotFoundWhenOperatorNotConfigured(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{}}
	vaultReader := &fakeVaultReader{}
	lookup := New(config, vaultReader)

	_, found, err := lookup.ExpectedToken(context.Background(), "unknown_operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected found=false for unconfigured operator")
	}
	if vaultReader.calls.Load() != 0 {
		t.Fatalf("не должен обращаться к Vault, когда Configuration Redis не знает оператора, calls=%d", vaultReader.calls.Load())
	}
}

func TestExpectedTokenFailsClosedOnConfigReaderError(t *testing.T) {
	config := &fakeConfigReader{errFor: map[string]error{"beeline_uz": errors.New("redis недоступен")}}
	vaultReader := &fakeVaultReader{}
	lookup := New(config, vaultReader)

	_, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err == nil {
		t.Fatal("ожидали ошибку")
	}
	if found {
		t.Fatal("found должен быть false при ошибке")
	}
}

func TestExpectedTokenFailsClosedOnMalformedCredentialRef(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{"beeline_uz": "not-a-vault-ref"}}
	vaultReader := &fakeVaultReader{}
	lookup := New(config, vaultReader)

	_, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err == nil {
		t.Fatal("ожидали ошибку разбора credential_ref")
	}
	if found {
		t.Fatal("found должен быть false")
	}
}

func TestExpectedTokenFailsClosedOnVaultReadError(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{"beeline_uz": "vault://operators/beeline_uz/webhook_bearer"}}
	vaultReader := &fakeVaultReader{errFor: map[string]error{"operators/beeline_uz/webhook_bearer": errors.New("vault недоступен")}}
	lookup := New(config, vaultReader)

	_, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err == nil {
		t.Fatal("ожидали ошибку от Vault")
	}
	if found {
		t.Fatal("found должен быть false")
	}
}

// TestCacheHitMakesZeroAdditionalRequests — мирроринг
// partner-rest-receiver::vault_auth cache_hit_against_fake_server_makes_zero_additional_requests.
func TestCacheHitMakesZeroAdditionalRequests(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{"beeline_uz": "vault://operators/beeline_uz/webhook_bearer"}}
	vaultReader := &fakeVaultReader{values: map[string]string{"operators/beeline_uz/webhook_bearer": "secret-abc"}}
	lookup := New(config, vaultReader) // дефолтный TTL=30с, точно не истечёт за тест

	token, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err != nil || !found || token != "secret-abc" {
		t.Fatalf("первый вызов: token=%q found=%v err=%v", token, found, err)
	}
	if config.calls.Load() != 1 || vaultReader.calls.Load() != 1 {
		t.Fatalf("первый вызов должен был обратиться к обоим ровно один раз: config=%d vault=%d", config.calls.Load(), vaultReader.calls.Load())
	}

	for range 5 {
		token, found, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
		if err != nil || !found || token != "secret-abc" {
			t.Fatalf("повторный вызов: token=%q found=%v err=%v", token, found, err)
		}
	}
	if config.calls.Load() != 1 || vaultReader.calls.Load() != 1 {
		t.Fatalf("последующие вызовы в пределах TTL не должны делать НИ ОДНОГО дополнительного запроса: config=%d vault=%d", config.calls.Load(), vaultReader.calls.Load())
	}
}

// TestTTLExpiryTriggersExactlyOneFreshRequest — мирроринг
// partner-rest-receiver::vault_auth ttl_expiry_triggers_exactly_one_fresh_request:
// доказывает, что ротация секрета (изменение значения в Vault "за спиной"
// кеша) подхватывается после истечения TTL, не раньше.
func TestTTLExpiryTriggersExactlyOneFreshRequest(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{"beeline_uz": "vault://operators/beeline_uz/webhook_bearer"}}
	vaultReader := &fakeVaultReader{values: map[string]string{"operators/beeline_uz/webhook_bearer": "secret-v1"}}
	lookup := New(config, vaultReader)
	lookup.TTL = 150 * time.Millisecond

	token, _, err := lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err != nil || token != "secret-v1" {
		t.Fatalf("первый вызов: token=%q err=%v", token, err)
	}
	if vaultReader.calls.Load() != 1 {
		t.Fatalf("ожидали 1 вызов Vault, получили %d", vaultReader.calls.Load())
	}

	// Ротация "за спиной" кеша.
	vaultReader.values["operators/beeline_uz/webhook_bearer"] = "secret-v2"

	// Всё ещё внутри TTL — должен использоваться кеш (value-v1).
	token, _, err = lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err != nil || token != "secret-v1" {
		t.Fatalf("в пределах TTL ожидали кешированное secret-v1, получили %q (err=%v)", token, err)
	}

	time.Sleep(200 * time.Millisecond)

	token, _, err = lookup.ExpectedToken(context.Background(), "beeline_uz")
	if err != nil || token != "secret-v2" {
		t.Fatalf("после истечения TTL ожидали secret-v2, получили %q (err=%v)", token, err)
	}
	if vaultReader.calls.Load() != 2 {
		t.Fatalf("истечение TTL должно вызвать РОВНО один новый запрос, получили calls=%d", vaultReader.calls.Load())
	}
}

// TestDifferentOperatorsAreIndependentInCache — два оператора с разными
// секретами не должны смешиваться в кеше (та же изоляция, что и во всей
// остальной цепочке — webhook.OperatorTokenAuthenticator/opconfig.Reader).
func TestDifferentOperatorsAreIndependentInCache(t *testing.T) {
	config := &fakeConfigReader{refs: map[string]string{
		"beeline_uz": "vault://operators/beeline_uz/webhook_bearer",
		"ucell_uz":   "vault://operators/ucell_uz/webhook_bearer",
	}}
	vaultReader := &fakeVaultReader{values: map[string]string{
		"operators/beeline_uz/webhook_bearer": "beeline-secret",
		"operators/ucell_uz/webhook_bearer":   "ucell-secret",
	}}
	lookup := New(config, vaultReader)

	beelineToken, _, _ := lookup.ExpectedToken(context.Background(), "beeline_uz")
	ucellToken, _, _ := lookup.ExpectedToken(context.Background(), "ucell_uz")

	if beelineToken != "beeline-secret" || ucellToken != "ucell-secret" {
		t.Fatalf("операторы не должны смешиваться: beeline=%q ucell=%q", beelineToken, ucellToken)
	}
}
