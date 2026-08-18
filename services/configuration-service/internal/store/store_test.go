package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/configuration-service/internal/validate"
)

// jsonEqual — payload column is JSONB: Postgres re-serializes on storage
// (e.g. `{"n":1}` comes back as `{"n": 1}`, a space after the colon) —
// compare structurally, not by exact byte string.
func jsonEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("got is not valid JSON: %v (%s)", err, got)
	}
	if err := json.Unmarshal([]byte(want), &wantVal); err != nil {
		t.Fatalf("want is not valid JSON: %v", err)
	}
	gotNorm, _ := json.Marshal(gotVal)
	wantNorm, _ := json.Marshal(wantVal)
	if string(gotNorm) != string(wantNorm) {
		t.Fatalf("payload mismatch: got %s, want %s", got, want)
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CONFIGURATION_SERVICE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	return pool
}

// uniqueEntityID — изолирует тесты друг от друга и от предыдущих прогонов
// в общей таблице config.config_versions.
func uniqueEntityID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestCreateImmutableVersionAndOutboxFirstVersion(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	entityID := uniqueEntityID("acme")
	v, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{"partner_id":"acme"}`), "tester")
	if err != nil {
		t.Fatalf("CreateImmutableVersionAndOutbox failed: %v", err)
	}
	if v.Version != 1 {
		t.Fatalf("ожидали первую версию = 1, получили %d", v.Version)
	}
	if v.Status != "active" {
		t.Fatalf("ожидали status=active, получили %s", v.Status)
	}

	var outboxCount int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM config.config_outbox WHERE entity_id = $1`, entityID).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("readback outbox failed: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("ожидали ровно 1 outbox-запись, получили %d", outboxCount)
	}
}

func TestCreateImmutableVersionAndOutboxIncrementsVersion(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("beeline_uz")

	v1, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityOperator, entityID, []byte(`{}`), "tester")
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	v2, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityOperator, entityID, []byte(`{}`), "tester")
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if v1.Version != 1 || v2.Version != 2 {
		t.Fatalf("ожидали версии 1 и 2, получили %d и %d", v1.Version, v2.Version)
	}
}

// TestConcurrentCreateOnBrandNewEntitySerializesVersions — CODE_REVIEW.md
// finding: "SELECT ... FOR UPDATE только блокирует уже существующие строки;
// для совершенно новой сущности (0 строк) ничего не блокируется, и два
// конкурентных CreateVersion могут оба вычислить nextVersion=1". Проверяет,
// что pg_advisory_xact_lock(hashtext(entity_type:entity_id)) реально
// сериализует такие вызовы — N конкурентных Create для одной НОВОЙ сущности
// должны получить N разных последовательных версий, без коллизии.
func TestConcurrentCreateOnBrandNewEntitySerializesVersions(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("race")

	const n = 10
	versions := make(chan int32, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			v, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{}`), "tester")
			if err != nil {
				errs <- err
				return
			}
			versions <- v.Version
		}()
	}

	seen := map[int32]bool{}
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent CreateImmutableVersionAndOutbox failed: %v", err)
		case v := <-versions:
			if seen[v] {
				t.Fatalf("версия %d выдана более одного раза — гонка не устранена", v)
			}
			seen[v] = true
		}
	}
	for v := int32(1); v <= n; v++ {
		if !seen[v] {
			t.Fatalf("версия %d никогда не была выдана: %v", v, seen)
		}
	}
}

func TestPolicyTemplateSkipsConfigVersionsTable(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("tmpl")

	_, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPolicyTemplate, entityID, []byte(`{}`), "tester")
	if err != nil {
		t.Fatalf("CreateImmutableVersionAndOutbox failed: %v", err)
	}

	var configVersionsCount int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM config.config_versions WHERE entity_id = $1`, entityID).Scan(&configVersionsCount)
	if err != nil {
		t.Fatalf("readback config_versions failed: %v", err)
	}
	if configVersionsCount != 0 {
		t.Fatalf("policy_template не должен писать строку в config_versions, нашли %d", configVersionsCount)
	}

	var outboxCount int
	var configVersionIDIsNull bool
	err = pool.QueryRow(ctx, `SELECT count(*), bool_and(config_version_id IS NULL) FROM config.config_outbox WHERE entity_id = $1`, entityID).
		Scan(&outboxCount, &configVersionIDIsNull)
	if err != nil {
		t.Fatalf("readback outbox failed: %v", err)
	}
	if outboxCount != 1 || !configVersionIDIsNull {
		t.Fatalf("ожидали 1 outbox-запись с config_version_id=NULL, получили count=%d null=%v", outboxCount, configVersionIDIsNull)
	}
}

func TestArchiveVersionSetsStatusArchived(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("acme")

	created, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{}`), "tester")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	archived, err := s.ArchiveVersion(ctx, validate.EntityPartner, entityID, created.Version)
	if err != nil {
		t.Fatalf("ArchiveVersion failed: %v", err)
	}
	if archived.Status != "archived" {
		t.Fatalf("ожидали status=archived, получили %s", archived.Status)
	}
}

func TestListVersionsReturnsAllCreatedVersions(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("acme")

	for i := 0; i < 3; i++ {
		if _, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{}`), "tester"); err != nil {
			t.Fatalf("create #%d failed: %v", i, err)
		}
	}

	versions, _, err := s.ListVersions(ctx, validate.EntityPartner, entityID, 50, "")
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("ожидали 3 версии, получили %d", len(versions))
	}
}
// TestGetVersionByNumberReturnsExactVersionPayload — luminous-hugging-charm.md
// Ф10 (DiffVersions). Two versions of one entity, real Postgres —
// confirms GetVersionByNumber fetches the SPECIFIC version asked for, not
// just the currently active one (GetActiveVersion would only ever see the
// second write here).
func TestGetVersionByNumberReturnsExactVersionPayload(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("acme")

	if _, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{"n":1}`), "tester"); err != nil {
		t.Fatalf("create v1 failed: %v", err)
	}
	if _, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{"n":2}`), "tester"); err != nil {
		t.Fatalf("create v2 failed: %v", err)
	}

	v1, err := s.GetVersionByNumber(ctx, validate.EntityPartner, entityID, 1)
	if err != nil {
		t.Fatalf("GetVersionByNumber(1) failed: %v", err)
	}
	jsonEqual(t, v1, `{"n":1}`)

	v2, err := s.GetVersionByNumber(ctx, validate.EntityPartner, entityID, 2)
	if err != nil {
		t.Fatalf("GetVersionByNumber(2) failed: %v", err)
	}
	jsonEqual(t, v2, `{"n":2}`)
}

func TestGetVersionByNumberUnknownVersionReturnsError(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("acme")

	if _, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, []byte(`{}`), "tester"); err != nil {
		t.Fatalf("create v1 failed: %v", err)
	}

	if _, err := s.GetVersionByNumber(ctx, validate.EntityPartner, entityID, 99); err == nil {
		t.Fatalf("ожидали ошибку для несуществующей версии 99")
	}
}

// TestGetActiveVersionReturnsPayloadFromRealRow — найдено при реализации
// partner-self-service-api (Фаза 3 плана): до добавления payload в SELECT
// GetActiveVersion читал только метаданные строки config.config_versions,
// само содержимое документа было недостижимо ни для одного клиента
// ConfigService. Проверяем реальный round-trip через Postgres, не через
// fakeStore (тот у grpcserver-пакета).
func TestGetActiveVersionReturnsPayloadFromRealRow(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	entityID := uniqueEntityID("acme")

	wantPayload := []byte(`{"partner_id":"acme","applications":[]}`)
	if _, err := s.CreateImmutableVersionAndOutbox(ctx, validate.EntityPartner, entityID, wantPayload, "tester"); err != nil {
		t.Fatalf("CreateImmutableVersionAndOutbox failed: %v", err)
	}

	v, err := s.GetActiveVersion(ctx, validate.EntityPartner, entityID)
	if err != nil {
		t.Fatalf("GetActiveVersion failed: %v", err)
	}

	// JSONB нормализует пробелы при чтении обратно — сравниваем
	// распарсенное содержимое, не байты.
	var got, want map[string]any
	if err := json.Unmarshal(v.Payload, &got); err != nil {
		t.Fatalf("payload из БД не распарсился как JSON: %v (%q)", err, v.Payload)
	}
	if err := json.Unmarshal(wantPayload, &want); err != nil {
		t.Fatalf("тестовый payload не распарсился как JSON: %v", err)
	}
	if got["partner_id"] != want["partner_id"] {
		t.Fatalf("payload не пробросился из реальной строки: получили %v, ожидали %v", got, want)
	}
}
