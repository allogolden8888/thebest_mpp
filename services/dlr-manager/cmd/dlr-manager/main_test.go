package main

import (
	"os"
	"testing"
)

// TestBuildDatabaseURLComposesFromDiscreteSecretVars — регрессия на
// реальную находку: k8s инжектит POSTGRES_HOST/PORT/DB/USER/PASSWORD
// дискретно (envFrom secretRef), не единую DATABASE_URL — без этого теста
// сборка URL могла бы незаметно продолжить читать несуществующую
// переменную и подключаться к hardcoded дефолту без креденшлов.
func TestBuildDatabaseURLComposesFromDiscreteSecretVars(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("POSTGRES_HOST", "pg.example.internal")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "mppdb")
	t.Setenv("POSTGRES_USER", "svc_user")
	t.Setenv("POSTGRES_PASSWORD", "s3cr3t")

	got := buildDatabaseURL()
	// pool_max_conns — из fef9c34 ("bounded Postgres pool", находка 2.3
	// WEAK_HARDWARE_AUDIT.md): pgxpool без явного лимита берёт
	// max(4, runtime.NumCPU()), а NumCPU() читает affinity-маску хоста, не
	// cgroup-квоту. Правка была намеренной и раскатана на 14 сервисов;
	// ожидание в этом тесте тогда не обновили (тот коммит проверял только
	// go build/go vet), и он падал на main начиная с fef9c34. Сервис-сосед
	// dlr-correlation-writer свой такой же тест уже привёл в соответствие.
	want := "postgres://svc_user:s3cr3t@pg.example.internal:5433/mppdb?sslmode=disable&pool_max_conns=8"
	if got != want {
		t.Errorf("buildDatabaseURL() = %q, want %q", got, want)
	}
}

func TestBuildDatabaseURLWithoutPasswordOmitsColon(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("POSTGRES_HOST", "pg.example.internal")
	t.Setenv("POSTGRES_USER", "svc_user")

	got := buildDatabaseURL()
	want := "postgres://svc_user@pg.example.internal:5432/mpp?sslmode=disable&pool_max_conns=8"
	if got != want {
		t.Errorf("buildDatabaseURL() = %q, want %q", got, want)
	}
}

func TestBuildDatabaseURLExplicitOverrideTakesPriority(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DATABASE_URL", "postgres://explicit-override/db")
	t.Setenv("POSTGRES_HOST", "should-be-ignored")

	got := buildDatabaseURL()
	if got != "postgres://explicit-override/db" {
		t.Errorf("явный DATABASE_URL должен побеждать дискретные переменные, получили %q", got)
	}
}

// POSTGRES_POOL_MAX_CONNS — настраиваемый предел, 8 лишь дефолт (fef9c34):
// фиксируем это явно, чтобы правка выше не читалась как «в URL зашита
// восьмёрка».
func TestBuildDatabaseURLPoolSizeIsOverridable(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("POSTGRES_HOST", "pg.example.internal")
	t.Setenv("POSTGRES_USER", "svc_user")
	t.Setenv("POSTGRES_POOL_MAX_CONNS", "4")

	got := buildDatabaseURL()
	want := "postgres://svc_user@pg.example.internal:5432/mpp?sslmode=disable&pool_max_conns=4"
	if got != want {
		t.Errorf("buildDatabaseURL() = %q, want %q", got, want)
	}
}

func TestBuildRedisRuntimeURLComposesFromDiscreteSecretVars(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_RUNTIME_HOST", "redis.example.internal")
	t.Setenv("REDIS_RUNTIME_PORT", "6380")
	t.Setenv("REDIS_RUNTIME_PASSWORD", "r3d1s")

	got := buildRedisRuntimeURL()
	want := "redis://:r3d1s@redis.example.internal:6380/0"
	if got != want {
		t.Errorf("buildRedisRuntimeURL() = %q, want %q", got, want)
	}
}

func TestBuildRedisRuntimeURLWithoutPasswordOmitsAuth(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_RUNTIME_HOST", "redis.example.internal")

	got := buildRedisRuntimeURL()
	want := "redis://redis.example.internal:6379/0"
	if got != want {
		t.Errorf("buildRedisRuntimeURL() = %q, want %q", got, want)
	}
}

// t.Setenv already restores the previous value (or unset) automatically at
// test end (Go 1.17+) — these helpers just need to start from a known-clean
// slate before each test sets only the variables it cares about.
func clearDBEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"DATABASE_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_POOL_MAX_CONNS"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}

func clearRedisEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"REDIS_RUNTIME_URL", "REDIS_RUNTIME_HOST", "REDIS_RUNTIME_PORT", "REDIS_RUNTIME_PASSWORD"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}
