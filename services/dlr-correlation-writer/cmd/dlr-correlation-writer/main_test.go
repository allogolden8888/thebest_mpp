package main

import (
	"os"
	"testing"
)

// TestBuildDatabaseURLComposesFromDiscreteSecretVars — регрессия на находку
// задокументированную в services/dlr-manager/README.md: k8s инжектит
// POSTGRES_HOST/PORT/DB/USER/PASSWORD дискретно, не единую DATABASE_URL.
func TestBuildDatabaseURLComposesFromDiscreteSecretVars(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("POSTGRES_HOST", "pg.example.internal")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "mppdb")
	t.Setenv("POSTGRES_USER", "svc_user")
	t.Setenv("POSTGRES_PASSWORD", "s3cr3t")

	got := buildDatabaseURL()
	want := "postgres://svc_user:s3cr3t@pg.example.internal:5433/mppdb?sslmode=disable"
	if got != want {
		t.Errorf("buildDatabaseURL() = %q, want %q", got, want)
	}
}

func TestBuildDatabaseURLWithoutPasswordOmitsColon(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("POSTGRES_HOST", "pg.example.internal")
	t.Setenv("POSTGRES_USER", "svc_user")

	got := buildDatabaseURL()
	want := "postgres://svc_user@pg.example.internal:5432/mpp?sslmode=disable"
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

func clearDBEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"DATABASE_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}
