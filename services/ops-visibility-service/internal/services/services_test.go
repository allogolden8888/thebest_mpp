package services

import "testing"

func TestListHasNoDuplicates(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range List {
		if seen[s] {
			t.Errorf("дубликат в services.List: %q", s)
		}
		seen[s] = true
	}
}

func TestListHasNoEmptyEntries(t *testing.T) {
	for i, s := range List {
		if s == "" {
			t.Errorf("пустое имя сервиса на позиции %d", i)
		}
	}
}

// TestListMatchesKnownCountFromGenerateManifests — регрессия на дрейф:
// k8s/generate_manifests.py::SERVICES содержал 34 сервиса на момент
// написания этого списка (см. doc comment в services.go). Если это число
// меняется, тест явно падает и напоминает о ручной синхронизации, вместо
// того чтобы список тихо разъехался с generate_manifests.py.
func TestListMatchesKnownCountFromGenerateManifests(t *testing.T) {
	const wantCount = 34
	if len(List) != wantCount {
		t.Errorf("len(List) = %d, want %d — если generate_manifests.py::SERVICES реально изменился, обнови и это число, и сам список (см. doc comment в services.go)", len(List), wantCount)
	}
}

// TestListDoesNotContainSelfOrOutOfScopeServices — ops-visibility-service
// не опрашивает само себя, и явно вне скоупа incident-service (Ф7,
// параллельная работа) до тех пор, пока k8s/generate_manifests.py не
// заведёт их обоих официально (не эта фаза работы).
func TestListDoesNotContainSelfOrOutOfScopeServices(t *testing.T) {
	excluded := []string{"ops-visibility-service", "incident-service"}
	for _, s := range List {
		for _, ex := range excluded {
			if s == ex {
				t.Errorf("services.List не должен содержать %q на этом шаге", ex)
			}
		}
	}
}
