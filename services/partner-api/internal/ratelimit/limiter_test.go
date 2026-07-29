package ratelimit

import "testing"

func TestAllowsUpToBurstThenBlocks(t *testing.T) {
	l := New(1, 3)
	for i := 0; i < 3; i++ {
		if !l.Allow("acme") {
			t.Fatalf("запрос %d должен был пройти в пределах burst", i)
		}
	}
	if l.Allow("acme") {
		t.Fatalf("4-й запрос должен был быть отклонён — burst исчерпан")
	}
}

func TestPartnersAreIndependent(t *testing.T) {
	l := New(1, 1)
	if !l.Allow("acme") {
		t.Fatalf("первый запрос acme должен пройти")
	}
	if !l.Allow("globex") {
		t.Fatalf("другой партнёр не должен зависеть от лимита acme")
	}
	if l.Allow("acme") {
		t.Fatalf("второй запрос acme подряд должен быть отклонён (burst=1)")
	}
}
