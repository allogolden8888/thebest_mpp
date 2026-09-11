package config

import "sync/atomic"

// Store — the live, hot-swappable partner snapshot. BACKOFFICE_ROADMAP.md
// P0 #4: kafkaio.HandleRecord reads through Store on every lifecycle/retry
// event, while internal/kafkaio.HandleConfigChangeRecord (driven by the
// config.changes consumer) publishes new Snapshot values as partners
// change — readers never block on writers and never observe a partially
// updated map, since Snapshot itself is immutable and Store only ever
// swaps the whole pointer (atomic.Pointer[Snapshot], same primitive family
// as the ArcSwap convention used in the Rust siblings for this exact
// class of problem).
type Store struct {
	ptr atomic.Pointer[Snapshot]
}

// NewStore — wraps an initial Snapshot (bootstrap load, either from
// PARTNER_CONFIG_PATH or from RedisSource.LoadAll) for atomic live
// updates.
func NewStore(initial Snapshot) *Store {
	s := &Store{}
	s.ptr.Store(&initial)
	return s
}

// Get — a consistent, immutable point-in-time view. Safe to call
// concurrently with Upsert/Remove from any number of goroutines.
func (s *Store) Get() Snapshot {
	return *s.ptr.Load()
}

// Application/FirstApplication — same signatures as Snapshot's, so Store
// is a drop-in replacement for kafkaio.Deps.Snapshot's previous
// config.Snapshot-by-value field.
func (s *Store) Application(partnerID, applicationID string) (Partner, Application, bool) {
	return s.Get().Application(partnerID, applicationID)
}

func (s *Store) FirstApplication(partnerID string) (Partner, Application, bool) {
	return s.Get().FirstApplication(partnerID)
}

// Upsert — atomically publishes partner p as the new live version (add or
// replace). Retries the compare-and-swap on concurrent writers (config.changes
// consumer is effectively single-threaded per partition in practice, but
// this makes Store correct regardless of caller concurrency).
func (s *Store) Upsert(p Partner) {
	for {
		old := s.ptr.Load()
		next := old.withPartner(p)
		if s.ptr.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Remove — atomically drops partner_id from the live snapshot. Used both
// for genuine removal (config:current missing in Configuration Redis) and
// for archival (Partner.IsArchived(), see partner.go) — either way the
// partner must stop resolving to a delivery channel without a restart.
func (s *Store) Remove(partnerID string) {
	for {
		old := s.ptr.Load()
		if _, ok := old.partners[partnerID]; !ok {
			return // already absent — nothing to publish
		}
		next := old.withoutPartner(partnerID)
		if s.ptr.CompareAndSwap(old, &next) {
			return
		}
	}
}
