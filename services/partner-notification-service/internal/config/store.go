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
// concurrently with ApplyPartner/ApplyArchive from any number of goroutines.
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

// ApplyPartner atomically publishes a newer active/suspended partner version.
// Returns false for a stale/duplicate replay.
func (s *Store) ApplyPartner(version int64, p Partner) bool {
	for {
		old := s.ptr.Load()
		if version <= old.versions[p.PartnerID] {
			return false
		}
		next := old.withPartner(version, p)
		if s.ptr.CompareAndSwap(old, &next) {
			return true
		}
	}
}

// ApplyArchive drops the partner but retains its version as a tombstone.
func (s *Store) ApplyArchive(version int64, partnerID string) bool {
	for {
		old := s.ptr.Load()
		installedVersion := old.versions[partnerID]
		if version < installedVersion || (version == installedVersion && old.archived[partnerID]) {
			return false
		}
		next := old.withoutPartner(version, partnerID)
		if s.ptr.CompareAndSwap(old, &next) {
			return true
		}
	}
}
