package grpcserver

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/incident-service/internal/store"
)

// fakeStore — реализует Store без реального Postgres, чтобы проверить
// маппинг ошибок/валидацию на уровне gRPC-сервера отдельно от store_test.go
// (тот же реальный round-trip против Postgres) — тот же паттерн, что
// iam-service/internal/grpcserver/server_test.go.
type fakeStore struct {
	incidents []store.Incident
	timeline  []store.TimelineEntry
	notes     []store.IncidentNote

	openErr    error
	getErr     error
	addNoteErr error
	resolveErr error

	lastOpenedTitle    string
	lastOpenedSeverity string
	lastOpenedBy       string
	lastAddNoteID      int64
	lastResolvedID     int64
	lastPostmortem     string
}

func (f *fakeStore) OpenIncident(_ context.Context, title, severity, openedBy string) (store.Incident, error) {
	f.lastOpenedTitle, f.lastOpenedSeverity, f.lastOpenedBy = title, severity, openedBy
	if f.openErr != nil {
		return store.Incident{}, f.openErr
	}
	return store.Incident{ID: 1, Title: title, Severity: severity, Status: "OPEN", OpenedBy: openedBy, OpenedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) ListIncidents(_ context.Context, _ string) ([]store.Incident, error) {
	return f.incidents, nil
}

func (f *fakeStore) GetIncident(_ context.Context, incidentID int64) (store.Incident, error) {
	if f.getErr != nil {
		return store.Incident{}, f.getErr
	}
	return store.Incident{ID: incidentID, Title: "t", Severity: "HIGH", Status: "OPEN", OpenedBy: "opener", OpenedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) TimelineForIncident(_ context.Context, _ int64) ([]store.TimelineEntry, error) {
	return f.timeline, nil
}

func (f *fakeStore) ListNotes(_ context.Context, _ int64) ([]store.IncidentNote, error) {
	return f.notes, nil
}

func (f *fakeStore) AddNote(_ context.Context, incidentID int64, author, note string) (store.IncidentNote, error) {
	f.lastAddNoteID = incidentID
	if f.addNoteErr != nil {
		return store.IncidentNote{}, f.addNoteErr
	}
	return store.IncidentNote{ID: 1, IncidentID: incidentID, Author: author, Note: note, CreatedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) ResolveIncident(_ context.Context, incidentID int64, resolvedBy, postmortemNotes string) (store.Incident, error) {
	f.lastResolvedID = incidentID
	f.lastPostmortem = postmortemNotes
	if f.resolveErr != nil {
		return store.Incident{}, f.resolveErr
	}
	return store.Incident{ID: incidentID, Title: "t", Severity: "HIGH", Status: "RESOLVED", OpenedBy: "opener", OpenedAt: time.Unix(0, 0), ResolvedBy: resolvedBy, PostmortemNotes: postmortemNotes}, nil
}

func grpcCode(t *testing.T, err error) codes.Code {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("ожидали gRPC status error, получили %v", err)
	}
	return st.Code()
}

func TestOpenIncidentValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.OpenIncidentRequest{
		{Severity: "HIGH", OpenedBy: "u1"},              // пустой title
		{Title: "t", OpenedBy: "u1"},                    // пустой severity
		{Title: "t", Severity: "BOGUS", OpenedBy: "u1"}, // недопустимый severity
		{Title: "t", Severity: "HIGH"},                  // пустой opened_by
	}
	for _, req := range cases {
		if _, err := s.OpenIncident(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestOpenIncidentAcceptsAllAllowedSeverities(t *testing.T) {
	for _, sev := range []string{"LOW", "MEDIUM", "HIGH", "CRITICAL"} {
		fs := &fakeStore{}
		s := New(fs)
		resp, err := s.OpenIncident(context.Background(), &grpcv1.OpenIncidentRequest{Title: "t", Severity: sev, OpenedBy: "u1"})
		if err != nil {
			t.Fatalf("severity=%q: OpenIncident: %v", sev, err)
		}
		if resp.GetSeverity() != sev {
			t.Errorf("severity=%q: неожиданный ответ %+v", sev, resp)
		}
	}
}

func TestOpenIncidentPassesThroughToStore(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.OpenIncident(context.Background(), &grpcv1.OpenIncidentRequest{Title: "db down", Severity: "CRITICAL", OpenedBy: "oncall-1"})
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	if fs.lastOpenedTitle != "db down" || fs.lastOpenedSeverity != "CRITICAL" || fs.lastOpenedBy != "oncall-1" {
		t.Errorf("store не получил ожидаемые аргументы: title=%q severity=%q openedBy=%q", fs.lastOpenedTitle, fs.lastOpenedSeverity, fs.lastOpenedBy)
	}
	if resp.GetId() != 1 || resp.GetStatus() != "OPEN" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestListIncidentsRejectsInvalidStatus(t *testing.T) {
	s := New(&fakeStore{})

	if _, err := s.ListIncidents(context.Background(), &grpcv1.ListIncidentsRequest{Status: "BOGUS"}); grpcCode(t, err) != codes.InvalidArgument {
		t.Errorf("ожидали InvalidArgument для недопустимого status")
	}
	if _, err := s.ListIncidents(context.Background(), &grpcv1.ListIncidentsRequest{Status: ""}); err != nil {
		t.Errorf("пустой status должен быть валиден (все инциденты), получили %v", err)
	}
	if _, err := s.ListIncidents(context.Background(), &grpcv1.ListIncidentsRequest{Status: "OPEN"}); err != nil {
		t.Errorf("status=OPEN должен быть валиден, получили %v", err)
	}
}

func TestListIncidentsTranslatesStoreIncidentsToProto(t *testing.T) {
	s := New(&fakeStore{incidents: []store.Incident{
		{ID: 1, Title: "t1", Severity: "LOW", Status: "OPEN", OpenedBy: "u1", OpenedAt: time.Unix(0, 0)},
	}})

	resp, err := s.ListIncidents(context.Background(), &grpcv1.ListIncidentsRequest{})
	if err != nil {
		t.Fatalf("ListIncidents: %v", err)
	}
	if len(resp.GetIncidents()) != 1 || resp.GetIncidents()[0].GetTitle() != "t1" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestGetIncidentRequiresIncidentId(t *testing.T) {
	s := New(&fakeStore{})
	if _, err := s.GetIncident(context.Background(), &grpcv1.GetIncidentRequest{}); grpcCode(t, err) != codes.InvalidArgument {
		t.Errorf("ожидали InvalidArgument при отсутствующем incident_id")
	}
}

func TestGetIncidentMapsNotFound(t *testing.T) {
	s := New(&fakeStore{getErr: store.ErrIncidentNotFound})
	_, err := s.GetIncident(context.Background(), &grpcv1.GetIncidentRequest{IncidentId: 999})
	if grpcCode(t, err) != codes.NotFound {
		t.Errorf("ожидали NotFound, получили %v", err)
	}
}

func TestGetIncidentIncludesTimelineAndNotes(t *testing.T) {
	fs := &fakeStore{
		timeline: []store.TimelineEntry{
			{ID: 1, Scope: "PARTNER_STAGE", ScopeID: "acme:billing", State: "PAUSED", AdmissionRate: 0, Reason: "billing_freeze", RequestedBy: "billing-reconciliation", CreatedAt: time.Unix(0, 0)},
		},
		notes: []store.IncidentNote{
			{ID: 1, IncidentID: 7, Author: "responder-1", Note: "investigating", CreatedAt: time.Unix(0, 0)},
		},
	}
	s := New(fs)

	resp, err := s.GetIncident(context.Background(), &grpcv1.GetIncidentRequest{IncidentId: 7})
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if resp.GetIncident().GetId() != 7 {
		t.Errorf("неожиданный incident: %+v", resp.GetIncident())
	}
	if len(resp.GetTimeline()) != 1 || resp.GetTimeline()[0].GetReason() != "billing_freeze" {
		t.Errorf("неожиданный timeline: %+v", resp.GetTimeline())
	}
	if len(resp.GetNotes()) != 1 || resp.GetNotes()[0].GetNote() != "investigating" {
		t.Errorf("неожиданные notes: %+v", resp.GetNotes())
	}
}

func TestAddNoteValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.AddNoteRequest{
		{Author: "a", Note: "n"},                  // пустой incident_id
		{IncidentId: 1, Note: "n"},                // пустой author
		{IncidentId: 1, Author: "a"},              // пустой note
		{IncidentId: 1, Author: "  ", Note: "n"},  // author из пробелов
		{IncidentId: 1, Author: "a", Note: "   "}, // note из пробелов
	}
	for _, req := range cases {
		if _, err := s.AddNote(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestAddNoteMapsNotFound(t *testing.T) {
	s := New(&fakeStore{addNoteErr: store.ErrIncidentNotFound})
	_, err := s.AddNote(context.Background(), &grpcv1.AddNoteRequest{IncidentId: 999, Author: "a", Note: "n"})
	if grpcCode(t, err) != codes.NotFound {
		t.Errorf("ожидали NotFound, получили %v", err)
	}
}

func TestAddNotePassesThroughToStore(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.AddNote(context.Background(), &grpcv1.AddNoteRequest{IncidentId: 42, Author: "responder-1", Note: "investigating"})
	if err != nil {
		t.Fatalf("AddNote: %v", err)
	}
	if fs.lastAddNoteID != 42 {
		t.Errorf("store не получил ожидаемый incident_id: %d", fs.lastAddNoteID)
	}
	if resp.GetIncidentId() != 42 || resp.GetAuthor() != "responder-1" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestResolveIncidentRequiresPostmortemNotes(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.ResolveIncidentRequest{
		{ResolvedBy: "r1"},                // пустой incident_id
		{IncidentId: 1},                   // пустой resolved_by
		{IncidentId: 1, ResolvedBy: "r1"}, // пустой postmortem_notes
		{IncidentId: 1, ResolvedBy: "r1", PostmortemNotes: "   "}, // только пробелы
	}
	for _, req := range cases {
		if _, err := s.ResolveIncident(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument (постмортем обязателен), получили %v", req, err)
		}
	}
}

func TestResolveIncidentSucceedsWithNonEmptyPostmortem(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.ResolveIncident(context.Background(), &grpcv1.ResolveIncidentRequest{
		IncidentId: 5, ResolvedBy: "resolver-1", PostmortemNotes: "root cause: bad config, rolled back",
	})
	if err != nil {
		t.Fatalf("ResolveIncident: %v", err)
	}
	if fs.lastResolvedID != 5 || fs.lastPostmortem != "root cause: bad config, rolled back" {
		t.Errorf("store не получил ожидаемые аргументы: id=%d postmortem=%q", fs.lastResolvedID, fs.lastPostmortem)
	}
	if resp.GetStatus() != "RESOLVED" || resp.GetPostmortemNotes() == "" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestResolveIncidentMapsStoreErrorsToGrpcCodes(t *testing.T) {
	cases := []struct {
		name     string
		storeErr error
		want     codes.Code
	}{
		{"unknown incident", store.ErrIncidentNotFound, codes.NotFound},
		{"already resolved", store.ErrAlreadyResolved, codes.FailedPrecondition},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(&fakeStore{resolveErr: tc.storeErr})
			_, err := s.ResolveIncident(context.Background(), &grpcv1.ResolveIncidentRequest{
				IncidentId: 1, ResolvedBy: "r1", PostmortemNotes: "notes",
			})
			if grpcCode(t, err) != tc.want {
				t.Errorf("ожидали код %v, получили %v", tc.want, err)
			}
		})
	}
}
