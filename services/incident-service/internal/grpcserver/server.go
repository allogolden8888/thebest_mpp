// Package grpcserver реализует mpp.grpc.v1.IncidentService
// (platform-contracts/grpc/incident.proto) — вызывается Backoffice API для
// раздела backoffice-ui "Incidents" (открыть/закрыть, связанные overrides,
// таймлайн, постмортем; право incident:manage, migrations/V025__iam.sql).
package grpcserver

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/incident-service/internal/store"
)

// allowedSeverities — CHECK-ограничение incident.incidents.severity
// (migrations/V028__incident.sql). Проверяется здесь тоже, чтобы вызывающий
// получил понятный InvalidArgument, а не сырую ошибку вставки в БД (тот же
// принцип, что execution-control-service validAdmissionRate).
var allowedSeverities = map[string]bool{"LOW": true, "MEDIUM": true, "HIGH": true, "CRITICAL": true}

// Store — минимальный интерфейс, который нужен серверу от store.Postgres
// (позволяет подменять в тестах фейком без реального Postgres).
type Store interface {
	OpenIncident(ctx context.Context, title, severity, openedBy string) (store.Incident, error)
	ListIncidents(ctx context.Context, status string) ([]store.Incident, error)
	GetIncident(ctx context.Context, incidentID int64) (store.Incident, error)
	TimelineForIncident(ctx context.Context, incidentID int64) ([]store.TimelineEntry, error)
	ListNotes(ctx context.Context, incidentID int64) ([]store.IncidentNote, error)
	AddNote(ctx context.Context, incidentID int64, author, note string) (store.IncidentNote, error)
	ResolveIncident(ctx context.Context, incidentID int64, resolvedBy, postmortemNotes string) (store.Incident, error)
}

type Server struct {
	grpcv1.UnimplementedIncidentServiceServer

	store Store
}

func New(s Store) *Server {
	return &Server{store: s}
}

func (s *Server) OpenIncident(ctx context.Context, req *grpcv1.OpenIncidentRequest) (*grpcv1.Incident, error) {
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "title обязателен")
	}
	if !allowedSeverities[req.GetSeverity()] {
		return nil, status.Errorf(codes.InvalidArgument, "severity должен быть одним из LOW/MEDIUM/HIGH/CRITICAL, получили %q", req.GetSeverity())
	}
	if req.GetOpenedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "opened_by обязателен для аудита")
	}

	inc, err := s.store.OpenIncident(ctx, req.GetTitle(), req.GetSeverity(), req.GetOpenedBy())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoIncident(inc), nil
}

func (s *Server) ListIncidents(ctx context.Context, req *grpcv1.ListIncidentsRequest) (*grpcv1.ListIncidentsResponse, error) {
	if st := req.GetStatus(); st != "" && st != "OPEN" && st != "RESOLVED" {
		return nil, status.Errorf(codes.InvalidArgument, "status должен быть пустым, OPEN или RESOLVED, получили %q", st)
	}

	incidents, err := s.store.ListIncidents(ctx, req.GetStatus())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListIncidentsResponse{Incidents: make([]*grpcv1.Incident, 0, len(incidents))}
	for _, inc := range incidents {
		resp.Incidents = append(resp.Incidents, toProtoIncident(inc))
	}
	return resp, nil
}

// GetIncident — карточка инцидента ВКЛЮЧАЯ таймлайн (cross-schema чтение
// control.execution_control_audit, store.TimelineForIncident) и заметки.
// Три независимых запроса собираются в один ответ здесь, не в store —
// store остаётся набором узких методов, композиция — обязанность
// gRPC-слоя (тот же принцип разделения, что iam-service.ListRoles vs.
// его SQL-запрос с JOIN, только наоборот: там JOIN в SQL, здесь — в Go,
// потому что источники в разных схемах/таблицах без общего ключа для JOIN
// в одном запросе, который было бы разумно писать руками).
func (s *Server) GetIncident(ctx context.Context, req *grpcv1.GetIncidentRequest) (*grpcv1.IncidentDetail, error) {
	if req.GetIncidentId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "incident_id обязателен")
	}

	inc, err := s.store.GetIncident(ctx, req.GetIncidentId())
	if err != nil {
		if errors.Is(err, store.ErrIncidentNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	timeline, err := s.store.TimelineForIncident(ctx, req.GetIncidentId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	notes, err := s.store.ListNotes(ctx, req.GetIncidentId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	detail := &grpcv1.IncidentDetail{
		Incident: toProtoIncident(inc),
		Timeline: make([]*grpcv1.TimelineEntry, 0, len(timeline)),
		Notes:    make([]*grpcv1.IncidentNote, 0, len(notes)),
	}
	for _, e := range timeline {
		detail.Timeline = append(detail.Timeline, toProtoTimelineEntry(e))
	}
	for _, n := range notes {
		detail.Notes = append(detail.Notes, toProtoNote(n))
	}
	return detail, nil
}

func (s *Server) AddNote(ctx context.Context, req *grpcv1.AddNoteRequest) (*grpcv1.IncidentNote, error) {
	if req.GetIncidentId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "incident_id обязателен")
	}
	if strings.TrimSpace(req.GetAuthor()) == "" {
		return nil, status.Error(codes.InvalidArgument, "author обязателен")
	}
	if strings.TrimSpace(req.GetNote()) == "" {
		return nil, status.Error(codes.InvalidArgument, "note обязателен")
	}

	n, err := s.store.AddNote(ctx, req.GetIncidentId(), req.GetAuthor(), req.GetNote())
	if err != nil {
		if errors.Is(err, store.ErrIncidentNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoNote(n), nil
}

// ResolveIncident — постмортем ОБЯЗАТЕЛЕН (непустой, не только пробелы) для
// перехода в RESOLVED: закрытый без разбора причин инцидент обесценивает
// саму идею трекинга — настоящая валидация, не подсказка (см.
// incident.proto IncidentService.ResolveIncident docstring).
func (s *Server) ResolveIncident(ctx context.Context, req *grpcv1.ResolveIncidentRequest) (*grpcv1.Incident, error) {
	if req.GetIncidentId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "incident_id обязателен")
	}
	if req.GetResolvedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "resolved_by обязателен для аудита")
	}
	if strings.TrimSpace(req.GetPostmortemNotes()) == "" {
		return nil, status.Error(codes.InvalidArgument, "postmortem_notes обязателен для закрытия инцидента — закрытие без разбора причин не поддерживается")
	}

	inc, err := s.store.ResolveIncident(ctx, req.GetIncidentId(), req.GetResolvedBy(), req.GetPostmortemNotes())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrIncidentNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		case errors.Is(err, store.ErrAlreadyResolved):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return toProtoIncident(inc), nil
}

func toProtoIncident(inc store.Incident) *grpcv1.Incident {
	p := &grpcv1.Incident{
		Id:              inc.ID,
		Title:           inc.Title,
		Severity:        inc.Severity,
		Status:          inc.Status,
		OpenedBy:        inc.OpenedBy,
		OpenedAt:        timestamppb.New(inc.OpenedAt),
		ResolvedBy:      inc.ResolvedBy,
		PostmortemNotes: inc.PostmortemNotes,
	}
	if inc.ResolvedAt != nil {
		p.ResolvedAt = timestamppb.New(*inc.ResolvedAt)
	}
	return p
}

func toProtoTimelineEntry(e store.TimelineEntry) *grpcv1.TimelineEntry {
	p := &grpcv1.TimelineEntry{
		Id:            e.ID,
		Scope:         e.Scope,
		ScopeId:       e.ScopeID,
		State:         e.State,
		AdmissionRate: e.AdmissionRate,
		Reason:        e.Reason,
		RequestedBy:   e.RequestedBy,
		CreatedAt:     timestamppb.New(e.CreatedAt),
	}
	if e.ExpiresAt != nil {
		p.ExpiresAt = timestamppb.New(*e.ExpiresAt)
	}
	return p
}

func toProtoNote(n store.IncidentNote) *grpcv1.IncidentNote {
	return &grpcv1.IncidentNote{
		Id:         n.ID,
		IncidentId: n.IncidentID,
		Author:     n.Author,
		Note:       n.Note,
		CreatedAt:  timestamppb.New(n.CreatedAt),
	}
}
