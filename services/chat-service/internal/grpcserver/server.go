// Package grpcserver реализует mpp.grpc.v1.ChatService
// (platform-contracts/grpc/chat.proto) — вызывается ОБОИМИ backoffice-api
// (все партнёры, backoffice-ui "Chat", право chat:write) и
// partner-self-service-api (только собственный тред партнёра, partner_id
// всегда из JWT claim на стороне вызывающего, никогда из этого сервиса —
// chat-service сам не знает, кто вызывает, только то, что ему передали в
// запросе; изоляция "партнёр не видит чужой тред" обеспечивается
// ИСКЛЮЧИТЕЛЬНО на уровне partner-self-service-api, тем же принципом, что
// applications.go/webhook.go уже используют для конфигурации партнёра).
package grpcserver

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/chat-service/internal/store"
)

// allowedSenderTypes — CHECK-ограничение support.chat_messages.sender_type
// (migrations/V034__chat_messages.sql). Проверяется здесь тоже для понятного
// InvalidArgument вместо сырой ошибки вставки — тот же принцип, что
// incident-service's allowedSeverities.
var allowedSenderTypes = map[string]bool{"partner": true, "admin": true}

// Store — минимальный интерфейс, который нужен серверу от store.Postgres
// (позволяет подменять в тестах фейком без реального Postgres).
type Store interface {
	SendMessage(ctx context.Context, partnerID, senderType, senderID, body string) (store.ChatMessage, error)
	ListMessages(ctx context.Context, partnerID string, since *time.Time, viewerType string) ([]store.ChatMessage, error)
	ListThreads(ctx context.Context) ([]store.ChatThread, error)
}

type Server struct {
	grpcv1.UnimplementedChatServiceServer

	store Store
}

func New(s Store) *Server {
	return &Server{store: s}
}

func (s *Server) SendMessage(ctx context.Context, req *grpcv1.SendMessageRequest) (*grpcv1.ChatMessage, error) {
	if strings.TrimSpace(req.GetPartnerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "partner_id обязателен")
	}
	if !allowedSenderTypes[req.GetSenderType()] {
		return nil, status.Errorf(codes.InvalidArgument, "sender_type должен быть partner или admin, получили %q", req.GetSenderType())
	}
	if strings.TrimSpace(req.GetSenderId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sender_id обязателен для аудита")
	}
	if strings.TrimSpace(req.GetBody()) == "" {
		return nil, status.Error(codes.InvalidArgument, "body не может быть пустым")
	}

	m, err := s.store.SendMessage(ctx, req.GetPartnerId(), req.GetSenderType(), req.GetSenderId(), req.GetBody())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoMessage(m), nil
}

func (s *Server) ListMessages(ctx context.Context, req *grpcv1.ListMessagesRequest) (*grpcv1.ListMessagesResponse, error) {
	if strings.TrimSpace(req.GetPartnerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "partner_id обязателен")
	}
	if !allowedSenderTypes[req.GetViewerType()] {
		return nil, status.Errorf(codes.InvalidArgument, "viewer_type должен быть partner или admin, получили %q", req.GetViewerType())
	}

	// req.GetSince() == nil означает "поле since не установлено вызывающим"
	// (proto3 message-типы — единственный способ различить "0" и
	// "не задано" без отдельного bool-флага) -> вернуть тред с начала.
	var since *time.Time
	if req.GetSince() != nil {
		t := req.GetSince().AsTime()
		since = &t
	}

	messages, err := s.store.ListMessages(ctx, req.GetPartnerId(), since, req.GetViewerType())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &grpcv1.ListMessagesResponse{Messages: make([]*grpcv1.ChatMessage, 0, len(messages))}
	for _, m := range messages {
		resp.Messages = append(resp.Messages, toProtoMessage(m))
	}
	return resp, nil
}

func (s *Server) ListThreads(ctx context.Context, _ *grpcv1.ListThreadsRequest) (*grpcv1.ListThreadsResponse, error) {
	threads, err := s.store.ListThreads(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &grpcv1.ListThreadsResponse{Threads: make([]*grpcv1.ChatThread, 0, len(threads))}
	for _, t := range threads {
		resp.Threads = append(resp.Threads, &grpcv1.ChatThread{
			PartnerId:       t.PartnerID,
			LastMessageBody: t.LastBody,
			LastMessageAt:   timestamppb.New(t.LastMessageAt),
			UnreadCount:     t.UnreadCount,
		})
	}
	return resp, nil
}

func toProtoMessage(m store.ChatMessage) *grpcv1.ChatMessage {
	p := &grpcv1.ChatMessage{
		Id:         m.ID,
		PartnerId:  m.PartnerID,
		SenderType: m.SenderType,
		SenderId:   m.SenderID,
		Body:       m.Body,
		CreatedAt:  timestamppb.New(m.CreatedAt),
	}
	if m.ReadAt != nil {
		p.ReadAt = timestamppb.New(*m.ReadAt)
	}
	return p
}
