// handleChat* — /v1/chat/* (право chat:write, BACKOFFICE_DESIGN_SPEC.md
// Экран 27 "Chat"). Тонкий gRPC-прокси поверх ChatService
// (platform-contracts/grpc/chat.proto) для экрана backoffice-ui "Chat".
//
// sender_id ВСЕГДА берётся из claims.Subject, никогда из тела запроса —
// тот же принцип, что opened_by/author/resolved_by в incidents.go:
// вызывающий не может подделать, кто отправил сообщение от имени
// платформы. sender_type фиксирован как "admin" здесь (в отличие от
// partner-self-service-api's chat.go, где он фиксирован как "partner") —
// backoffice-api это единственная сторона, с которой сообщения физически
// могут прийти как admin.
//
// Отдельный сервис (chat-service), не прямой доступ к
// support.chat_messages — см. platform-contracts/grpc/chat.proto package
// doc и services/chat-service/README.md за полным разбором решения.
package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

type chatMessageResponse struct {
	ID         int64  `json:"id"`
	PartnerID  string `json:"partner_id"`
	SenderType string `json:"sender_type"`
	SenderID   string `json:"sender_id"`
	Body       string `json:"body"`
	CreatedAt  string `json:"created_at,omitempty"`
	ReadAt     string `json:"read_at,omitempty"`
}

func toChatMessageResponse(m *grpcv1.ChatMessage) chatMessageResponse {
	return chatMessageResponse{
		ID:         m.GetId(),
		PartnerID:  m.GetPartnerId(),
		SenderType: m.GetSenderType(),
		SenderID:   m.GetSenderId(),
		Body:       m.GetBody(),
		CreatedAt:  formatTimestamp(m.GetCreatedAt()),
		ReadAt:     formatTimestamp(m.GetReadAt()),
	}
}

type chatThreadResponse struct {
	PartnerID       string `json:"partner_id"`
	LastMessageBody string `json:"last_message_body"`
	LastMessageAt   string `json:"last_message_at,omitempty"`
	UnreadCount     int64  `json:"unread_count"`
}

// handleListChatThreads — GET /v1/chat/threads: сайдбар "какие партнёры
// написали" (design-референс MPP Backoffice.dc.html, экран 27).
func handleListChatThreads(client grpcv1.ChatServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListThreads(r.Context(), &grpcv1.ListThreadsRequest{})
		if err != nil {
			writeChatGRPCError(w, "chat_threads_list", err)
			return
		}
		threads := make([]chatThreadResponse, 0, len(resp.GetThreads()))
		for _, t := range resp.GetThreads() {
			threads = append(threads, chatThreadResponse{
				PartnerID:       t.GetPartnerId(),
				LastMessageBody: t.GetLastMessageBody(),
				LastMessageAt:   formatTimestamp(t.GetLastMessageAt()),
				UnreadCount:     t.GetUnreadCount(),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Threads []chatThreadResponse `json:"threads"`
		}{Threads: threads})
	}
}

// handleListChatMessages — GET /v1/chat/{partner_id}/messages?since=<RFC3339>.
// viewer_type=admin — как побочный эффект помечает партнёрские сообщения
// этого треда read_at=now() (см. chat.proto ChatService.ListMessages
// docstring), что и обнуляет unread_count в следующем ListThreads.
func handleListChatMessages(client grpcv1.ChatServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		partnerID := chi.URLParam(r, "partner_id")
		if partnerID == "" {
			http.Error(w, "partner_id обязателен", http.StatusBadRequest)
			return
		}

		var sinceTS *timestamppb.Timestamp
		if raw := r.URL.Query().Get("since"); raw != "" {
			t, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				http.Error(w, "неверный формат since (ожидался RFC3339): "+err.Error(), http.StatusBadRequest)
				return
			}
			sinceTS = timestamppb.New(t)
		}

		resp, err := client.ListMessages(r.Context(), &grpcv1.ListMessagesRequest{
			PartnerId: partnerID, Since: sinceTS, ViewerType: "admin",
		})
		if err != nil {
			writeChatGRPCError(w, "chat_messages_list", err)
			return
		}
		messages := make([]chatMessageResponse, 0, len(resp.GetMessages()))
		for _, m := range resp.GetMessages() {
			messages = append(messages, toChatMessageResponse(m))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Messages []chatMessageResponse `json:"messages"`
		}{Messages: messages})
	}
}

type sendChatMessageRequestBody struct {
	Body string `json:"body"`
}

// handleSendChatMessage — POST /v1/chat/{partner_id}/messages.
// sender_type фиксирован "admin", sender_id — ВСЕГДА claims.Subject.
func handleSendChatMessage(client grpcv1.ChatServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		partnerID := chi.URLParam(r, "partner_id")
		if partnerID == "" {
			http.Error(w, "partner_id обязателен", http.StatusBadRequest)
			return
		}
		var body sendChatMessageRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := client.SendMessage(r.Context(), &grpcv1.SendMessageRequest{
			PartnerId: partnerID, SenderType: "admin", SenderId: claims.Subject, Body: body.Body,
		})
		if err != nil {
			writeChatGRPCError(w, "chat_message_send", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(toChatMessageResponse(resp))
	}
}

// writeChatGRPCError — маппинг кодов ошибок ChatService в HTTP-статусы,
// тот же паттерн, что writeIncidentGRPCError в incidents.go.
func writeChatGRPCError(w http.ResponseWriter, context string, err error) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		internalError(w, http.StatusInternalServerError, context+": gRPC-вызов Chat Service не удался", err)
	}
}
