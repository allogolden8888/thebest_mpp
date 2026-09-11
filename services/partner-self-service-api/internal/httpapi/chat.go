// Package httpapi — GET/POST /v1/self-service/chat/messages
// (BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat", partner-portal-ui side).
// Тонкий gRPC-прокси поверх ChatService (platform-contracts/grpc/chat.proto)
// — та же ChatService, что backoffice-api/internal/httpapi/chat.go проксирует
// для backoffice-ui, здесь — единственный тред вызывающего партнёра.
//
// partner_id ВСЕГДА claims.PartnerID, sender_id ВСЕГДА claims.Subject,
// sender_type фиксирован "partner" (в отличие от backoffice-api's chat.go,
// где он фиксирован "admin") — тот же принцип, что claims.PartnerID во всех
// остальных хендлерах этого сервиса (partnerconfig.go/credentials.go):
// вызывающий не может ни прочитать чужой тред, ни отправить сообщение от
// чужого имени, подменив параметр в запросе. Нет ListThreads-эндпоинта
// здесь — партнёр не выбирает "чей тред читать", у него только один (см.
// platform-contracts/grpc/chat.proto ChatService.ListThreads docstring:
// "Не вызывается partner-self-service-api").
//
// Роль (RequireAdmin) НЕ гейтит эти эндпоинты — чтение/отправка в чат
// поддержки открыты любому валидному partner-токену, тот же класс решения,
// что handleListCredentials/handleGetWebhook (мутация чужого конфига
// требует partner-admin, но переписка с поддержкой — нет, это не
// разрушительное действие).
package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
)

const chatRPCTimeout = 5 * time.Second

func mountChat(r chi.Router, d Deps) {
	r.Get("/chat/messages", handleListChatMessages(d.ChatClient))
	r.Post("/chat/messages", handleSendChatMessage(d.ChatClient))
}

type chatMessageResponse struct {
	ID         int64  `json:"id"`
	PartnerID  string `json:"partner_id"`
	SenderType string `json:"sender_type"`
	SenderID   string `json:"sender_id"`
	Body       string `json:"body"`
	CreatedAt  string `json:"created_at,omitempty"`
	ReadAt     string `json:"read_at,omitempty"`
}

// formatTimestamp — mirрор backoffice-api/internal/httpapi/chat.go: nil
// (сообщение ещё не прочитано противоположной стороной, или поле не
// установлено) сериализуется как отсутствующий/пустой JSON-строковый ключ,
// не "0001-01-01T00:00:00Z".
func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339Nano)
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

// handleListChatMessages — GET /chat/messages?since=<RFC3339>. viewer_type
// зафиксирован "partner" — как побочный эффект (см. chat.proto
// ChatService.ListMessages docstring) помечает read_at=now() у сообщений
// admin-стороны этого треда, обнуляя "непрочитанное" с точки зрения
// партнёра. partner_id — всегда claims.PartnerID, никогда из query.
func handleListChatMessages(client grpcv1.ChatServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
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

		ctx, cancel := context.WithTimeout(r.Context(), chatRPCTimeout)
		defer cancel()

		resp, err := client.ListMessages(ctx, &grpcv1.ListMessagesRequest{
			PartnerId: claims.PartnerID, Since: sinceTS, ViewerType: "partner",
		})
		if err != nil {
			writeChatGRPCError(w, err)
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

// handleSendChatMessage — POST /chat/messages. sender_type фиксирован
// "partner", partner_id/sender_id — всегда claims.PartnerID/claims.Subject.
func handleSendChatMessage(client grpcv1.ChatServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		var body sendChatMessageRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), chatRPCTimeout)
		defer cancel()

		resp, err := client.SendMessage(ctx, &grpcv1.SendMessageRequest{
			PartnerId: claims.PartnerID, SenderType: "partner", SenderId: claims.Subject, Body: body.Body,
		})
		if err != nil {
			writeChatGRPCError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(toChatMessageResponse(resp))
	}
}

// writeChatGRPCError — тот же паттерн, что writeGRPCError (credentials.go):
// InvalidArgument -> 400, всё остальное -> 502 (сбой нижестоящего
// chat-service, не самого partner-self-service-api).
func writeChatGRPCError(w http.ResponseWriter, err error) {
	if status.Code(err) == codes.InvalidArgument {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, "сбой нижестоящего сервиса: "+err.Error(), http.StatusBadGateway)
}
