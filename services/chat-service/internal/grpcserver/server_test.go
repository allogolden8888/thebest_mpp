package grpcserver

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/chat-service/internal/store"
)

// fakeStore — реализует Store без реального Postgres, тот же паттерн, что
// incident-service/internal/grpcserver/server_test.go.
type fakeStore struct {
	messages []store.ChatMessage
	threads  []store.ChatThread

	sendErr error
	listErr error

	lastSendPartnerID  string
	lastSendSenderType string
	lastSendSenderID   string
	lastSendBody       string

	lastListPartnerID  string
	lastListSince      *time.Time
	lastListViewerType string
}

func (f *fakeStore) SendMessage(_ context.Context, partnerID, senderType, senderID, body string) (store.ChatMessage, error) {
	f.lastSendPartnerID, f.lastSendSenderType, f.lastSendSenderID, f.lastSendBody = partnerID, senderType, senderID, body
	if f.sendErr != nil {
		return store.ChatMessage{}, f.sendErr
	}
	return store.ChatMessage{ID: 1, PartnerID: partnerID, SenderType: senderType, SenderID: senderID, Body: body, CreatedAt: time.Unix(1000, 0)}, nil
}

func (f *fakeStore) ListMessages(_ context.Context, partnerID string, since *time.Time, viewerType string) ([]store.ChatMessage, error) {
	f.lastListPartnerID, f.lastListSince, f.lastListViewerType = partnerID, since, viewerType
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.messages, nil
}

func (f *fakeStore) ListThreads(_ context.Context) ([]store.ChatThread, error) {
	return f.threads, nil
}

func TestSendMessageValidation(t *testing.T) {
	tests := []struct {
		name string
		req  *grpcv1.SendMessageRequest
	}{
		{"пустой partner_id", &grpcv1.SendMessageRequest{PartnerId: "", SenderType: "admin", SenderId: "a", Body: "b"}},
		{"неизвестный sender_type", &grpcv1.SendMessageRequest{PartnerId: "p", SenderType: "bot", SenderId: "a", Body: "b"}},
		{"пустой sender_id", &grpcv1.SendMessageRequest{PartnerId: "p", SenderType: "admin", SenderId: "", Body: "b"}},
		{"пустой body", &grpcv1.SendMessageRequest{PartnerId: "p", SenderType: "admin", SenderId: "a", Body: ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(&fakeStore{})
			_, err := s.SendMessage(context.Background(), tt.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ожидали codes.InvalidArgument, получили %v", err)
			}
		})
	}
}

func TestSendMessageSuccess(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.SendMessage(context.Background(), &grpcv1.SendMessageRequest{
		PartnerId: "payme_uz", SenderType: "admin", SenderId: "a.karimov", Body: "Проверяем, ответим в течение дня",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if fs.lastSendPartnerID != "payme_uz" || fs.lastSendSenderType != "admin" || fs.lastSendSenderID != "a.karimov" || fs.lastSendBody != "Проверяем, ответим в течение дня" {
		t.Fatalf("store вызван с неожиданными аргументами: %+v", fs)
	}
	if resp.GetId() != 1 || resp.GetPartnerId() != "payme_uz" {
		t.Fatalf("неожиданный ответ: %+v", resp)
	}
}

func TestListMessagesValidation(t *testing.T) {
	s := New(&fakeStore{})
	_, err := s.ListMessages(context.Background(), &grpcv1.ListMessagesRequest{PartnerId: "", ViewerType: "admin"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали InvalidArgument для пустого partner_id, получили %v", err)
	}

	_, err = s.ListMessages(context.Background(), &grpcv1.ListMessagesRequest{PartnerId: "p", ViewerType: "moderator"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали InvalidArgument для неизвестного viewer_type, получили %v", err)
	}
}

// TestListMessagesSinceNilWhenNotSet — доказывает, что отсутствие поля
// since в запросе транслируется в nil *time.Time на уровне store (не
// нулевое время эпохи Unix) — см. doc-комментарий в server.go про
// nil-check вместо AsTime().Unix()==0.
func TestListMessagesSinceNilWhenNotSet(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	_, err := s.ListMessages(context.Background(), &grpcv1.ListMessagesRequest{PartnerId: "p", ViewerType: "admin"})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if fs.lastListSince != nil {
		t.Fatalf("ожидали nil since при неустановленном поле, получили %v", *fs.lastListSince)
	}

	sinceTS := timestamppb.New(time.Unix(500, 0))
	_, err = s.ListMessages(context.Background(), &grpcv1.ListMessagesRequest{PartnerId: "p", ViewerType: "admin", Since: sinceTS})
	if err != nil {
		t.Fatalf("ListMessages (с since): %v", err)
	}
	if fs.lastListSince == nil || !fs.lastListSince.Equal(time.Unix(500, 0)) {
		t.Fatalf("ожидали since=Unix(500,0), получили %v", fs.lastListSince)
	}
}

func TestListThreadsMapsFields(t *testing.T) {
	fs := &fakeStore{threads: []store.ChatThread{
		{PartnerID: "payme_uz", LastBody: "Здравствуйте", LastMessageAt: time.Unix(2000, 0), UnreadCount: 2},
	}}
	s := New(fs)

	resp, err := s.ListThreads(context.Background(), &grpcv1.ListThreadsRequest{})
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(resp.GetThreads()) != 1 {
		t.Fatalf("ожидали 1 тред, получили %d", len(resp.GetThreads()))
	}
	th := resp.GetThreads()[0]
	if th.GetPartnerId() != "payme_uz" || th.GetLastMessageBody() != "Здравствуйте" || th.GetUnreadCount() != 2 {
		t.Fatalf("неожиданное содержимое треда: %+v", th)
	}
}
