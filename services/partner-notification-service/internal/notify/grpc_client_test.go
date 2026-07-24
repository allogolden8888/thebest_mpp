package notify

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-notification-service/internal/registry"
)

// fakeGatewayServer — реальная реализация PartnerDeliverSmServiceServer,
// не мок интерфейса клиента: тест поднимает настоящий gRPC-сервер на
// локальном порту (bufconn/localhost) и настоящий сгенерированный клиент
// говорит с ним по-настоящему.
type fakeGatewayServer struct {
	grpcv1.UnimplementedPartnerDeliverSmServiceServer
	status      grpcv1.DeliverSmStatus
	lastRequest *grpcv1.DeliverSmRequest
}

func (s *fakeGatewayServer) DeliverSm(ctx context.Context, req *grpcv1.DeliverSmRequest) (*grpcv1.DeliverSmResponse, error) {
	s.lastRequest = req
	return &grpcv1.DeliverSmResponse{Status: s.status}, nil
}

func startFakeGateway(t *testing.T, status grpcv1.DeliverSmStatus) (*fakeGatewayServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	server := grpc.NewServer()
	fake := &fakeGatewayServer{status: status}
	grpcv1.RegisterPartnerDeliverSmServiceServer(server, fake)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return fake, lis.Addr().String()
}

func TestDeliverSmAgainstRealGrpcServerDelivered(t *testing.T) {
	fake, addr := startFakeGateway(t, grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_DELIVERED)

	client := NewSmppClient(2 * time.Second)
	defer client.Close()

	endpoint := registry.GatewayEndpoint{Endpoint: addr, SessionID: "sess1", SessionEpoch: 7}
	outcome, err := client.DeliverSm(context.Background(), endpoint, "m1", "click_uz", "click_uz_main", "DELIVERED", []byte("payload"))
	if err != nil {
		t.Fatalf("DeliverSm: %v", err)
	}
	if outcome != OutcomeDelivered {
		t.Fatalf("outcome = %v, want OutcomeDelivered", outcome)
	}
	if fake.lastRequest.GetMessageId() != "m1" || fake.lastRequest.GetSessionEpoch() != 7 {
		t.Errorf("сервер получил неожиданный запрос: %+v", fake.lastRequest)
	}
}

func TestDeliverSmStaleEpochIsRetryable(t *testing.T) {
	_, addr := startFakeGateway(t, grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_STALE_EPOCH)

	client := NewSmppClient(2 * time.Second)
	defer client.Close()

	endpoint := registry.GatewayEndpoint{Endpoint: addr}
	outcome, err := client.DeliverSm(context.Background(), endpoint, "m1", "click_uz", "click_uz_main", "DELIVERED", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome != OutcomeRetryable {
		t.Fatalf("outcome = %v, want OutcomeRetryable (STALE_EPOCH — партнёр мог переподключиться)", outcome)
	}
}

func TestDeliverSmNoActiveSessionIsRetryable(t *testing.T) {
	_, addr := startFakeGateway(t, grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_NO_ACTIVE_SESSION)

	client := NewSmppClient(2 * time.Second)
	defer client.Close()

	endpoint := registry.GatewayEndpoint{Endpoint: addr}
	outcome, _ := client.DeliverSm(context.Background(), endpoint, "m1", "click_uz", "click_uz_main", "DELIVERED", nil)
	if outcome != OutcomeRetryable {
		t.Fatalf("outcome = %v, want OutcomeRetryable", outcome)
	}
}

func TestDeliverSmConnectionReuseAcrossCalls(t *testing.T) {
	// Регрессия: connFor должен кэшировать соединение по endpoint, не
	// открывать новое на каждый вызов.
	_, addr := startFakeGateway(t, grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_DELIVERED)

	client := NewSmppClient(2 * time.Second)
	defer client.Close()

	endpoint := registry.GatewayEndpoint{Endpoint: addr}
	for i := 0; i < 3; i++ {
		if _, err := client.DeliverSm(context.Background(), endpoint, "m1", "p1", "a1", "DELIVERED", nil); err != nil {
			t.Fatalf("вызов %d: %v", i, err)
		}
	}
	client.mu.Lock()
	connCount := len(client.conns)
	client.mu.Unlock()
	if connCount != 1 {
		t.Fatalf("ожидали 1 закэшированное соединение после 3 вызовов на один endpoint, получили %d", connCount)
	}
}
