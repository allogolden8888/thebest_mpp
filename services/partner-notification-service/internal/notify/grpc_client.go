// Package notify — send_deliver_sm/send_rest_callback (service_internal_methods.md
// §7.1). gRPC-клиент — реальный сгенерированный стаб против
// `platform-contracts/grpc/partner_gateway.proto`; сервер (Partner SMPP
// Gateway) не реализован ни в одном репозитории на момент написания
// (владелец — Субагент 1) — вызов компилируется и типобезопасен, но live
// не проверен.
package notify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-notification-service/internal/registry"
)

type Outcome int

const (
	OutcomeDelivered Outcome = iota
	OutcomeRetryable
	OutcomePermanentFailure
)

type SmppClient struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	timeout time.Duration
}

func NewSmppClient(timeout time.Duration) *SmppClient {
	return &SmppClient{conns: make(map[string]*grpc.ClientConn), timeout: timeout}
}

func (c *SmppClient) connFor(endpoint string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[endpoint]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient(%s): %w", endpoint, err)
	}
	c.conns[endpoint] = conn
	return conn, nil
}

func (c *SmppClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

// DeliverSm — реальный gRPC вызов. `STALE_EPOCH`/`NO_ACTIVE_SESSION` —
// retryable (партнёр мог переподключиться, registry обновится, следующая
// попытка резолвит свежий endpoint/epoch заново — Lookup вызывается
// каждый раз, не кэшируется между попытками).
func (c *SmppClient) DeliverSm(ctx context.Context, endpoint registry.GatewayEndpoint, messageID, partnerID, systemID, statusText string, payload []byte) (Outcome, error) {
	conn, err := c.connFor(endpoint.Endpoint)
	if err != nil {
		return OutcomeRetryable, err
	}
	client := grpcv1.NewPartnerDeliverSmServiceClient(conn)

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := client.DeliverSm(callCtx, &grpcv1.DeliverSmRequest{
		MessageId:    messageID,
		PartnerId:    partnerID,
		SystemId:     systemID,
		SessionId:    endpoint.SessionID,
		SessionEpoch: endpoint.SessionEpoch,
		StatusText:   statusText,
		Payload:      payload,
	})
	if err != nil {
		return OutcomeRetryable, fmt.Errorf("DeliverSm rpc: %w", err)
	}

	switch resp.GetStatus() {
	case grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_DELIVERED:
		return OutcomeDelivered, nil
	case grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_STALE_EPOCH, grpcv1.DeliverSmStatus_DELIVER_SM_STATUS_NO_ACTIVE_SESSION:
		return OutcomeRetryable, nil
	default:
		return OutcomeRetryable, nil
	}
}
