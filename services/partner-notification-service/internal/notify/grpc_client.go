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

// connEntry — LOW/MEDIUM находка кодревью: `conns` раньше рос неограниченно
// на весь жизненный цикл процесса, ключ — endpoint (адрес пода/инстанса
// Partner SMPP Gateway из registry). Т.к. инстансы адресуются по
// pod/instance endpoint через registry, который меняется на каждый
// рестарт/rescale гейтвея, долгоживущий процесс копил бы устаревшие,
// простаивающие `grpc.ClientConn` вместе с их keepalive-горутинами
// бесконечно. idleEvictInterval-тикер закрывает и убирает записи, не
// использованные дольше idleTTL — см. NewSmppClient/evictIdle.
type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

const (
	defaultIdleTTL           = 10 * time.Minute
	defaultIdleEvictInterval = time.Minute
)

type SmppClient struct {
	mu        sync.Mutex
	conns     map[string]*connEntry
	timeout   time.Duration
	idleTTL   time.Duration
	stopEvict chan struct{}
}

func NewSmppClient(timeout time.Duration) *SmppClient {
	c := &SmppClient{
		conns:     make(map[string]*connEntry),
		timeout:   timeout,
		idleTTL:   defaultIdleTTL,
		stopEvict: make(chan struct{}),
	}
	go c.evictIdleLoop(defaultIdleEvictInterval)
	return c
}

func (c *SmppClient) evictIdleLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.evictIdle()
		case <-c.stopEvict:
			return
		}
	}
}

func (c *SmppClient) evictIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-c.idleTTL)
	for endpoint, entry := range c.conns {
		if entry.lastUsed.Before(cutoff) {
			_ = entry.conn.Close()
			delete(c.conns, endpoint)
		}
	}
}

func (c *SmppClient) connFor(endpoint string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.conns[endpoint]; ok {
		entry.lastUsed = time.Now()
		return entry.conn, nil
	}
	// insecure.NewCredentials() здесь НАМЕРЕННО, не пропущенный mTLS
	// (кодревью PART 2 отметило это как HIGH — расследовано, признано
	// false positive, см. README "Проверено кодревью: gRPC insecure
	// credentials — намеренно, не находка"). hld.md §"Instance-addressed
	// RPC" требует mTLS для этого вызова, и он реально обеспечен — но на
	// уровне service mesh, не в коде приложения: namespace `mpp` целиком
	// помечен `istio-injection: enabled` (k8s/generate_manifests.py) и
	// `PeerAuthentication` в режиме STRICT (infra/istio/peer-authentication-strict.yaml)
	// отклоняет ЛЮБОЕ plaintext-соединение между подами namespace —
	// Envoy sidecar каждого пода прозрачно поднимает mTLS между собой,
	// приложение видит только localhost-плейнтекст до своего sidecar.
	// Добавление TLS-конфигурации здесь поверх mesh было бы избыточным
	// double-mTLS без единого документированного источника
	// certs/CA для application-уровня — начиная качать это самостоятельно
	// означало бы придумывать инфраструктуру, которой нигде не
	// специфицировано, вместо использования уже работающей.
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient(%s): %w", endpoint, err)
	}
	c.conns[endpoint] = &connEntry{conn: conn, lastUsed: time.Now()}
	return conn, nil
}

func (c *SmppClient) Close() {
	close(c.stopEvict)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.conns {
		_ = entry.conn.Close()
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
