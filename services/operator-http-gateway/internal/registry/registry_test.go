package registry

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewClientFromRedis(rdb, "operator-http-gateway-test-0", 10*time.Second)
}

func TestRegisterWritesExpectedFields(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.Register(ctx, "beeline_uz", "route-1", 5, "https://api.beeline.uz/submit"); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	fields, err := c.Lookup(ctx, "beeline_uz", "route-1")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if fields["protocol"] != "HTTP" {
		t.Fatalf("ожидали protocol=HTTP, получили %s", fields["protocol"])
	}
	if fields["owning_instance_id"] != "operator-http-gateway-test-0" {
		t.Fatalf("неверный owning_instance_id: %s", fields["owning_instance_id"])
	}
	if fields["route_epoch"] != "5" {
		t.Fatalf("неверный route_epoch: %s", fields["route_epoch"])
	}
}

func TestHeartbeatUpdatesTimestampOnly(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	c.Register(ctx, "beeline_uz", "route-1", 1, "https://x")

	first, _ := c.Lookup(ctx, "beeline_uz", "route-1")
	time.Sleep(5 * time.Millisecond)
	if err := c.Heartbeat(ctx, "beeline_uz", "route-1"); err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	second, _ := c.Lookup(ctx, "beeline_uz", "route-1")
	if first["heartbeat"] == second["heartbeat"] {
		t.Fatalf("heartbeat должен измениться")
	}
	if second["protocol"] != "HTTP" {
		t.Fatalf("heartbeat не должен менять остальные поля")
	}
}

func TestUnregisterRemovesEntry(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	c.Register(ctx, "beeline_uz", "route-1", 1, "https://x")
	if err := c.Unregister(ctx, "beeline_uz", "route-1"); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	fields, _ := c.Lookup(ctx, "beeline_uz", "route-1")
	if len(fields) != 0 {
		t.Fatalf("ожидали пустой результат после unregister, получили %v", fields)
	}
}