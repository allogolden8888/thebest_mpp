package projector

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewClientFromRedis(rdb)
}

func TestWriteProjectionWritesVersionAndCurrentForActive(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		Version:     2,
		PayloadJson: []byte(`{"partner_id":"acme"}`),
		Status:      "active",
		CreatedAt:   timestamppb.Now(),
	}

	if err := c.WriteProjection(ctx, event); err != nil {
		t.Fatalf("WriteProjection failed: %v", err)
	}

	current, err := c.CurrentVersion(ctx, "partner", "acme")
	if err != nil {
		t.Fatalf("CurrentVersion failed: %v", err)
	}
	if current != 2 {
		t.Fatalf("ожидали current version 2, получили %d", current)
	}

	payload, err := c.VersionPayload(ctx, "partner", "acme", 2)
	if err != nil {
		t.Fatalf("VersionPayload failed: %v", err)
	}
	if string(payload) != `{"partner_id":"acme"}` {
		t.Fatalf("payload не совпадает: %s", payload)
	}
}

func TestWriteProjectionDoesNotUpdateCurrentForArchived(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	active := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR, EntityId: "beeline_uz",
		Version: 1, PayloadJson: []byte(`{}`), Status: "active",
	}
	if err := c.WriteProjection(ctx, active); err != nil {
		t.Fatalf("WriteProjection (active) failed: %v", err)
	}

	archived := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR, EntityId: "beeline_uz",
		Version: 2, PayloadJson: []byte(`{"v":2}`), Status: "archived",
	}
	if err := c.WriteProjection(ctx, archived); err != nil {
		t.Fatalf("WriteProjection (archived) failed: %v", err)
	}

	current, err := c.CurrentVersion(ctx, "operator", "beeline_uz")
	if err != nil {
		t.Fatalf("CurrentVersion failed: %v", err)
	}
	if current != 1 {
		t.Fatalf("archived-версия не должна становиться current, ожидали 1, получили %d", current)
	}

	// Но version-запись для архивной версии всё равно должна быть доступна.
	payload, err := c.VersionPayload(ctx, "operator", "beeline_uz", 2)
	if err != nil {
		t.Fatalf("VersionPayload(2) failed: %v", err)
	}
	if string(payload) != `{"v":2}` {
		t.Fatalf("archived payload не совпадает: %s", payload)
	}
}

func TestEntityTypeStringMatchesPostgresConvention(t *testing.T) {
	cases := map[commonv1.ConfigEntityType]string{
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE:           "pipeline",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET:     "policy_ruleset",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE:    "policy_template",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF:     "billing_tariff",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE:      "routing_table",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE:       "number_range",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER:            "partner",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR:           "operator",
		commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT: "subscriber_consent",
	}
	for proto, want := range cases {
		if got := entityTypeString(proto); got != want {
			t.Fatalf("entityTypeString(%v) = %q, want %q", proto, got, want)
		}
	}
}
