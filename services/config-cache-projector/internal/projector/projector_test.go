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

// CODE_REVIEW.md finding #2: malformed/unset entity_type is silently
// accepted and projected under a bogus "unspecified" key. Fixed:
// kafkaio.DecodeConfigChangeEvent already rejects this before it reaches
// WriteProjection, but WriteProjection itself must not silently trust
// that — defense in depth.
func TestWriteProjectionRejectsUnspecifiedEntityType(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		EntityId:    "x",
		Version:     1,
		PayloadJson: []byte(`{}`),
		Status:      "active",
	}
	if err := c.WriteProjection(ctx, event); err == nil {
		t.Fatalf("ожидали ошибку для entity_type=UNSPECIFIED")
	}

	if _, err := c.CurrentVersion(ctx, "unspecified", "x"); err == nil {
		t.Fatalf("не должно быть записи под config:current:unspecified:x — WriteProjection должен был отклонить событие до записи")
	}
}

// CODE_REVIEW.md finding #3: WriteProjection did two non-atomic Redis
// writes with no compensation on partial failure. This test doesn't
// simulate a mid-write network failure (miniredis doesn't expose that
// hook), but it does assert that a single WriteProjection call for
// status=active leaves both keys mutually consistent — a regression test
// for the shape of the bug (split-brain between config:version and
// config:current), even though the atomicity guarantee itself
// (TxPipelined/MULTI-EXEC) is exercised structurally, not by fault
// injection.
func TestWriteProjectionKeepsVersionAndCurrentConsistentForActive(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE, EntityId: "default",
		Version: 5, PayloadJson: []byte(`{"v":5}`), Status: "active",
	}
	if err := c.WriteProjection(ctx, event); err != nil {
		t.Fatalf("WriteProjection failed: %v", err)
	}

	current, err := c.CurrentVersion(ctx, "routing_table", "default")
	if err != nil {
		t.Fatalf("CurrentVersion failed: %v", err)
	}
	if current != 5 {
		t.Fatalf("ожидали current=5, получили %d", current)
	}
	payload, err := c.VersionPayload(ctx, "routing_table", "default", 5)
	if err != nil {
		t.Fatalf("VersionPayload failed: %v", err)
	}
	if string(payload) != `{"v":5}` {
		t.Fatalf("payload не совпадает: %s", payload)
	}
}

// Ф2 плана (luminous-hugging-charm.md, "Реестр отправителей"): partner
// entity_type + status=active должен спроецировать senders[] в
// config:sender:{sender_id} -> partner_id, ПОМИМО существующих
// config:current/config:version для самого partner-объекта (не в
// изоляции — если этот тест сломает существующий путь, это регрессия).
func TestWriteProjectionWritesSenderOwnersForActivePartner(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    3,
		PayloadJson: []byte(`{
			"partner_id": "acme",
			"senders": [
				{"sender_id": "ACME-ALPHA", "type": "ALPHANAME", "status": "active"},
				{"sender_id": "998900000000", "type": "SHORT_NUMBER", "status": "active"}
			]
		}`),
		Status: "active",
	}

	if err := c.WriteProjection(ctx, event); err != nil {
		t.Fatalf("WriteProjection failed: %v", err)
	}

	// Существующий путь не должен был сломаться.
	current, err := c.CurrentVersion(ctx, "partner", "acme")
	if err != nil {
		t.Fatalf("CurrentVersion failed: %v", err)
	}
	if current != 3 {
		t.Fatalf("ожидали current version 3, получили %d", current)
	}
	payload, err := c.VersionPayload(ctx, "partner", "acme", 3)
	if err != nil {
		t.Fatalf("VersionPayload failed: %v", err)
	}
	if string(payload) != string(event.PayloadJson) {
		t.Fatalf("payload не совпадает: %s", payload)
	}

	// Новый sender-реестр.
	for _, senderID := range []string{"ACME-ALPHA", "998900000000"} {
		got, err := c.rdb.Get(ctx, senderOwnerKey(senderID)).Result()
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", senderOwnerKey(senderID), err)
		}
		if got != "acme" {
			t.Fatalf("config:sender:%s = %q, ожидали \"acme\"", senderID, got)
		}
	}
}

// Гейтирование config:sender:* должно совпадать с гейтированием
// config:current — архивная версия не должна затирать живой реестр
// отправителей устаревшими данными.
func TestWriteProjectionDoesNotWriteSenderOwnersForArchivedPartner(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    4,
		PayloadJson: []byte(`{
			"partner_id": "acme",
			"senders": [{"sender_id": "ACME-ALPHA", "type": "ALPHANAME", "status": "active"}]
		}`),
		Status: "archived",
	}

	if err := c.WriteProjection(ctx, event); err != nil {
		t.Fatalf("WriteProjection failed: %v", err)
	}

	// config:version всё равно должен быть записан (существующее поведение).
	payload, err := c.VersionPayload(ctx, "partner", "acme", 4)
	if err != nil {
		t.Fatalf("VersionPayload failed: %v", err)
	}
	if string(payload) != string(event.PayloadJson) {
		t.Fatalf("archived payload не совпадает: %s", payload)
	}

	if _, err := c.rdb.Get(ctx, senderOwnerKey("ACME-ALPHA")).Result(); err != redis.Nil {
		t.Fatalf("config:sender:ACME-ALPHA не должен быть записан для archived-версии, err=%v", err)
	}
}

// Малформед/отсутствующий senders в payload не должен ронять запись
// config:current/config:version для самого partner-объекта.
func TestWriteProjectionToleratesMalformedSendersPayload(t *testing.T) {
	cases := map[string]string{
		"malformed_json":  `{not valid json`,
		"missing_senders": `{"partner_id":"acme"}`,
		"senders_empty":   `{"partner_id":"acme","senders":[]}`,
	}

	for name, payloadJSON := range cases {
		t.Run(name, func(t *testing.T) {
			c := newTestClient(t)
			ctx := context.Background()

			event := &eventsv1.ConfigChangeEvent{
				EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
				EntityId:    "acme",
				Version:     1,
				PayloadJson: []byte(payloadJSON),
				Status:      "active",
			}

			if err := c.WriteProjection(ctx, event); err != nil {
				t.Fatalf("WriteProjection failed (payload=%s): %v", payloadJSON, err)
			}

			current, err := c.CurrentVersion(ctx, "partner", "acme")
			if err != nil {
				t.Fatalf("CurrentVersion failed: %v", err)
			}
			if current != 1 {
				t.Fatalf("ожидали current version 1, получили %d", current)
			}
		})
	}
}

// Sanity check: sender-реестр строго ограничен entity_type=partner —
// другие entity types (например policy_template) не должны писать
// config:sender:* даже если бы в их payload случайно оказалось поле
// senders.
func TestWriteProjectionDoesNotWriteSenderOwnersForNonPartnerEntity(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE,
		EntityId:   "tmpl-1",
		Version:    1,
		PayloadJson: []byte(`{
			"partner_id": "acme",
			"senders": [{"sender_id": "ACME-ALPHA", "type": "ALPHANAME", "status": "active"}]
		}`),
		Status: "active",
	}

	if err := c.WriteProjection(ctx, event); err != nil {
		t.Fatalf("WriteProjection failed: %v", err)
	}

	if _, err := c.rdb.Get(ctx, senderOwnerKey("ACME-ALPHA")).Result(); err != redis.Nil {
		t.Fatalf("config:sender:ACME-ALPHA не должен быть записан для entity_type=policy_template, err=%v", err)
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
