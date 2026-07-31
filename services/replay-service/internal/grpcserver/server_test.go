// Тесты orchestration-логики RequestReplay через фейки (CODE_REVIEW.md
// test-quality finding: раньше не было ни одного теста для server.go —
// оба Critical бага (#1 отсутствие проверки requested_by, #2 TOCTOU
// double-replay) были невидимы для существующего test suite).
package grpcserver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/replay-service/internal/core"
	"mpp/replay-service/internal/store"
)

// fakeDlqStore — реалистичная in-memory реализация claim-семантики
// (pending -> in_progress -> replayed/expired, release обратно в pending)
// под mutex — позволяет тестировать и последовательные, и конкурентные
// сценарии без реального Postgres (сама атомарность UPDATE ... RETURNING
// отдельно проверена против реального Postgres в internal/store/store_test.go
// TestClaimForReplayConcurrentClaimsOnlyOneWins).
type fakeDlqStore struct {
	mu      sync.Mutex
	records map[string]*fakeDlqRow

	markReplayedErr error // если не nil, MarkReplayed возвращает эту ошибку
}

type fakeDlqRow struct {
	record core.DlqRecord
	status string
}

func newFakeDlqStore() *fakeDlqStore {
	return &fakeDlqStore{records: make(map[string]*fakeDlqRow)}
}

func (f *fakeDlqStore) put(id string, record core.DlqRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record.ReplayStatus = "pending"
	f.records[id] = &fakeDlqRow{record: record, status: "pending"}
}

func (f *fakeDlqStore) ClaimForReplay(ctx context.Context, id string) (core.DlqRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.records[id]
	if !ok {
		return core.DlqRecord{}, false, fmt.Errorf("not found")
	}
	if row.status != "pending" {
		return core.DlqRecord{StageExecutionID: id, ReplayStatus: row.status}, false, nil
	}
	row.status = "in_progress"
	rec := row.record
	rec.ReplayStatus = "in_progress"
	return rec, true, nil
}

func (f *fakeDlqStore) ReleaseClaim(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.records[id]; ok && row.status == "in_progress" {
		row.status = "pending"
	}
	return nil
}

func (f *fakeDlqStore) MarkReplayed(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markReplayedErr != nil {
		return f.markReplayedErr
	}
	if row, ok := f.records[id]; ok {
		row.status = "replayed"
	}
	return nil
}

func (f *fakeDlqStore) MarkExpired(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if row, ok := f.records[id]; ok {
		row.status = "expired"
	}
	return nil
}

func (f *fakeDlqStore) status(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[id].status
}

type fakeLedger struct{ chargeExists bool }

func (f fakeLedger) ChargeExistsInLedger(ctx context.Context, chargeID string) (bool, error) {
	return f.chargeExists, nil
}

type fakeCorrelation struct{ exists bool }

func (f fakeCorrelation) DlrCorrelationExists(ctx context.Context, stageExecutionID string) (bool, error) {
	return f.exists, nil
}

type fakeAudit struct {
	mu      sync.Mutex
	entries []string // outcome values, порядок сохраняется
}

func (f *fakeAudit) WriteAudit(ctx context.Context, stageExecutionID, requestedBy string, checks store.ChecksPassed, outcome, targetTopic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, outcome)
	return nil
}

type fakeControl struct{ pausedStage string }

func (f fakeControl) IsPaused(stageName string) bool {
	return f.pausedStage != "" && f.pausedStage == stageName
}

type fakePublisher struct {
	mu        sync.Mutex
	published int
	failNext  bool
}

func (f *fakePublisher) Republish(ctx context.Context, cmd *commonv1.StageExecuteCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return fmt.Errorf("simulated publish failure")
	}
	f.published++
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published
}

func testRecord(t *testing.T, stageExecutionID, stageName string, ttl time.Time) core.DlqRecord {
	t.Helper()
	cmd := &commonv1.StageExecuteCommand{
		MessageId: "msg-" + stageExecutionID,
		StageName: stageNameEnum(stageName),
		MessageTtl: timestamppb.New(ttl),
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal test command failed: %v", err)
	}
	return core.DlqRecord{
		StageExecutionID: stageExecutionID,
		MessageID:        cmd.GetMessageId(),
		StageName:        stageName,
		OriginalCommand:  payload,
		MessageTTL:       ttl,
	}
}

func stageNameEnum(s string) commonv1.StageName {
	v, ok := commonv1.StageName_value["STAGE_NAME_"+s]
	if !ok {
		return commonv1.StageName_STAGE_NAME_UNSPECIFIED
	}
	return commonv1.StageName(v)
}

func TestRequestReplayRejectsMissingRequestedBy(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-1", testRecord(t, "se-1", "ROUTING", time.Now().Add(time.Hour)))
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-1"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "REQUESTED_BY_REQUIRED" {
		t.Fatalf("ожидали REQUESTED_BY_REQUIRED, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
	if dlq.status("se-1") != "pending" {
		t.Fatalf("запись не должна была быть тронута до проверки requested_by, статус=%s", dlq.status("se-1"))
	}
}

func TestRequestReplayHappyPath(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-2", testRecord(t, "se-2", "ROUTING", time.Now().Add(time.Hour)))
	audit := &fakeAudit{}
	pub := &fakePublisher{}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, audit, fakeControl{}, pub)

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-2", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("ожидали Accepted=true, получили reason=%q", resp.GetRejectionReason())
	}
	if pub.count() != 1 {
		t.Fatalf("ожидали 1 republish, получили %d", pub.count())
	}
	if dlq.status("se-2") != "replayed" {
		t.Fatalf("ожидали статус replayed, получили %s", dlq.status("se-2"))
	}
	if len(audit.entries) != 1 || audit.entries[0] != "REPUBLISHED" {
		t.Fatalf("ожидали одну аудит-запись REPUBLISHED, получили %+v", audit.entries)
	}
}

// TestRequestReplayConcurrentCallsOnlyOnePublishes — CODE_REVIEW.md
// CRITICAL finding #2 на уровне orchestration (реальная атомарность
// проверена в store_test.go против настоящего Postgres) — здесь
// проверяем, что Server.RequestReplay корректно полагается на
// claimed=false и не republish'ит, если claim не удался.
func TestRequestReplayConcurrentCallsOnlyOnePublishes(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-3", testRecord(t, "se-3", "DELIVERY", time.Now().Add(time.Hour)))
	pub := &fakePublisher{}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, pub)

	const concurrency = 20
	var wg sync.WaitGroup
	var accepted int64
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-3", RequestedBy: "ops@mpp"})
			if err != nil {
				t.Errorf("RequestReplay failed: %v", err)
				return
			}
			if resp.GetAccepted() {
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	wg.Wait()

	if accepted != 1 {
		t.Fatalf("ожидали ровно 1 Accepted из %d конкурентных вызовов, получили %d", concurrency, accepted)
	}
	if pub.count() != 1 {
		t.Fatalf("ожидали ровно 1 republish, получили %d — двойная физическая доставка", pub.count())
	}
}

func TestRequestReplayRejectsExpiredTTL(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-4", testRecord(t, "se-4", "ROUTING", time.Now().Add(-time.Hour)))
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-4", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "TTL_EXPIRED" {
		t.Fatalf("ожидали TTL_EXPIRED, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
	if dlq.status("se-4") != "expired" {
		t.Fatalf("ожидали статус expired, получили %s", dlq.status("se-4"))
	}
}

func TestRequestReplayBillingUnsafeReleasesClaimBackToPending(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-5", testRecord(t, "se-5", "BILLING", time.Now().Add(time.Hour)))
	srv := New(dlq, fakeLedger{chargeExists: true}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-5", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "BILLING_SIDE_EFFECT_UNSAFE" {
		t.Fatalf("ожидали BILLING_SIDE_EFFECT_UNSAFE, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
	// CODE_REVIEW.md-consistent поведение: unsafe-проверка не должна
	// сжигать запись навсегда — она возвращается в pending, доступна для
	// будущей попытки, если условие перестанет быть unsafe.
	if dlq.status("se-5") != "pending" {
		t.Fatalf("ожидали, что claim будет освобождён обратно в pending, получили %s", dlq.status("se-5"))
	}
}

func TestRequestReplayDeliveryAmbiguousReleasesClaim(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-6", testRecord(t, "se-6", "DELIVERY", time.Now().Add(time.Hour)))
	srv := New(dlq, fakeLedger{}, fakeCorrelation{exists: true}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-6", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "DELIVERY_AMBIGUITY_UNSAFE" {
		t.Fatalf("ожидали DELIVERY_AMBIGUITY_UNSAFE, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
	if dlq.status("se-6") != "pending" {
		t.Fatalf("ожидали освобождённый claim (pending), получили %s", dlq.status("se-6"))
	}
}

// TestRequestReplayRejectsWhenStagePaused — CODE_REVIEW.md HIGH finding
// #4: раньше republish полностью игнорировал execution.control.
func TestRequestReplayRejectsWhenStagePaused(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-7", testRecord(t, "se-7", "BILLING", time.Now().Add(time.Hour)))
	pub := &fakePublisher{}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{pausedStage: "BILLING"}, pub)

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-7", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "STAGE_PAUSED" {
		t.Fatalf("ожидали STAGE_PAUSED, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
	if pub.count() != 0 {
		t.Fatalf("republish не должен был вызываться при активной паузе стадии")
	}
	if dlq.status("se-7") != "pending" {
		t.Fatalf("ожидали освобождённый claim (pending), получили %s", dlq.status("se-7"))
	}
}

func TestRequestReplayDoesNotBlockOnPauseOfOtherStage(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-8", testRecord(t, "se-8", "ROUTING", time.Now().Add(time.Hour)))
	pub := &fakePublisher{}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{pausedStage: "BILLING"}, pub)

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-8", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("пауза BILLING не должна блокировать ROUTING, получили reason=%q", resp.GetRejectionReason())
	}
}

// TestRequestReplayMarkReplayedFailureStillReportsAcceptedButLeavesInProgress
// — CODE_REVIEW.md HIGH finding #3: раньше ошибка MarkReplayed
// отбрасывалась и запись оставалась 'pending', открывая молчаливый
// повторный replay. Теперь запись уже 'in_progress' (claim), и при сбое
// MarkReplayed остаётся 'in_progress' (требует ручного вмешательства) —
// fail-safe, не fail-open. Сообщение уже реально ушло в Kafka, поэтому
// Accepted всё равно true.
func TestRequestReplayMarkReplayedFailureStillReportsAcceptedButLeavesInProgress(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-9", testRecord(t, "se-9", "ROUTING", time.Now().Add(time.Hour)))
	dlq.markReplayedErr = fmt.Errorf("simulated db failure")
	audit := &fakeAudit{}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, audit, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-9", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("сообщение реально ушло в Kafka — Accepted должен остаться true даже при сбое MarkReplayed")
	}
	if dlq.status("se-9") != "in_progress" {
		t.Fatalf("ожидали, что запись останется in_progress (fail-safe) при сбое MarkReplayed, получили %s", dlq.status("se-9"))
	}
	if len(audit.entries) != 1 || audit.entries[0] != "REPUBLISHED_BUT_MARK_FAILED" {
		t.Fatalf("ожидали аудит REPUBLISHED_BUT_MARK_FAILED, получили %+v", audit.entries)
	}
}

func TestRequestReplayAlreadyInProgressRejected(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-10", testRecord(t, "se-10", "ROUTING", time.Now().Add(time.Hour)))
	if _, claimed, err := dlq.ClaimForReplay(context.Background(), "se-10"); err != nil || !claimed {
		t.Fatalf("setup claim failed")
	}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-10", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "REPLAY_IN_PROGRESS" {
		t.Fatalf("ожидали REPLAY_IN_PROGRESS, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
}

func TestRequestReplayAlreadyReplayedRejected(t *testing.T) {
	dlq := newFakeDlqStore()
	dlq.put("se-11", testRecord(t, "se-11", "ROUTING", time.Now().Add(time.Hour)))
	if _, claimed, err := dlq.ClaimForReplay(context.Background(), "se-11"); err != nil || !claimed {
		t.Fatalf("setup claim failed")
	}
	if err := dlq.MarkReplayed(context.Background(), "se-11"); err != nil {
		t.Fatalf("setup MarkReplayed failed: %v", err)
	}
	srv := New(dlq, fakeLedger{}, fakeCorrelation{}, &fakeAudit{}, fakeControl{}, &fakePublisher{})

	resp, err := srv.RequestReplay(context.Background(), &grpcv1.RequestReplayRequest{StageExecutionId: "se-11", RequestedBy: "ops@mpp"})
	if err != nil {
		t.Fatalf("RequestReplay failed: %v", err)
	}
	if resp.GetAccepted() || resp.GetRejectionReason() != "ALREADY_REPLAYED" {
		t.Fatalf("ожидали ALREADY_REPLAYED, получили accepted=%v reason=%q", resp.GetAccepted(), resp.GetRejectionReason())
	}
}
