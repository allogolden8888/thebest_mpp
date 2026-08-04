package main

import (
	"context"
	"errors"
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/scheduler-critical-sweep/internal/sweep"
)

// fakeStore/fakePublisher — CODE_REVIEW.md "Test quality": раньше
// processTick принимал конкретные *redisio.Client/*kafkaio.Publisher и не
// мог быть протестирован без реального Redis/Kafka. Теперь processTick/
// processEntry зависят от deadlineStore/commandPublisher (интерфейсы,
// main.go) — эти фейки их реализуют.

type deferCall struct {
	entry sweep.ExpiredEntry
	until time.Time
}

type fakeStore struct {
	entries []sweep.ExpiredEntry
	tickErr error

	messageIDs map[string]string
	resolveErr map[string]error

	states  map[string]sweep.ExecutionState
	loadErr map[string]error

	claimResult map[string]bool
	claimErr    map[string]error
	claimCalls  []string

	restoreCalls []sweep.ExpiredEntry
	restoreErr   error

	deferCalls []deferCall
	deferErr   error
}

func (f *fakeStore) TickSweep(ctx context.Context, now time.Time) ([]sweep.ExpiredEntry, error) {
	return f.entries, f.tickErr
}

func (f *fakeStore) ResolveMessageID(ctx context.Context, stageExecutionID string) (string, error) {
	if err, ok := f.resolveErr[stageExecutionID]; ok {
		return "", err
	}
	return f.messageIDs[stageExecutionID], nil
}

func (f *fakeStore) LoadExecutionState(ctx context.Context, messageID string) (sweep.ExecutionState, error) {
	if err, ok := f.loadErr[messageID]; ok {
		return sweep.ExecutionState{}, err
	}
	return f.states[messageID], nil
}

func (f *fakeStore) ClaimDeadline(ctx context.Context, entry sweep.ExpiredEntry, now time.Time) (bool, error) {
	f.claimCalls = append(f.claimCalls, entry.StageExecutionID)
	if err, ok := f.claimErr[entry.StageExecutionID]; ok {
		return false, err
	}
	if claimed, ok := f.claimResult[entry.StageExecutionID]; ok {
		return claimed, nil
	}
	return true, nil // по умолчанию клэйм успешен — большинству тестов не важна конкуренция
}

func (f *fakeStore) RestoreDeadline(ctx context.Context, entry sweep.ExpiredEntry) error {
	f.restoreCalls = append(f.restoreCalls, entry)
	return f.restoreErr
}

func (f *fakeStore) DeferDeadline(ctx context.Context, entry sweep.ExpiredEntry, until time.Time) error {
	f.deferCalls = append(f.deferCalls, deferCall{entry: entry, until: until})
	return f.deferErr
}

type fakePublisher struct {
	retryCalls   []*commonv1.StageExecuteCommand
	timeoutCalls []*commonv1.StageCompletedEvent
	dlqCalls     []*eventsv1.DlqRecord

	retryErr   error
	timeoutErr error
	dlqErr     error
}

func (f *fakePublisher) PublishRetry(ctx context.Context, cmd *commonv1.StageExecuteCommand) error {
	f.retryCalls = append(f.retryCalls, cmd)
	return f.retryErr
}

func (f *fakePublisher) PublishTimeout(ctx context.Context, ev *commonv1.StageCompletedEvent) error {
	f.timeoutCalls = append(f.timeoutCalls, ev)
	return f.timeoutErr
}

func (f *fakePublisher) PublishDlq(ctx context.Context, rec *eventsv1.DlqRecord) error {
	f.dlqCalls = append(f.dlqCalls, rec)
	return f.dlqErr
}

type fakeSnapshot struct{ paused map[string]bool }

func (f fakeSnapshot) IsPaused(stageName string) bool { return f.paused[stageName] }

func billingStateWithExtensionData(attempt int32) sweep.ExecutionState {
	return sweep.ExecutionState{
		MessageID:          "msg-1",
		StageExecutionID:   "exec-1",
		StageName:          "BILLING",
		Attempt:            attempt,
		ResolvedOperatorID: "beeline",
		Category:           "TRANSACTION",
		SegmentCount:       2,
	}
}

// TestProcessEntryHoldDefersWithoutClaimingOrPublishing — finding #9: Hold
// не должен клэймить/публиковать вообще, только откладывать перепроверку.
func TestProcessEntryHoldDefersWithoutClaimingOrPublishing(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0, DeadlineUnixMs: now.UnixMilli()}
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		states:     map[string]sweep.ExecutionState{"msg-1": {MessageID: "msg-1", StageExecutionID: "exec-1", StageName: "BILLING", Attempt: 1}},
	}
	pub := &fakePublisher{}
	snap := fakeSnapshot{paused: map[string]bool{"BILLING": true}}

	processEntry(context.Background(), now, entry, store, pub, snap)

	if len(store.claimCalls) != 0 {
		t.Fatalf("Hold не должен клэймить, получили claimCalls=%v", store.claimCalls)
	}
	if len(pub.retryCalls) != 0 || len(pub.timeoutCalls) != 0 || len(pub.dlqCalls) != 0 {
		t.Fatalf("Hold не должен ничего публиковать")
	}
	if len(store.deferCalls) != 1 {
		t.Fatalf("ожидали ровно один DeferDeadline, получили %d", len(store.deferCalls))
	}
	if want := now.Add(heldBackoff); !store.deferCalls[0].until.Equal(want) {
		t.Fatalf("DeferDeadline.until = %v, want %v", store.deferCalls[0].until, want)
	}
}

// TestProcessEntryRetrySuccessClaimsThenPublishesWithIncrementedAttempt —
// happy path: достаточно данных для stage_extension -> клэйм -> publish_retry
// с attempt+1.
func TestProcessEntryRetrySuccessClaimsThenPublishesWithIncrementedAttempt(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0, DeadlineUnixMs: now.UnixMilli()}
	state := billingStateWithExtensionData(1)
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		states:     map[string]sweep.ExecutionState{"msg-1": state},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(store.claimCalls) != 1 || store.claimCalls[0] != "exec-1" {
		t.Fatalf("ожидали один клэйм exec-1, получили %v", store.claimCalls)
	}
	if len(pub.retryCalls) != 1 {
		t.Fatalf("ожидали одну публикацию retry, получили %d", len(pub.retryCalls))
	}
	if got := pub.retryCalls[0].GetAttempt(); got != 2 {
		t.Fatalf("attempt в retry-команде = %d, want 2", got)
	}
	if pub.retryCalls[0].GetStageExtension() == nil {
		t.Fatalf("retry-команда должна нести stage_extension при достаточных данных")
	}
	if len(pub.dlqCalls) != 0 || len(store.restoreCalls) != 0 {
		t.Fatalf("успешный retry не должен трогать DLQ/RestoreDeadline")
	}
}

// TestProcessEntryRetryWithoutExtensionDataGoesToDlqInstead — finding #1:
// без накопленных данных (сегодняшняя норма) republish гарантированно
// REJECTED downstream — processEntry обязан уйти в DLQ, а не публиковать
// заведомо неверную команду.
func TestProcessEntryRetryWithoutExtensionDataGoesToDlqInstead(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0, DeadlineUnixMs: now.UnixMilli()}
	state := sweep.ExecutionState{MessageID: "msg-1", StageExecutionID: "exec-1", StageName: "BILLING", Attempt: 1} // без ResolvedOperatorID/Category
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		states:     map[string]sweep.ExecutionState{"msg-1": state},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(pub.retryCalls) != 0 {
		t.Fatalf("не должны публиковать retry без stage_extension, получили %d", len(pub.retryCalls))
	}
	if len(pub.dlqCalls) != 1 {
		t.Fatalf("ожидали публикацию в DLQ вместо retry, получили %d", len(pub.dlqCalls))
	}
	if pub.dlqCalls[0].GetOriginalCommand().GetAttempt() != 1 {
		t.Fatalf("original_command.attempt должен быть исчерпанной попыткой (1), получили %d", pub.dlqCalls[0].GetOriginalCommand().GetAttempt())
	}
}

// TestProcessEntrySkipsWhenClaimLostToAnotherReplica — finding #3: прямая
// проверка на уровне processEntry, что проигранный клэйм не приводит ни к
// какой публикации.
func TestProcessEntrySkipsWhenClaimLostToAnotherReplica(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}
	store := &fakeStore{
		messageIDs:  map[string]string{"exec-1": "msg-1"},
		states:      map[string]sweep.ExecutionState{"msg-1": billingStateWithExtensionData(1)},
		claimResult: map[string]bool{"exec-1": false},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(pub.retryCalls) != 0 || len(pub.timeoutCalls) != 0 || len(pub.dlqCalls) != 0 {
		t.Fatalf("проигранный клэйм не должен приводить ни к какой публикации")
	}
	if len(store.restoreCalls) != 0 {
		t.Fatalf("проигранный клэйм не должен вызывать RestoreDeadline (мы и не забирали запись)")
	}
}

// TestProcessEntryRestoresDeadlineWhenPublishFails — finding #7: клэйм
// прошёл, публикация не удалась -> запись обязана вернуться в планирование.
func TestProcessEntryRestoresDeadlineWhenPublishFails(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 3, DeadlineUnixMs: 12345}
	// defaultRetryPolicy.MaxAttempts=3 для всех стадий (см. main.go) — Attempt=1
	// с достаточными данными для stage_extension даёт ActionRetry, publish_retry.
	state := billingStateWithExtensionData(1)
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		states:     map[string]sweep.ExecutionState{"msg-1": state},
	}
	pub := &fakePublisher{retryErr: errors.New("kafka недоступна")}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(store.restoreCalls) != 1 {
		t.Fatalf("ожидали ровно один RestoreDeadline после неудачной публикации, получили %d", len(store.restoreCalls))
	}
	if store.restoreCalls[0] != entry {
		t.Fatalf("RestoreDeadline должен получить тот же entry, получили %+v", store.restoreCalls[0])
	}
}

// TestProcessEntryDlqOriginalCommandUsesExhaustedAttempt — finding #2 на
// уровне processEntry (не только builders.go): DLQ-путь должен передавать
// attempt=state.Attempt, не +1.
func TestProcessEntryDlqOriginalCommandUsesExhaustedAttempt(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}
	state := billingStateWithExtensionData(3) // MaxAttempts=3 -> Exhausted -> ActionDlq
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		states:     map[string]sweep.ExecutionState{"msg-1": state},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(pub.dlqCalls) != 1 {
		t.Fatalf("ожидали одну публикацию в DLQ, получили %d", len(pub.dlqCalls))
	}
	rec := pub.dlqCalls[0]
	if rec.GetAttempt() != 3 {
		t.Fatalf("DlqRecord.attempt = %d, want 3", rec.GetAttempt())
	}
	if rec.GetOriginalCommand().GetAttempt() != 3 {
		t.Fatalf("original_command.attempt = %d, want 3 (не 4)", rec.GetOriginalCommand().GetAttempt())
	}
}

// TestProcessEntryResolveMessageIDFailureDefersInsteadOfBusyLoop — finding
// #8: раньше эта ветка просто `continue`, оставляя запись с истёкшим
// дедлайном — гарантированный тайт-луп на каждый тик.
func TestProcessEntryResolveMessageIDFailureDefersInsteadOfBusyLoop(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}
	store := &fakeStore{
		resolveErr: map[string]error{"exec-1": errors.New("stage_exec_index не найден")},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(store.claimCalls) != 0 {
		t.Fatalf("не должны клэймить, если не смогли даже резолвить message_id")
	}
	if len(store.deferCalls) != 1 {
		t.Fatalf("ожидали DeferDeadline вместо busy-loop, получили %d вызовов", len(store.deferCalls))
	}
	if want := now.Add(unresolvedBackoff); !store.deferCalls[0].until.Equal(want) {
		t.Fatalf("DeferDeadline.until = %v, want %v", store.deferCalls[0].until, want)
	}
}

// TestProcessEntryLoadExecutionStateFailureDefersInsteadOfBusyLoop —
// finding #8, вторая ветка отказа (exec:{message_id} не найден/протух).
func TestProcessEntryLoadExecutionStateFailureDefersInsteadOfBusyLoop(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}
	store := &fakeStore{
		messageIDs: map[string]string{"exec-1": "msg-1"},
		loadErr:    map[string]error{"msg-1": errors.New("exec:msg-1 не найден")},
	}
	pub := &fakePublisher{}

	processEntry(context.Background(), now, entry, store, pub, nil)

	if len(store.deferCalls) != 1 {
		t.Fatalf("ожидали DeferDeadline вместо busy-loop, получили %d вызовов", len(store.deferCalls))
	}
}

// TestProcessTickProcessesEveryEntryFromTickSweep — processTick должен
// пройтись по каждой записи, отданной TickSweep.
func TestProcessTickProcessesEveryEntryFromTickSweep(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{
		entries: []sweep.ExpiredEntry{
			{StageExecutionID: "exec-1", Bucket: 0},
			{StageExecutionID: "exec-2", Bucket: 1},
		},
		messageIDs: map[string]string{"exec-1": "msg-1", "exec-2": "msg-2"},
		states: map[string]sweep.ExecutionState{
			"msg-1": billingStateWithExtensionData(1),
			"msg-2": func() sweep.ExecutionState {
				s := billingStateWithExtensionData(1)
				s.MessageID, s.StageExecutionID = "msg-2", "exec-2"
				return s
			}(),
		},
	}
	pub := &fakePublisher{}

	processTick(context.Background(), now, store, pub, nil)

	if len(store.claimCalls) != 2 {
		t.Fatalf("ожидали 2 клэйма (по одному на запись), получили %d", len(store.claimCalls))
	}
	if len(pub.retryCalls) != 2 {
		t.Fatalf("ожидали 2 публикации retry, получили %d", len(pub.retryCalls))
	}
}

func TestProcessTickReturnsEarlyOnTickSweepError(t *testing.T) {
	store := &fakeStore{tickErr: errors.New("redis недоступен")}
	pub := &fakePublisher{}

	processTick(context.Background(), time.Now(), store, pub, nil)

	if len(store.claimCalls) != 0 || len(pub.retryCalls) != 0 {
		t.Fatalf("при ошибке tick_sweep ничего обрабатывать не должны")
	}
}
