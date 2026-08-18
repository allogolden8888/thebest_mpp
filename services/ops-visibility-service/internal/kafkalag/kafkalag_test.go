package kafkalag

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- Чистые тесты BuildSnapshot, без сети (аналог processRecords в
// config-cache-projector/internal/kafkaio: трансформация вынесена из
// FetchAll именно ради этого). ---

func TestBuildSnapshotEmptyGroupsProducesEmptySnapshot(t *testing.T) {
	snap := BuildSnapshot([]string{"broker:9092"}, kadm.DescribedGroupLags{}, nil)

	if snap.Error != "" {
		t.Errorf("Error = %q, want empty", snap.Error)
	}
	if len(snap.Groups) != 0 {
		t.Errorf("Groups = %v, want empty", snap.Groups)
	}
	if len(snap.BootstrapServers) != 1 || snap.BootstrapServers[0] != "broker:9092" {
		t.Errorf("BootstrapServers = %v", snap.BootstrapServers)
	}
}

func TestBuildSnapshotTopLevelFetchErrorSurfacesAsSnapshotError(t *testing.T) {
	fetchErr := errors.New("boom: no brokers reachable")
	snap := BuildSnapshot([]string{"broker:9092"}, nil, fetchErr)

	if snap.Error != fetchErr.Error() {
		t.Errorf("Error = %q, want %q", snap.Error, fetchErr.Error())
	}
	if len(snap.Groups) != 0 {
		t.Errorf("Groups должен быть пуст при top-level ошибке, got %v", snap.Groups)
	}
}

func TestBuildSnapshotComputesTotalLagAndPerPartitionFields(t *testing.T) {
	lags := kadm.DescribedGroupLags{
		"my-consumer-group": kadm.DescribedGroupLag{
			Group: "my-consumer-group",
			State: "Stable",
			Lag: kadm.GroupLag{
				"my-topic": {
					0: kadm.GroupMemberLag{
						Topic: "my-topic", Partition: 0,
						Commit: kadm.Offset{Topic: "my-topic", Partition: 0, At: 90},
						End:    kadm.ListedOffset{Topic: "my-topic", Partition: 0, Offset: 100},
						Lag:    10,
					},
					1: kadm.GroupMemberLag{
						Topic: "my-topic", Partition: 1,
						Commit: kadm.Offset{Topic: "my-topic", Partition: 1, At: 50},
						End:    kadm.ListedOffset{Topic: "my-topic", Partition: 1, Offset: 55},
						Lag:    5,
					},
				},
			},
		},
	}

	snap := BuildSnapshot([]string{"broker:9092"}, lags, nil)

	if len(snap.Groups) != 1 {
		t.Fatalf("ожидали 1 группу, получили %d", len(snap.Groups))
	}
	g := snap.Groups[0]
	if g.Group != "my-consumer-group" {
		t.Errorf("Group = %q", g.Group)
	}
	if g.State != "Stable" {
		t.Errorf("State = %q, want Stable", g.State)
	}
	if g.TotalLag != 15 {
		t.Errorf("TotalLag = %d, want 15", g.TotalLag)
	}
	if len(g.Partitions) != 2 {
		t.Fatalf("ожидали 2 партиции, получили %d", len(g.Partitions))
	}
	for _, p := range g.Partitions {
		if p.Topic != "my-topic" {
			t.Errorf("Topic = %q", p.Topic)
		}
		switch p.Partition {
		case 0:
			if p.CommitOffset != 90 || p.EndOffset != 100 || p.Lag != 10 {
				t.Errorf("partition 0: %+v", p)
			}
		case 1:
			if p.CommitOffset != 50 || p.EndOffset != 55 || p.Lag != 5 {
				t.Errorf("partition 1: %+v", p)
			}
		default:
			t.Errorf("неожиданная партиция %d", p.Partition)
		}
	}
}

func TestBuildSnapshotGroupLevelErrorSurfacesOnGroupNotWholeSnapshot(t *testing.T) {
	groupErr := errors.New("coordinator not available")
	lags := kadm.DescribedGroupLags{
		"broken-group": kadm.DescribedGroupLag{
			Group:       "broken-group",
			DescribeErr: groupErr,
		},
		"healthy-group": kadm.DescribedGroupLag{
			Group: "healthy-group",
			State: "Empty",
			Lag:   kadm.GroupLag{},
		},
	}

	snap := BuildSnapshot([]string{"broker:9092"}, lags, nil)

	if snap.Error != "" {
		t.Errorf("Snapshot.Error должен остаться пуст — ошибка только у одной группы, got %q", snap.Error)
	}
	if len(snap.Groups) != 2 {
		t.Fatalf("ожидали 2 группы, получили %d", len(snap.Groups))
	}
	var broken, healthy *ConsumerGroupLag
	for i := range snap.Groups {
		switch snap.Groups[i].Group {
		case "broken-group":
			broken = &snap.Groups[i]
		case "healthy-group":
			healthy = &snap.Groups[i]
		}
	}
	if broken == nil || broken.Error != groupErr.Error() {
		t.Errorf("broken-group.Error = %+v, want %q", broken, groupErr.Error())
	}
	if healthy == nil || healthy.Error != "" {
		t.Errorf("healthy-group.Error должен быть пуст, got %+v", healthy)
	}
}

func TestBuildSnapshotSortsGroupsByName(t *testing.T) {
	lags := kadm.DescribedGroupLags{
		"zeta":  kadm.DescribedGroupLag{Group: "zeta"},
		"alpha": kadm.DescribedGroupLag{Group: "alpha"},
		"mike":  kadm.DescribedGroupLag{Group: "mike"},
	}

	snap := BuildSnapshot([]string{"broker:9092"}, lags, nil)

	want := []string{"alpha", "mike", "zeta"}
	if len(snap.Groups) != len(want) {
		t.Fatalf("ожидали %d групп, получили %d", len(want), len(snap.Groups))
	}
	for i, w := range want {
		if snap.Groups[i].Group != w {
			t.Errorf("snap.Groups[%d].Group = %q, want %q", i, snap.Groups[i].Group, w)
		}
	}
}

// --- Реальный round-trip против локального Kafka (docker-compose,
// infra/docker/docker-compose.yml, EXTERNAL listener localhost:9094) —
// t.Skip, если брокер недостижим, тот же принцип, что и везде в этой
// сессии: не подделываем round-trip, которого нет, но и не пропускаем его,
// если окружение реально его поддерживает. ---

func realKafkaBrokers(t *testing.T) []string {
	t.Helper()
	addr := "localhost:9094"
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Skipf("локальный Kafka (%s) недостижим, пропускаю integration-тест: %v", addr, err)
	}
	_ = conn.Close()
	return []string{addr}
}

func TestFetchAllAgainstRealKafkaReportsRealLag(t *testing.T) {
	brokers := realKafkaBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kgoAdmin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer kgoAdmin.Close()
	admin := kadm.NewClient(kgoAdmin)

	topic := fmt.Sprintf("ops-visibility-test-%d", time.Now().UnixNano())
	group := fmt.Sprintf("ops-visibility-test-group-%d", time.Now().UnixNano())

	if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("CreateTopic(%s): %v", topic, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		admin.DeleteGroup(cleanupCtx, group)
		admin.DeleteTopics(cleanupCtx, topic)
	})

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("kgo.NewClient (producer): %v", err)
	}
	defer producer.Close()

	const numRecords = 10
	for i := 0; i < numRecords; i++ {
		res := producer.ProduceSync(ctx, kgo.StringRecord(fmt.Sprintf("record-%d", i)))
		if err := res.FirstErr(); err != nil {
			t.Fatalf("ProduceSync record %d: %v", i, err)
		}
	}

	// Коммитим оффсет вручную на известное значение (K=6 из 10), чтобы
	// получить детерминированный ожидаемый lag=4, без необходимости
	// реально гонять consumer group protocol (JoinGroup/SyncGroup) в
	// тесте — kadm.CommitOffsets регистрирует группу в
	// __consumer_offsets, ровно то, что нужно, чтобы группа стала видна
	// DescribeGroups/Lag (в состоянии Empty — нет активных участников, но
	// коммиты есть, тот же кейс, что описан в doc comment
	// GroupMemberLag в kadm: "If the group is Empty, lag is calculated
	// for all partitions in a topic, but the member is nil").
	const committedOffset = 6
	var offsets kadm.Offsets
	offsets.AddOffset(topic, 0, committedOffset, -1)
	if _, err := admin.CommitOffsets(ctx, group, offsets); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	client, err := NewClient(brokers)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	snap := client.FetchAll(ctx, brokers)
	if snap.Error != "" {
		t.Fatalf("FetchAll вернул ошибку всего цикла: %s", snap.Error)
	}

	var found *ConsumerGroupLag
	for i := range snap.Groups {
		if snap.Groups[i].Group == group {
			found = &snap.Groups[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("группа %q не найдена в снапшоте среди %d групп", group, len(snap.Groups))
	}
	if found.Error != "" {
		t.Fatalf("группа %q несёт ошибку: %s", group, found.Error)
	}
	wantLag := int64(numRecords - committedOffset)
	if found.TotalLag != wantLag {
		t.Errorf("TotalLag = %d, want %d", found.TotalLag, wantLag)
	}
	if len(found.Partitions) != 1 {
		t.Fatalf("ожидали 1 партицию, получили %d", len(found.Partitions))
	}
	p := found.Partitions[0]
	if p.Topic != topic {
		t.Errorf("Partition.Topic = %q, want %q", p.Topic, topic)
	}
	if p.CommitOffset != committedOffset {
		t.Errorf("CommitOffset = %d, want %d", p.CommitOffset, committedOffset)
	}
	if p.EndOffset != numRecords {
		t.Errorf("EndOffset = %d, want %d", p.EndOffset, numRecords)
	}
	if p.Lag != wantLag {
		t.Errorf("Lag = %d, want %d", p.Lag, wantLag)
	}
}

func TestPingAgainstRealKafka(t *testing.T) {
	brokers := realKafkaBrokers(t)
	client, err := NewClient(brokers)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping failed against reachable broker: %v", err)
	}
}
