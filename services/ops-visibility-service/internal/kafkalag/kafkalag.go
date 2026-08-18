// Package kafkalag — опрос consumer-group lag по ВСЕМ группам, реально
// присутствующим на брокерах KAFKA_BOOTSTRAP_SERVERS, без статического
// списка имён групп в коде (Фаза 8 плана: "новый потребляющий сервис не
// должен требовать правки этого сервиса").
//
// kadm.Client.Lag (github.com/twmb/franz-go/pkg/kadm@v1.18.0/groups.go)
// внутри сам делает то, что нужно: если вызвать его без имён групп,
// DescribeGroups сначала выполняет ListGroups и описывает ВСЕ найденные
// classic-группы (см. комментарий "describes either all classic groups
// specified, or all classic groups in the cluster if none are specified" и
// комментарий в Lag: "If the input set of groups is empty, DescribeGroups
// returns all groups. We add to `set` here so that the Lag function itself
// can calculate lag for all groups."). Это ровно то поведение, которое
// нужно здесь — отдельный ListGroups-проход не нужен.
package kafkalag

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionLag — lag одной топик-партиции внутри группы.
type PartitionLag struct {
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	CommitOffset int64  `json:"commit_offset"`
	EndOffset    int64  `json:"end_offset"`
	Lag          int64  `json:"lag"`
	Error        string `json:"error,omitempty"`
}

// ConsumerGroupLag — lag одной consumer group целиком. Имя намеренно не
// "GroupLag" — так уже называется тип в самом kadm (map[string]map[int32]
// GroupMemberLag), совпадение имён в комментариях/логах было бы путающим.
type ConsumerGroupLag struct {
	Group      string         `json:"group"`
	State      string         `json:"state"`
	TotalLag   int64          `json:"total_lag"`
	Partitions []PartitionLag `json:"partitions"`
	Error      string         `json:"error,omitempty"`
}

// Snapshot — результат одного цикла опроса, ровно то, что пишется в Redis
// (см. internal/store) под ключом ops:kafka-lag:snapshot.
type Snapshot struct {
	GeneratedAt      time.Time          `json:"generated_at"`
	BootstrapServers []string           `json:"bootstrap_servers"`
	Groups           []ConsumerGroupLag `json:"groups"`
	// Error — ошибка ВСЕГО цикла опроса (например, ни один брокер не
	// достижим). Пустая строка, если цикл прошёл (отдельные группы всё
	// равно могут нести собственную Error — например, у группы отвалился
	// координатор, пока остальные опросились нормально).
	Error string `json:"error,omitempty"`
}

// Client — тонкая обёртка над kadm.Client (который сам по себе тонкая
// обёртка над *kgo.Client — см. заголовок kadm.go). Отдельный *kgo.Client
// здесь не потребляет ни один топик (ConsumeTopics не вызывается) — он
// используется только как транспорт для admin-запросов (ListGroups/
// DescribeGroups/ListOffsets/OffsetFetch), поэтому ConsumerGroup здесь не
// нужен и не задаётся.
type Client struct {
	kgoClient *kgo.Client
	admin     *kadm.Client
}

func NewClient(brokers []string) (*Client, error) {
	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Client{kgoClient: kgoClient, admin: kadm.NewClient(kgoClient)}, nil
}

func (c *Client) Close() { c.kgoClient.Close() }

// Ping — /readyz dependency check.
func (c *Client) Ping(ctx context.Context) error { return c.kgoClient.Ping(ctx) }

// FetchAll опрашивает lag для всех групп на брокерах brokers. brokers
// передаётся отдельно от Client только затем, чтобы попасть в
// Snapshot.BootstrapServers (для отображения в /snapshot, что именно
// опрашивалось) — сам admin-клиент уже сконфигурирован на них в NewClient.
func (c *Client) FetchAll(ctx context.Context, brokers []string) Snapshot {
	lags, err := c.admin.Lag(ctx)
	return BuildSnapshot(brokers, lags, err)
}

// BuildSnapshot — чистая функция трансформации kadm.DescribedGroupLags в
// наш JSON-контракт, вынесена из FetchAll специально ради юнит-тестируемости
// без реального брокера (тот же принцип, что processRecords в
// config-cache-projector/internal/kafkaio/consumer.go).
func BuildSnapshot(brokers []string, lags kadm.DescribedGroupLags, fetchErr error) Snapshot {
	snap := Snapshot{
		GeneratedAt:      time.Now().UTC(),
		BootstrapServers: brokers,
	}
	if fetchErr != nil {
		snap.Error = fetchErr.Error()
		return snap
	}

	for _, gl := range lags.Sorted() {
		g := ConsumerGroupLag{
			Group:    gl.Group,
			State:    gl.State,
			TotalLag: gl.Lag.Total(),
		}
		if gerr := gl.Error(); gerr != nil {
			g.Error = gerr.Error()
		}
		for _, m := range gl.Lag.Sorted() {
			p := PartitionLag{
				Topic:        m.Topic,
				Partition:    m.Partition,
				CommitOffset: m.Commit.At,
				EndOffset:    m.End.Offset,
				Lag:          m.Lag,
			}
			if m.Err != nil {
				p.Error = m.Err.Error()
			}
			g.Partitions = append(g.Partitions, p)
		}
		snap.Groups = append(snap.Groups, g)
	}
	sort.Slice(snap.Groups, func(i, j int) bool { return snap.Groups[i].Group < snap.Groups[j].Group })
	return snap
}
