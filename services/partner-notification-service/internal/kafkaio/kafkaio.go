// Package kafkaio — оркестрация всего service_internal_methods.md §7.1:
// on_lifecycle_event/on notification.retry -> resolve_delivery_channel ->
// lookup_gateway_instance (только SMPP) -> send_deliver_sm/send_rest_callback
// -> handle_delivery_failure/evaluate_ttl -> publish scheduler.background.commands.
package kafkaio

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/partner-notification-service/internal/config"
	"mpp/partner-notification-service/internal/msgctx"
	"mpp/partner-notification-service/internal/notify"
	"mpp/partner-notification-service/internal/pending"
	"mpp/partner-notification-service/internal/registry"
	"mpp/partner-notification-service/internal/schedule"
)

const (
	TopicLifecycle           = "message.lifecycle"
	TopicNotificationRetry   = "notification.retry"
	TopicSchedulerBackground = "scheduler.background.commands"
)

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(TopicLifecycle, TopicNotificationRetry),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

func (c *Consumer) PollOnce(ctx context.Context, onRecord func(*kgo.Record), errHandler func(error)) {
	fetches := c.client.PollFetches(ctx)
	fetches.EachError(func(_ string, _ int32, err error) {
		if errHandler != nil {
			errHandler(fmt.Errorf("fetch error: %w", err))
		}
	})
	fetches.EachRecord(onRecord)
}

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient (producer): %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Close() { p.client.Close() }

func (p *Producer) PublishRetryTask(ctx context.Context, task *eventsv1.SchedulerBackgroundTask) error {
	payload, err := proto.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal SchedulerBackgroundTask: %w", err)
	}
	record := &kgo.Record{Topic: TopicSchedulerBackground, Key: []byte(task.GetSourceEventId()), Value: payload}
	result := p.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", TopicSchedulerBackground, err)
	}
	return nil
}

type Deps struct {
	Snapshot         config.Snapshot
	MsgCtxStore      *msgctx.Store
	RegistryStore    *registry.Store
	PendingStore     *pending.Store
	SmppClient       *notify.SmppClient
	RestClient       *notify.RestClient
	Producer         *Producer
	NotificationTTL  time.Duration
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
}

// HandleRecord — вся оркестрация для одной Kafka-записи. Возвращает error
// только для инфраструктурных сбоев, которые должны заблокировать commit
// оффсета (напр. не удалось опубликовать retry-задачу) — реальная неудача
// доставки партнёру (gRPC/HTTP) НЕ считается processing-ошибкой: единственный
// retry-механизм в этом сервисе — через Scheduler
// ("второго, самостоятельного механизма retry внутри сервиса нет",
// services_specifictaion.md §8.1), не редоставка Kafka.
func HandleRecord(ctx context.Context, deps Deps, record *kgo.Record) error {
	fromRetry := record.Topic == TopicNotificationRetry

	var lifecycleEvent *eventsv1.MessageLifecycleEvent
	var eventID string
	var attempt int32 = 1

	if fromRetry {
		var task eventsv1.SchedulerBackgroundTask
		if err := proto.Unmarshal(record.Value, &task); err != nil {
			log.Printf("не удалось декодировать SchedulerBackgroundTask: %v", err)
			return nil
		}
		eventID = task.GetSourceEventId()
		attempt = task.GetAttempt()
		cached, found, err := deps.PendingStore.Get(ctx, eventID)
		if err != nil {
			return err
		}
		if !found {
			log.Printf("pending MessageLifecycleEvent для event_id=%s не найден в кэше, retry невозможен", eventID)
			return nil
		}
		lifecycleEvent = cached
	} else {
		var event eventsv1.MessageLifecycleEvent
		if err := proto.Unmarshal(record.Value, &event); err != nil {
			log.Printf("не удалось декодировать MessageLifecycleEvent: %v", err)
			return nil
		}
		lifecycleEvent = &event
		eventID = event.GetEventId()
	}

	partnerID, found, err := deps.MsgCtxStore.PartnerID(ctx, lifecycleEvent.GetMessageId())
	if err != nil {
		return err
	}
	if !found {
		log.Printf("msgctx не найден для message_id=%s, partner_id неизвестен — уведомление невозможно", lifecycleEvent.GetMessageId())
		if fromRetry {
			return deps.PendingStore.Delete(ctx, eventID)
		}
		return nil
	}

	_, app, found := deps.Snapshot.FirstApplication(partnerID)
	if !found {
		log.Printf("партнёр %s не найден в снапшоте конфигурации", partnerID)
		return nil
	}

	channel, err := config.ResolveDeliveryChannel(app)
	if err != nil {
		log.Printf("resolve_delivery_channel: %v", err)
		return nil
	}

	delivered := attemptDelivery(ctx, deps, channel, partnerID, app, lifecycleEvent)

	if delivered {
		if fromRetry {
			return deps.PendingStore.Delete(ctx, eventID)
		}
		return nil
	}

	decision := schedule.EvaluateTTL(lifecycleEvent.GetOccurredAt().AsTime(), deps.NotificationTTL, time.Now())
	if decision == schedule.DecisionExpire {
		log.Printf("notification TTL истёк для message_id=%s, push прекращён без побочных эффектов", lifecycleEvent.GetMessageId())
		if fromRetry {
			return deps.PendingStore.Delete(ctx, eventID)
		}
		return nil
	}

	if !fromRetry {
		if err := deps.PendingStore.Save(ctx, eventID, lifecycleEvent, deps.NotificationTTL); err != nil {
			return err
		}
	}
	delay := schedule.NextRetryDelay(attempt, deps.RetryBackoffBase, deps.RetryBackoffMax)
	task := schedule.BuildRetryTask(eventID, attempt, time.Now(), time.Now().Add(delay))
	return deps.Producer.PublishRetryTask(ctx, task)
}

func attemptDelivery(ctx context.Context, deps Deps, channel config.Channel, partnerID string, app config.Application, event *eventsv1.MessageLifecycleEvent) bool {
	switch channel {
	case config.ChannelSMPP:
		endpoint, found, err := deps.RegistryStore.Lookup(ctx, partnerID, app.ApplicationID)
		if err != nil {
			log.Printf("registry lookup: %v", err)
			return false
		}
		if !found {
			log.Printf("нет активной SMPP-сессии для partner_id=%s system_id=%s", partnerID, app.ApplicationID)
			return false
		}
		outcome, err := deps.SmppClient.DeliverSm(ctx, endpoint, event.GetMessageId(), partnerID, app.ApplicationID, statusText(event), nil)
		if err != nil {
			log.Printf("DeliverSm: %v", err)
		}
		return outcome == notify.OutcomeDelivered
	case config.ChannelREST:
		outcome, err := deps.RestClient.SendCallback(ctx, app.NotificationCallbackURL, event)
		if err != nil {
			log.Printf("SendCallback: %v", err)
		}
		return outcome == notify.OutcomeDelivered
	default:
		return false
	}
}

func statusText(event *eventsv1.MessageLifecycleEvent) string {
	return event.GetStatus().String()
}
