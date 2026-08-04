// Package kafkaio — publish_submit_accepted + normalize_and_publish_dlr
// (service_internal_methods.md §1.3a) — те же топики/ключевание, что
// operator-smpp-session-manager (одна и та же логическая capability,
// разный протокол, service_io_contracts.md "Ключевание": operator.* по
// operator_id).
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

const (
	SubmitAcceptedTopic = "operator.submit.accepted"
	DlrTopic            = "operator.dlr"
)

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string) (*Publisher, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) Close() { p.client.Close() }

func (p *Publisher) PublishSubmitAccepted(ctx context.Context, event *eventsv1.OperatorSubmitAccepted) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("proto.Marshal(OperatorSubmitAccepted): %w", err)
	}
	rec := &kgo.Record{Topic: SubmitAcceptedTopic, Key: []byte(event.GetOperatorId()), Value: payload}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *Publisher) PublishDlr(ctx context.Context, event *eventsv1.OperatorDlr) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("proto.Marshal(OperatorDlr): %w", err)
	}
	rec := &kgo.Record{Topic: DlrTopic, Key: []byte(event.GetOperatorId()), Value: payload}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}