// Package grpcserver реализует mpp.grpc.v1.OperatorSubmitService.Submit —
// handle_submit_command + enforce_tps + send_http_submit +
// handle_http_submit_response + publish_submit_accepted + reply_submit_result
// (service_internal_methods.md §1.3a). Instance-addressed, вызывается
// Delivery Service.
package grpcserver

import (
	"context"
	"time"

	eventsv1 "mpp/platformcontracts/events/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/operator-http-gateway/internal/core"
	"mpp/operator-http-gateway/internal/httpio"
	"mpp/operator-http-gateway/internal/kafkaio"
)

// EndpointResolver — резолв endpoint_url по route_id (из Runtime Redis
// registry в проде); интерфейс — чтобы тестировать без реального Redis.
type EndpointResolver interface {
	ResolveEndpoint(operatorID, routeID string) (string, error)
}

// EventPublisher — минимальный интерфейс от kafkaio.Publisher (позволяет
// фейковать в тестах без реального Kafka-брокера).
type EventPublisher interface {
	PublishSubmitAccepted(ctx context.Context, event *eventsv1.OperatorSubmitAccepted) error
}

type Server struct {
	grpcv1.UnimplementedOperatorSubmitServiceServer

	httpClient *httpio.Client
	tpsBucket  *core.TokenBucket
	publisher  EventPublisher
	endpoints  EndpointResolver
}

func New(httpClient *httpio.Client, tpsBucket *core.TokenBucket, publisher EventPublisher, endpoints EndpointResolver) *Server {
	return &Server{httpClient: httpClient, tpsBucket: tpsBucket, publisher: publisher, endpoints: endpoints}
}

func (s *Server) Submit(ctx context.Context, req *grpcv1.SubmitRequest) (*grpcv1.SubmitResponse, error) {
	if !s.tpsBucket.TryAcquire(time.Now()) {
		return &grpcv1.SubmitResponse{
			Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED,
			ReasonCode: "TPS_THROTTLED",
		}, nil
	}

	endpoint, err := s.endpoints.ResolveEndpoint(req.GetOperatorId(), req.GetRouteId())
	if err != nil {
		return &grpcv1.SubmitResponse{
			Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
			ReasonCode: "ROUTE_NOT_FOUND: " + err.Error(),
		}, nil
	}

	var smscMessageID string
	for _, segment := range req.GetSegments() {
		outcome, err := s.httpClient.SubmitSegment(ctx, endpoint, req.GetDestinationAddress(), segment.GetContent(), segment.GetEncoding(), req.GetQueueMsgId())
		if err != nil {
			return &grpcv1.SubmitResponse{
				Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
				ReasonCode: "HTTP_CLIENT_ERROR: " + err.Error(),
			}, nil
		}

		switch outcome.Status {
		case httpio.OutcomeAccepted:
			smscMessageID = outcome.SmscMessageID
		case httpio.OutcomeRejected:
			return &grpcv1.SubmitResponse{
				Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED,
				ReasonCode: outcome.ReasonCode,
			}, nil
		default:
			return &grpcv1.SubmitResponse{
				Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
				ReasonCode: outcome.ReasonCode,
			}, nil
		}
	}

	event := kafkaio.BuildSubmitAcceptedEvent(req.GetMessageId(), req.GetStageExecutionId(), req.GetOperatorId(), smscMessageID, 0, time.Now())
	if err := s.publisher.PublishSubmitAccepted(ctx, event); err != nil {
		// Отправлено оператору успешно, но публикация в Kafka не удалась —
		// это разрыв (оператор принял, downstream не узнает) не решён в
		// этом срезе, см. README "Что НЕ реализовано".
		return &grpcv1.SubmitResponse{
			Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
			ReasonCode: "KAFKA_PUBLISH_FAILED: " + err.Error(),
		}, nil
	}

	return &grpcv1.SubmitResponse{
		Status:        grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_ACCEPTED,
		SmscMessageId: smscMessageID,
	}, nil
}
