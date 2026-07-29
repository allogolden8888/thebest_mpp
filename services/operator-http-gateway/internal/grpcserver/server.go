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

// idempotencyTTL — сколько держим запись о частично отправленном
// сообщении, ожидая ретрая с тем же message_id (см. idempotency.go).
const idempotencyTTL = 10 * time.Minute

type Server struct {
	grpcv1.UnimplementedOperatorSubmitServiceServer

	httpClient  *httpio.Client
	tpsBucket   *core.TokenBucket
	publisher   EventPublisher
	endpoints   EndpointResolver
	idempotency *segmentIdempotency
}

func New(httpClient *httpio.Client, tpsBucket *core.TokenBucket, publisher EventPublisher, endpoints EndpointResolver) *Server {
	return &Server{
		httpClient:  httpClient,
		tpsBucket:   tpsBucket,
		publisher:   publisher,
		endpoints:   endpoints,
		idempotency: newSegmentIdempotency(idempotencyTTL),
	}
}

// Submit — CODE_REVIEW.md finding #3/#4/#7 (все три — раньше объединённый
// цикл по сегментам с публикацией ОДНОГО события после всех сегментов):
//   - #3 (HIGH): segment_id в публикуемом событии был захардкожен в 0, а
//     smsc_message_id перезаписывался на каждой итерации — для сообщения
//     из 3 сегментов данные сегментов 0 и 1 терялись безвозвратно.
//     Исправлено: событие публикуется ПОСЛЕ КАЖДОГО принятого сегмента, с
//     его настоящим segment_id и smsc_message_id.
//   - #4 (HIGH): при частичном сбое (сегмент 1 принят — уже необратимо
//     ушёл абоненту, — сегмент 2 отклонён) весь Submit возвращал ошибку;
//     ретрай всего запроса заново отправлял уже принятый сегмент 1
//     физически повторно. Исправлено: s.idempotency запоминает принятые
//     сегменты по message_id, повторный Submit с тем же message_id
//     пропускает уже принятые сегменты, не отправляя их оператору снова.
//   - #7 (MEDIUM): TPS проверялся один раз на весь Submit, а не на каждый
//     реальный исходящий HTTP-запрос — сообщение из N сегментов тратило 1
//     токен, но генерировало N реальных запросов оператору, до Nx
//     превышая настроенный лимит. Исправлено: TryAcquire — внутри цикла,
//     непосредственно перед реальным HTTP-вызовом (уже принятые ранее
//     сегменты токен не тратят, т.к. не отправляются повторно).
func (s *Server) Submit(ctx context.Context, req *grpcv1.SubmitRequest) (*grpcv1.SubmitResponse, error) {
	endpoint, err := s.endpoints.ResolveEndpoint(req.GetOperatorId(), req.GetRouteId())
	if err != nil {
		return &grpcv1.SubmitResponse{
			Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
			ReasonCode: "ROUTE_NOT_FOUND: " + err.Error(),
		}, nil
	}

	var lastSmscMessageID string
	for _, segment := range req.GetSegments() {
		segmentID := segment.GetSegmentId()

		if cachedSmscID, ok := s.idempotency.get(req.GetMessageId(), segmentID); ok {
			lastSmscMessageID = cachedSmscID
			continue
		}

		if !s.tpsBucket.TryAcquire(time.Now()) {
			return &grpcv1.SubmitResponse{
				Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED,
				ReasonCode: "TPS_THROTTLED",
			}, nil
		}

		outcome, err := s.httpClient.SubmitSegment(ctx, endpoint, req.GetDestinationAddress(), segment.GetContent(), segment.GetEncoding(), req.GetQueueMsgId())
		if err != nil {
			return &grpcv1.SubmitResponse{
				Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
				ReasonCode: "HTTP_CLIENT_ERROR: " + err.Error(),
			}, nil
		}

		switch outcome.Status {
		case httpio.OutcomeAccepted:
			lastSmscMessageID = outcome.SmscMessageID
			event := kafkaio.BuildSubmitAcceptedEvent(req.GetMessageId(), req.GetStageExecutionId(), req.GetOperatorId(), outcome.SmscMessageID, segmentID, time.Now())
			if err := s.publisher.PublishSubmitAccepted(ctx, event); err != nil {
				// Оператор уже принял сегмент необратимо — фиксируем ДО
				// возврата ошибки, чтобы ретрай не отправил его физически
				// повторно; сам Kafka publish безопасно повторить позже
				// (тот же смысл, что и остальной сессии — см. README "Что
				// НЕ реализовано" про разрыв между приёмом оператором и
				// публикацией).
				s.idempotency.record(req.GetMessageId(), segmentID, outcome.SmscMessageID)
				return &grpcv1.SubmitResponse{
					Status:     grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS,
					ReasonCode: "KAFKA_PUBLISH_FAILED: " + err.Error(),
				}, nil
			}
			s.idempotency.record(req.GetMessageId(), segmentID, outcome.SmscMessageID)
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

	s.idempotency.forget(req.GetMessageId())
	return &grpcv1.SubmitResponse{
		Status:        grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_ACCEPTED,
		SmscMessageId: lastSmscMessageID,
	}, nil
}
