// GET /v1/messages/{message_id}/pdu-log — пер-PDU лог сообщения
// (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40): каждый реальный SMPP PDU
// (submit_sm/submit_sm_resp для A2P, deliver_sm/deliver_sm_resp для DLR),
// а не агрегат по стадии, как /timeline (timeline.go), и не сегмент
// dlr.dlr_correlation, как /operator-events (billing.go). Источник —
// analytics.operator_pdu_log (ClickHouse), которую пишет pdu-log-writer
// из topic operator.pdu.log.
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"mpp/backoffice-api/internal/store"
)

type pduLogEntryResponse struct {
	Direction        string `json:"direction"`
	PduType          string `json:"pdu_type"`
	SequenceNumber   int32  `json:"sequence_number"`
	Protocol         string `json:"protocol"`
	MessageID        string `json:"message_id"`
	StageExecutionID string `json:"stage_execution_id"`
	SmscMessageID    string `json:"smsc_message_id"`
	SegmentID        int32  `json:"segment_id"`
	Status           string `json:"status"`
	OccurredAt       string `json:"occurred_at"`
}

// handleMessagePduLog — DLR-направление PDU (deliver_sm/_resp) не несёт
// message_id (архитектурный барьер, см. OperatorPduLog doc-комментарий в
// platform-contracts/events/operator_events.proto), только
// smsc_message_id. Поэтому сперва резолвим smsc_message_id этого
// сообщения через dlr.dlr_correlation (тот же Postgres-запрос, что уже
// использует /operator-events — OperatorEventsByMessage), затем читаем
// ClickHouse по (message_id = ? OR smsc_message_id IN (...)), объединяя
// A2P- и DLR-направления в одну хронологическую ленту.
func handleMessagePduLog(ch *store.ClickHouse, pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		messageID := chi.URLParam(r, "message_id")
		if ch == nil {
			http.Error(w, "аналитика недоступна: ClickHouse не сконфигурирован", http.StatusServiceUnavailable)
			return
		}

		var smscMessageIDs []string
		if pg != nil {
			segments, err := pg.OperatorEventsByMessage(r.Context(), messageID)
			if err != nil {
				internalError(w, http.StatusInternalServerError, "message_pdu_log: ошибка чтения dlr.dlr_correlation из PostgreSQL", err)
				return
			}
			seen := make(map[string]struct{}, len(segments))
			for _, s := range segments {
				if s.SmscMessageID == "" {
					continue
				}
				if _, ok := seen[s.SmscMessageID]; ok {
					continue
				}
				seen[s.SmscMessageID] = struct{}{}
				smscMessageIDs = append(smscMessageIDs, s.SmscMessageID)
			}
		}

		entries, err := ch.MessagePduLog(r.Context(), messageID, smscMessageIDs)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "message_pdu_log: ошибка чтения из ClickHouse", err)
			return
		}

		pdus := make([]pduLogEntryResponse, 0, len(entries))
		for _, e := range entries {
			pdus = append(pdus, pduLogEntryResponse{
				Direction:        e.Direction,
				PduType:          e.PduType,
				SequenceNumber:   e.SequenceNumber,
				Protocol:         e.Protocol,
				MessageID:        e.MessageID,
				StageExecutionID: e.StageExecutionID,
				SmscMessageID:    e.SmscMessageID,
				SegmentID:        e.SegmentID,
				Status:           e.Status,
				OccurredAt:       e.OccurredAt.Format(timeFormat),
			})
		}

		writeJSON(w, struct {
			MessageID string                `json:"message_id"`
			Pdus      []pduLogEntryResponse `json:"pdus"`
		}{MessageID: messageID, Pdus: pdus})
	}
}
