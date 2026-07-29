package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/backoffice-api/internal/auth"
	"mpp/backoffice-api/internal/kafkaio"
)

func parseTaskType(s string) (commonv1.CriticalCommandType, bool) {
	v, ok := commonv1.CriticalCommandType_value[s]
	if !ok || commonv1.CriticalCommandType(v) == commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_UNSPECIFIED {
		return 0, false
	}
	return commonv1.CriticalCommandType(v), true
}

type forceSchedulerCommandRequestBody struct {
	StageExecutionID string `json:"stage_execution_id"`
	TaskType         string `json:"task_type"`
	Reason           string `json:"reason"`
}

// handle_force_scheduler_command — service_internal_methods.md §7.3:
// HTTP-запрос от UI -> Kafka publish на scheduler.critical.commands.
// Backoffice API — единственный producer этого топика (аудируется через
// requested_by в самом событии, HLD §9.1).
func handleForceSchedulerCommand(publisher *kafkaio.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body forceSchedulerCommandRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.StageExecutionID == "" {
			http.Error(w, "требуется stage_execution_id", http.StatusBadRequest)
			return
		}
		taskType, ok := parseTaskType(body.TaskType)
		if !ok {
			http.Error(w, "task_type должен быть CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT или CRITICAL_COMMAND_TYPE_FORCE_RETRY", http.StatusBadRequest)
			return
		}

		if body.Reason == "" {
			http.Error(w, "reason обязателен для force scheduler command", http.StatusBadRequest)
			return
		}

		cmd := kafkaio.BuildCriticalCommand(body.StageExecutionID, taskType, claims.Subject, body.Reason, time.Now())
		if err := publisher.PublishCriticalCommand(r.Context(), cmd); err != nil {
			internalError(w, http.StatusBadGateway, "force_scheduler_command: публикация в Kafka не удалась", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(struct {
			Accepted bool `json:"accepted"`
		}{Accepted: true})
	}
}