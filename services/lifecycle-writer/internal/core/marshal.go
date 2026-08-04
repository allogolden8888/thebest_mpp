package core

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// marshalOriginalCommand — dlq_record.original_command (BYTEA) хранит
// serialized StageExecuteCommand целиком (migrations/V006 комментарий),
// не сериализацию всего DlqRecord.
func marshalOriginalCommand(rec *eventsv1.DlqRecord) ([]byte, error) {
	cmd := rec.GetOriginalCommand()
	if cmd == nil {
		return nil, fmt.Errorf("DlqRecord без original_command (stage_execution_id=%s)", rec.GetStageExecutionId())
	}
	return proto.Marshal(cmd)
}