package hysteresis

// Scope — область действия записи execution.control (HLD §8.1,
// mpp.common.v1.ExecutionControlScope в platform-contracts/common/enums.proto).
type Scope int

const (
	ScopeGlobal Scope = iota
	ScopeStage
	ScopePartner
	ScopePartnerStage
	ScopeOperatorRoute
)

// Record — одна применимая запись execution.control по конкретному scope
// (compose_effective_rate, service_internal_methods.md §3.1).
type Record struct {
	Scope          Scope
	ScopeID        string
	State          State
	AdmissionRate  float64
	DispatchRate   float64
}

// MessageContext — координаты конкретного сообщения/потока, для которых
// нужно вычислить эффективную ставку: partner_id, stage_name,
// "partner_id:stage_name" и operator_route_id.
type MessageContext struct {
	PartnerID       string
	StageName       string
	OperatorRouteID string
}

func (r Record) applies(ctx MessageContext) bool {
	switch r.Scope {
	case ScopeGlobal:
		return true
	case ScopeStage:
		return r.ScopeID == ctx.StageName
	case ScopePartner:
		return r.ScopeID == ctx.PartnerID
	case ScopePartnerStage:
		return r.ScopeID == ctx.PartnerID+":"+ctx.StageName
	case ScopeOperatorRoute:
		return r.ScopeID == ctx.OperatorRouteID
	default:
		return false
	}
}

// EffectiveRate — compose_effective_rate: среди всех записей, применимых к
// данному контексту сообщения, effective_rate = min(admission_rate),
// а состояние — наиболее строгое (PAUSED > DEGRADED > ACTIVE), поскольку
// одного PAUSED-scope в цепочке достаточно, чтобы остановить admission
// целиком (HLD §8.1: "Кто читает execution.control напрямую" — контроль
// применяется по всей scope-иерархии одновременно, не только по самому
// специфичному scope).
func EffectiveRate(records []Record, ctx MessageContext) (rate float64, state State) {
	rate = 1.0
	state = StateActive
	matched := false

	for _, r := range records {
		if !r.applies(ctx) {
			continue
		}
		matched = true
		if r.AdmissionRate < rate {
			rate = r.AdmissionRate
		}
		if scopeOrder[r.State] > scopeOrder[state] {
			state = r.State
		}
	}

	if !matched {
		return 1.0, StateActive
	}
	return rate, state
}
