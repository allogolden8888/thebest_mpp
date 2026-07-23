package hysteresis

import "testing"

func TestEffectiveRateNoRecordsDefaultsToFullActive(t *testing.T) {
	rate, state := EffectiveRate(nil, MessageContext{PartnerID: "acme", StageName: "billing"})
	if rate != 1.0 || state != StateActive {
		t.Fatalf("ожидали (1.0, ACTIVE) без записей, получили (%v, %v)", rate, state)
	}
}

func TestEffectiveRateTakesMinAcrossScopeHierarchy(t *testing.T) {
	records := []Record{
		{Scope: ScopeGlobal, ScopeID: "", State: StateActive, AdmissionRate: 1.0},
		{Scope: ScopePartner, ScopeID: "acme", State: StateDegraded, AdmissionRate: 0.5},
		{Scope: ScopePartnerStage, ScopeID: "acme:billing", State: StateActive, AdmissionRate: 0.7},
		{Scope: ScopeStage, ScopeID: "routing", State: StatePaused, AdmissionRate: 0.0}, // другой stage, не должен влиять
	}
	rate, state := EffectiveRate(records, MessageContext{PartnerID: "acme", StageName: "billing"})
	if rate != 0.5 {
		t.Fatalf("ожидали min(1.0, 0.5, 0.7) = 0.5, получили %v", rate)
	}
	if state != StateDegraded {
		t.Fatalf("ожидали наиболее строгое состояние из применимых (DEGRADED), получили %v", state)
	}
}

func TestEffectiveRatePausedScopeWinsOverLessStrict(t *testing.T) {
	records := []Record{
		{Scope: ScopeGlobal, ScopeID: "", State: StateActive, AdmissionRate: 1.0},
		{Scope: ScopePartnerStage, ScopeID: "acme:billing", State: StatePaused, AdmissionRate: 0.0, DispatchRate: 0.0},
	}
	rate, state := EffectiveRate(records, MessageContext{PartnerID: "acme", StageName: "billing"})
	if state != StatePaused {
		t.Fatalf("PAUSED-scope в цепочке должен доминировать, получили %v", state)
	}
	if rate != 0.0 {
		t.Fatalf("ожидали rate 0.0 под PAUSED, получили %v", rate)
	}
}

func TestEffectiveRateOperatorRouteScopeMatchesOnlyItsRoute(t *testing.T) {
	records := []Record{
		{Scope: ScopeOperatorRoute, ScopeID: "beeline-uz", State: StateDegraded, AdmissionRate: 0.3},
	}
	rate, state := EffectiveRate(records, MessageContext{OperatorRouteID: "ucell-uz"})
	if rate != 1.0 || state != StateActive {
		t.Fatalf("запись другого operator_route не должна применяться, получили (%v, %v)", rate, state)
	}

	rate, state = EffectiveRate(records, MessageContext{OperatorRouteID: "beeline-uz"})
	if rate != 0.3 || state != StateDegraded {
		t.Fatalf("запись своего operator_route должна применяться, получили (%v, %v)", rate, state)
	}
}
