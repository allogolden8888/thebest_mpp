// Семантические проверки, невыразимые чистым JSON Schema — перенос 1:1 из
// config_schemas/validate_all.py (SEMANTIC_CHECKS), запускаются после
// структурной валидации, только если она уже прошла (та же последовательность,
// что в validate_all.py::run).
package validate

import (
	"encoding/json"
	"fmt"
)

type semanticCheck func(instance any) []string

var semanticChecks = map[EntityType]semanticCheck{
	EntityPipeline:     validatePipelineGraph,
	EntityRoutingTable: validateRoutingTable,
	EntityNumberRange:  validateNumberRangeSemantics,
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asSlice(v any) ([]any, bool) {
	s, ok := v.([]any)
	return s, ok
}

// validatePipelineGraph — hld.md §6/§5.3.1: entry_node_id должен указывать
// на DESTINATION_RESOLUTION; POLICY.REJECTED должен вести в BILLING, не
// терминировать; next не должен ссылаться на несуществующие узлы.
func validatePipelineGraph(instance any) []string {
	doc, ok := asMap(instance)
	if !ok {
		return []string{"pipeline payload не является объектом"}
	}
	nodesRaw, _ := asSlice(doc["nodes"])
	nodesByID := make(map[string]map[string]any, len(nodesRaw))
	for _, n := range nodesRaw {
		node, ok := asMap(n)
		if !ok {
			continue
		}
		if id, ok := node["node_id"].(string); ok {
			nodesByID[id] = node
		}
	}

	var errors []string

	entryID, _ := doc["entry_node_id"].(string)
	entry, found := nodesByID[entryID]
	if !found {
		errors = append(errors, fmt.Sprintf("entry_node_id=%q не найден среди nodes", entryID))
	} else if entry["stage_name"] != "DESTINATION_RESOLUTION" {
		errors = append(errors, fmt.Sprintf(
			"Правило hld.md §6: entry_node_id обязан указывать на узел DESTINATION_RESOLUTION, а не %v",
			entry["stage_name"]))
	}

	for _, n := range nodesRaw {
		node, ok := asMap(n)
		if !ok || node["stage_name"] != "POLICY" {
			continue
		}
		next, _ := asMap(node["next"])
		rejectedTargetID, hasRejected := next["REJECTED"].(string)
		if !hasRejected || rejectedTargetID == "" {
			if v, exists := next["REJECTED"]; !exists || v == nil {
				errors = append(errors, fmt.Sprintf(
					"Правило hld.md §5.3.1: узел POLICY '%v' обязан вести REJECTED в узел BILLING, а не терминировать напрямую",
					node["node_id"]))
				continue
			}
		}
		target, targetFound := nodesByID[rejectedTargetID]
		if !targetFound || target["stage_name"] != "BILLING" {
			gotStage := "несуществующий узел"
			if targetFound {
				gotStage = fmt.Sprintf("%v", target["stage_name"])
			}
			errors = append(errors, fmt.Sprintf(
				"Правило hld.md §5.3.1: POLICY.REJECTED узла '%v' ведёт в %s, а должен — в BILLING",
				node["node_id"], gotStage))
		}
	}

	for _, n := range nodesRaw {
		node, ok := asMap(n)
		if !ok {
			continue
		}
		next, _ := asMap(node["next"])
		for outcome, targetRaw := range next {
			if targetRaw == nil {
				continue
			}
			targetID, _ := targetRaw.(string)
			if _, exists := nodesByID[targetID]; !exists {
				errors = append(errors, fmt.Sprintf("Узел '%v' ссылается на несуществующий next '%s' (%s)", node["node_id"], targetID, outcome))
			}
		}
	}

	return errors
}

// validateRoutingTable — active_route_id должен входить в routes[],
// failover_priority должен быть уникален.
func validateRoutingTable(instance any) []string {
	doc, ok := asMap(instance)
	if !ok {
		return []string{"routing_table payload не является объектом"}
	}
	routesRaw, _ := asSlice(doc["routes"])
	routeIDs := make(map[string]bool, len(routesRaw))
	priorities := make(map[float64]int, len(routesRaw))
	var priorityOrder []float64

	for _, r := range routesRaw {
		route, ok := asMap(r)
		if !ok {
			continue
		}
		if id, ok := route["route_id"].(string); ok {
			routeIDs[id] = true
		}
		if p, ok := route["failover_priority"].(float64); ok {
			priorities[p]++
			priorityOrder = append(priorityOrder, p)
		}
	}

	activeRouteID, _ := doc["active_route_id"].(string)
	if !routeIDs[activeRouteID] {
		ids := make([]string, 0, len(routeIDs))
		for id := range routeIDs {
			ids = append(ids, id)
		}
		return []string{fmt.Sprintf("active_route_id=%q не входит в routes[]: %v", activeRouteID, ids)}
	}

	for _, count := range priorities {
		if count > 1 {
			return []string{fmt.Sprintf("failover_priority не уникален внутри routes[]: %v", priorityOrder)}
		}
	}
	return nil
}

// validateNumberRangeSemantics — range_end >= range_start (JSON Schema не
// выражает межпольную арифметику).
func validateNumberRangeSemantics(instance any) []string {
	doc, ok := asMap(instance)
	if !ok {
		return []string{"number_range payload не является объектом"}
	}
	start, _ := doc["range_start"].(float64)
	end, _ := doc["range_end"].(float64)
	if end < start {
		return []string{fmt.Sprintf("range_end (%v) < range_start (%v) — пустой или обратный диапазон", end, start)}
	}
	return nil
}

// runSemanticChecks — вызывается Validate после успешной структурной
// проверки. instance уже unmarshalled JSON (any/map).
func runSemanticChecks(entityType EntityType, payloadJSON []byte) []string {
	check, ok := semanticChecks[entityType]
	if !ok {
		return nil
	}
	var instance any
	if err := json.Unmarshal(payloadJSON, &instance); err != nil {
		return nil // уже отловлено структурной валидацией
	}
	return check(instance)
}