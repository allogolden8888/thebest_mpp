// Package services — статический список имён сервисов платформы,
// опрашиваемых на /readyz.
//
// ⚠️ ЭТОТ СПИСОК НАДО ВРУЧНУЮ ДЕРЖАТЬ В СИНХРОНЕ С k8s/generate_manifests.py
// (переменная SERVICES) — тот же класс дублирования, который этот кодбейз
// уже сознательно принимает в других местах (см.
// services/credential-issuer-service/README.md про credential_ref_to_env_var,
// независимо продублированный в Rust/Python/Go/Java). k8s/generate_manifests.py
// — единственный владелец правды о том, какие сервисы вообще существуют и
// как они называются; здесь — намеренно read-only копия имён на момент
// написания (34 сервиса, снято 2026-08-13). Эта копия НЕ редактируется этой
// фазой работы в generate_manifests.py — тот файл трогает отдельная задача
// (онбординг ops-visibility-service и incident-service в манифесты вместе).
//
// Каждое имя ниже опрашивается по адресу
// http://<name>.mpp.svc:9090/readyz — HEALTH_PORT=9090 это платформенная
// конвенция (k8s/generate_manifests.py:30), которую отдают ВСЕ 34 сервиса
// независимо от workload_class (_container() в generate_manifests.py
// безусловно добавляет health-порт и readinessProbe/livenessProbe на
// /healthz и /readyz для каждого Service, включая "frontend"
// (backoffice-ui) — проверено чтением generate_manifests.py, не
// предположение).
//
// Если сервис добавлен в SERVICES в generate_manifests.py, но не добавлен
// сюда — он просто не появится в readyz-гриде до ручной правки этого
// файла. Для случаев, когда редеплой этого сервиса нежелателен (тестовый
// стенд, временный дополнительный сервис, локальная отладка) — см.
// EXTRA_SERVICES/SERVICES_OVERRIDE в cmd/ops-visibility-service/main.go.
package services

// List — 34 имени из k8s/generate_manifests.py::SERVICES, в том же
// порядке, что там (порядок не значим для опроса — Snapshot сортируется по
// имени в internal/readyz — но сохранён для удобства диффа между этим
// списком и generate_manifests.py при ручной синхронизации).
var List = []string{
	"partner-rest-receiver",
	"partner-smpp-gateway",
	"operator-smpp-session-manager",
	"operator-http-gateway",
	"pipeline-engine",
	"destination-resolution-service",
	"policy-service",
	"billing-service",
	"routing-service",
	"delivery-service",
	"delivery-reconciliation-service",
	"scheduler-critical-sweep",
	"scheduler-standard-lane",
	"scheduler-background-lane",
	"message-state-resolver",
	"execution-control-service",
	"iam-service",
	"credential-issuer-service",
	"configuration-service",
	"config-event-publisher",
	"config-cache-projector",
	"consent-cache-projector",
	"dlr-correlation-writer",
	"dlr-manager",
	"billing-outbox-publisher",
	"billing-ledger-writer",
	"billing-reconciliation",
	"partner-api",
	"backoffice-api",
	"replay-service",
	"lifecycle-writer",
	"analytics-writer",
	"partner-notification-service",
	"backoffice-ui",
}
