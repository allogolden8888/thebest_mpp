# Container registry — цель для CI-пайплайна (.github/workflows/ci.yml),
# заменяет плейсхолдер `registry.mpp.internal` из k8s/generate_manifests.py
# IMAGE_REGISTRY на реальный адрес после первого apply (см. outputs.tf).

resource "yandex_container_registry" "mpp" {
  name      = "mpp-${var.environment}"
  folder_id = var.folder_id
}

resource "yandex_container_repository" "service" {
  for_each = toset([for s in local.service_names : s])
  name     = "${yandex_container_registry.mpp.id}/${each.value}"
}

# Список имён сервисов — синхронизирован вручную со SERVICES в
# k8s/generate_manifests.py на момент написания; при добавлении нового
# сервиса нужно обновить оба места (перенос в общий источник истины —
# кандидат на дальнейшую доработку, см. infra/README.md).
locals {
  service_names = [
    "partner-rest-receiver", "partner-smpp-gateway", "operator-smpp-session-manager",
    "operator-http-gateway", "pipeline-engine", "destination-resolution-service",
    "policy-service", "billing-service", "routing-service", "delivery-service",
    "delivery-reconciliation-service", "scheduler-critical-sweep", "scheduler-standard-lane",
    "scheduler-background-lane", "message-state-resolver", "execution-control-service",
    "iam-service", "credential-issuer-service", "incident-service", "ops-visibility-service",
    "chat-service", "configuration-service", "config-event-publisher", "config-cache-projector",
    "consent-cache-projector", "dlr-correlation-writer", "dlr-manager",
    "billing-outbox-publisher", "billing-ledger-writer", "billing-reconciliation",
    "billing-self-service-api", "compliance-api", "partner-api", "backoffice-api",
    "partner-self-service-api", "replay-service", "lifecycle-writer",
    "analytics-writer", "pdu-log-writer", "partner-notification-service",
    "template-management-service", "backoffice-ui", "partner-portal-ui",
  ]
}
