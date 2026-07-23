output "k8s_cluster_id" {
  value = yandex_kubernetes_cluster.mpp.id
}

output "k8s_external_endpoint" {
  value = yandex_kubernetes_cluster.mpp.master[0].external_v4_endpoint
}

output "postgresql_fqdn" {
  value = yandex_mdb_postgresql_cluster.mpp.host[0].fqdn
}

output "redis_fqdns" {
  value = { for k, v in yandex_mdb_redis_cluster.mpp : k => v.host[0].fqdn }
}

output "clickhouse_fqdn" {
  value = values(yandex_mdb_clickhouse_cluster_v2.mpp.hosts)[0].fqdn
}

output "container_registry_id" {
  value       = yandex_container_registry.mpp.id
  description = "Подставить вместо IMAGE_REGISTRY в k8s/generate_manifests.py"
}
