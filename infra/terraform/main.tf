provider "yandex" {
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zones[0]
}

resource "yandex_vpc_network" "mpp" {
  name = "mpp-${var.environment}"
}

resource "yandex_vpc_subnet" "mpp" {
  for_each       = toset(var.zones)
  name           = "mpp-${var.environment}-${each.value}"
  zone           = each.value
  network_id     = yandex_vpc_network.mpp.id
  v4_cidr_blocks = [cidrsubnet("10.${var.environment == "production" ? 0 : 1}.0.0/16", 8, index(var.zones, each.value))]
}

resource "yandex_vpc_security_group" "mpp_internal" {
  name       = "mpp-${var.environment}-internal"
  network_id = yandex_vpc_network.mpp.id

  ingress {
    protocol       = "TCP"
    description    = "Внутрикластерный трафик — тонкая фильтрация делается NetworkPolicy/Istio PeerAuthentication внутри k8s (k8s/network_policies.py, infra/istio/), эта группа только периметр VPC"
    v4_cidr_blocks = ["10.0.0.0/8"]
    from_port      = 0
    to_port        = 65535
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 0
    to_port        = 65535
  }
}
