# Kubernetes-манифесты — LLD

**Основание:** roadmap-пункт "Kubernetes-манифесты" — деплой на Kubernetes был подтверждён пользователем ("Всё будет на кубере"), но ни один документ до этого шага не фиксировал ни kind (Deployment/StatefulSet), ни resource requests/limits, ни autoscaling-механизм, ни какие сервисы нуждаются в стабильной сетевой идентичности.

**Статус:** 32 сервиса (28 из `services_specifictaion.md` + Backoffice UI + 3 воркера, учтённых отдельно от их родительских доменов: Config Event Publisher, Config Cache Projector, Consent Cache Projector), сгенерированы из одной таблицы (`generate_manifests.py`), провалидированы `kubeconform` против настоящих OpenAPI-схем Kubernetes 1.34.

```bash
brew install kubeconform
python3 k8s/generate_manifests.py
kubeconform -strict -kubernetes-version 1.34.0 -ignore-missing-schemas -summary k8s/rendered/*.yaml
```

Результат прогона: **`Summary: 98 resources found in 34 files - Valid: 82, Invalid: 0, Errors: 0, Skipped: 16`**. 16 skipped — все 16 `ScaledObject` (KEDA CRD, `keda.sh/v1alpha1`) — kubeconform не имеет офлайн-схемы для стороннего CRD, это честно репортится как "нет схемы", не как "валидно". Без `-ignore-missing-schemas` тот же прогон явно печатает `could not find schema for ScaledObject` на каждый из них — проверено (`kubeconform -verbose rendered/pipeline-engine.yaml`). Все 82 нативных k8s-объекта (Deployment/StatefulSet/Service/PodDisruptionBudget/Namespace/ConfigMap) прошли строгую схема-валидацию. (Рост с 93/77 до 98/82 — новый `00-partner-config.yaml` ConfigMap + доп. env/volume/secret-поля на partner-rest-receiver/partner-notification-service, см. "Проверено кодревью" ниже — не новые сервисы, тот же список из 32.)

`kubectl apply --dry-run=client` не используется как проверка — в этом окружении нет живого кластера (та же причина, что раньше не позволила поднять Docker для PostgreSQL, см. `migrations/README.md`), а client-side dry-run в текущих версиях `kubectl` всё равно обращается к API-серверу за OpenAPI-схемой, а не валидирует полностью офлайн. `kubeconform` — правильный инструмент именно для офлайн-валидации манифестов без кластера.

## Почему генератор, а не 32 файла руками

Сервисы делятся всего на 4 класса нагрузки; манифест внутри класса идентичен по структуре и отличается только именем/языком/размером/портами. `SERVICES` в `generate_manifests.py` — единственный источник истины, с цитатой на `capacity_model.md`/`services_specifictaion.md` на каждое число (или явной пометкой, если числа нет — см. "Что не в capacity model" ниже).

## Классы нагрузки — новое решение этого LLD-шага

| Класс | Kind | Сервисы | Почему |
|---|---|---|---|
| `stateless` | Deployment | Большинство (Pipeline Engine, Policy, Billing, Routing, Delivery, все Go-воркеры, ...) | Нет ни стабильной identity, ни sticky-требования — Kafka-consumer или REST, масштабируется горизонтально без координации |
| `sticky-statefulset` | StatefulSet + headless Service | Partner SMPP Gateway, Operator SMPP Session Manager, Operator HTTP Gateway | Другие сервисы адресуют **конкретный под** по `owning_instance_id`/`endpoint` из Runtime Redis registry (hld.md §11) — StatefulSet даёт стабильное DNS-имя пода (`<pod>.<svc>-headless.mpp.svc`), которое можно положить в этот registry как `endpoint` |
| `kafka-streams-statefulset` | StatefulSet + headless Service + PVC | Message State Resolver, Scheduler Standard/Background Lane | RocksDB local state store требует персистентного диска, привязанного к конкретному поду (иначе каждый рестарт — полный `restore` из changelog с нуля, а не быстрый resume) |
| `frontend` | Deployment | Backoffice UI | Обычный SPA, ничем не отличается от stateless по факту, выделен только для читаемости |

## Решения, зафиксированные здесь впервые (не противоречат HLD, но и не были в нём)

1. **KEDA, не голый HPA, для Kafka-consumer сервисов.** CPU плохо коррелирует с нагрузкой consumer-группы (сервис может простаивать по CPU, но накапливать lag при медленном downstream). Consumer lag — тот же сигнал, что уже в списке метрик мониторинга (`hld.md`: "Kafka consumer lag" в §... наблюдаемости) — здесь он впервые становится триггером автоскейлинга, а не только метрикой дашборда. `ScaledObject` — CRD от KEDA, требует установленного KEDA-оператора в кластере, это явная новая зависимость инфраструктуры.
2. **`scheduler-critical-sweep` без autoscaling.** Redis sorted-set polling ~1с — не Kafka consumer, TPS-масштабирование неприменимо (`capacity_model.md`: "плоский размер, не масштабируется по TPS"), фиксированное число реплик.
3. **Guaranteed QoS (`requests == limits`) для sticky/statefulset-классов и любого сервиса с посчитанным числом в `capacity_model.md`; Burstable для pooled-сервисов без отдельной записи.** Hot-path задержка не должна деградировать от CPU throttling под соседской нагрузкой; pooled control-plane сервисы — не настолько чувствительны, и Burstable снижает суммарный резерв кластера.
4. **`topologySpreadConstraints` только для `sticky-statefulset`.** Отказ ноды с несколькими живыми SMPP bind-сессиями одновременно — хуже, чем то же самое для stateless-пода, который просто пересоздаётся без потери адресуемого состояния.
5. **HA floor = 2 реплики для всех сервисов**, даже там, где `ceil(vCPU / per-instance)` дал бы 1 — единственная реплика means единственная точка отказа независимо от того, что говорит capacity-математика.
6. **`/healthz`/`/readyz`/`/metrics` на порту 9090 — новая платформенная конвенция.** Ни один документ раньше не фиксировал health-check путь или порт (только общее упоминание "Micronaut... health endpoints" как возможность фреймворка, `services_specifictaion.md:59`) — здесь это стало обязательным контрактом для всех 32 сервисов, независимо от языка.

## Расхождение между документами, найденное на этом шаге (не решено, требует вашего решения)

`billing-outbox-publisher` и `billing-ledger-writer`: `services_specifictaion.md` называет их **Java 25** сервисами в репо-группе `billing-platform-java` (services_specifictaion.md:770-813, 1120-1125), но `capacity_model.md` группирует оба в пул **"Мелкие Go control-plane сервисы"** (`capacity_model.md:12,113`). Генератор здесь доверяет `services_specifictaion.md` (более детальный источник по языку/фреймворку на сервис) и разворачивает оба как Java с Burstable QoS и floor-реплика=2 — но реальный размер пула в `capacity_model.md` был посчитан в предположении, что это Go-сервисы (другой профиль vCPU/памяти). Стоит сверить, какой документ устарел.

## NetworkPolicy — сегментация (закрыто отдельным шагом)

`network_policies.py` генерирует default-deny + явный allow поверх той же таблицы `SERVICES` плюс `CALL_GRAPH` (реальные прямые gRPC-вызовы между сервисами, взятые из hld.md/services_specifictaion.md — не Kafka-путь, тот покрыт одним общим правилом на брокер).

```bash
python3 k8s/network_policies.py
python3 k8s/test_network_policy_reachability.py
kubeconform -strict -kubernetes-version 1.34.0 -ignore-missing-schemas -summary k8s/rendered/*.yaml
```

`test_network_policy_reachability.py` (6/6) проверяет не форму YAML (это уже делает kubeconform), а семантику — реально ли разрешён каждый заявленный вызов, и не разрешено ли что-то лишнее:

* **Billing-изоляция — не отдельный механизм, а доказанное следствие.** `billing-service`, `billing-ledger-writer`, `billing-outbox-publisher` не получают ни одного ingress-правила вообще (`test_billing_services_have_no_direct_ingress_rule_at_all`) — по документам к ним никто не обращается напрямую, только через Kafka (покрыто общим `allow-kafka-egress`). Default-deny + отсутствие специального правила = изоляция, без необходимости в отдельном "billing namespace" или дополнительном security-механизме.
* Каждое ребро `CALL_GRAPH` (`backoffice-api → configuration-service/execution-control-service/replay-service`, `billing-reconciliation → execution-control-service`, `partner-notification-service → partner-smpp-gateway`, `delivery-service → operator-smpp-session-manager/operator-http-gateway`, `delivery-reconciliation-service → operator-smpp-session-manager`) реально разрешено сгенерированной policy, а не только присутствует в исходном списке — тест читает **рендер**, не источник, иначе он подтверждал бы собственный баг генератора.
* Произвольный не-авторизованный вызывающий (`policy-service`, `partner-notification-service`) не может достучаться до `execution-control-service`, хотя тот и имеет открытые правила для легитимных вызывающих.

**Важное ограничение, зафиксированное явно, не спрятанное:** NetworkPolicy — L3/L4 (IP/порт), она не проверяет identity. `hld.md` везде, где упоминает gRPC-вызовы, говорит "mTLS" ("mTLS gRPC to exact StatefulSet pod", "gRPC + mTLS со стандартной балансировкой Kubernetes") — NetworkPolicy и mTLS **два независимых слоя**: первая сокращает, кто вообще может открыть TCP-соединение, вторая проверяет, кто на самом деле на другом конце уже открытого соединения.

**Обновление:** механизм mTLS выбран и подключён — Istio (`infra/terraform/istio.tf`, `infra/istio/peer-authentication-strict.yaml`: `PeerAuthentication` STRICT + `DestinationRule` `ISTIO_MUTUAL` для `*.mpp.svc.cluster.local`), namespace `mpp` помечен `istio-injection: enabled` здесь же в генераторе (`main()`, `00-namespace.yaml`). Сервисы не реализуют TLS в собственном коде — Envoy sidecar каждого пода прозрачно поднимает mTLS между собой, приложение общается со своим sidecar по localhost plaintext. Кодревью (PART 2) отметило `partner-notification-service`'s `insecure.NewCredentials()` в gRPC-клиенте как HIGH ("нет mTLS") — расследовано и признано false positive именно по этой причине, см. `services/partner-notification-service/README.md`.

## Что осталось вне этого шага

* Ingress-контроллер и TLS-терминация для внешних `LoadBalancer`/`ClusterIP` сервисов (Partner REST Receiver, Partner SMPP Gateway, Operator HTTP Gateway, Partner API, Backoffice API/UI) — сами Service-объекты сгенерированы, конкретный Ingress class/сертификаты — вне этого LLD.
* ~~Механизм mTLS между сервисами — не выбран~~ — выбран и подключён (Istio, см. "Важное ограничение"/"Обновление" выше); что осталось вне этого шага — сама установка Istio control plane в кластер (`infra/terraform/istio.tf` — Terraform-ресурс, не разворачивается этим генератором, как и KEDA-оператор ниже).
* Container image build/CI — `image: registry.mpp.internal/<service>:latest` — плейсхолдер, реальный registry и тегирование (semver/git-sha) не определены.
* Secrets (SMPP bind-креды, DB DSN, Kafka SASL) — не в этом слое, ожидаются через внешний механизм (Vault/External Secrets Operator), сами манифесты его не подключают.
* KEDA-оператор как инфраструктурная зависимость — сам оператор не разворачивается этим генератором, только `ScaledObject`, которые предполагают его наличие в кластере.
