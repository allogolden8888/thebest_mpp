# A2P MPP — деплой на слабом железе (dev/staging)

Основание: `WEAK_HARDWARE_AUDIT.md` тир 1, находки 1.1 и 1.2 — до этого изменения не было способа получить компактную раскладку платформы без ручного редактирования production capacity-чисел (`capacity_model.md`), что заодно портило бы и k8s-сайзинг. Этот документ описывает два независимых, но связанных изменения: control-plane resource tier в `k8s/generate_manifests.py` (тир 1.1) и `--profile small` в `infra/kafka/generate_kafka_topics.py` (тир 1.2).

## Что даёт `--profile small`

```bash
python3 infra/kafka/generate_kafka_topics.py --profile small
```

Пишет в `infra/kafka/rendered-small/kafka-topics.yaml` (не трогает `rendered/` — production-авторитетный вывод при запуске без флагов остаётся байт-в-байт тем же, что и раньше):

| | production (по умолчанию) | small |
|---|---|---|
| `REPLICATION_FACTOR` | 3 | 1 |
| `MIN_INSYNC_REPLICAS` | 2 | 1 |
| Брокеров | 5 (`RF+2`) | 1 |
| Партиции WORKLOAD-топика | `ceil(реальные_реплики_потребителя × 1.5)` | `ceil(MIN_REPLICAS(=2) × 1.5)` = 3, для любого топика |
| `CONTROL_PARTITIONS` | 6 | 2 |
| `DLQ_PARTITIONS` | 3 | 1 |

## Что теряется явно (не спрятано)

**RF=1 + 1 брокер = ПОЛНОЕ отсутствие replica-level отказоустойчивости.** Потеря единственного брокера — это потеря данных, не деградация. Это осознанный компромисс, приемлемый ТОЛЬКО для одноузлового dev/staging-стенда, где сам под уже является единственной точкой отказа платформы независимо от Kafka — никогда не использовать этот профиль для production-трафика.

`min.insync.replicas=1` при `acks=all` означает, что подтверждение записи брокером достаточно само по себе (нет второго ISR, который мог бы его подтвердить) — та же семантика durability, что и локальный `docker-compose` Kafka на этой платформе уже имеет сегодня (RF=1 там тоже, см. `LATENCY_INVESTIGATION.md`), просто теперь формализовано как явный профиль генератора, а не только ручная compose-конфигурация.

## Парный фикс — control-plane resource tier (k8s)

Независимое, но естественно сочетающееся изменение — `k8s/generate_manifests.py` теперь даёт 13 низконагруженным admin/control-plane сервисам (`iam-service`, `incident-service`, `ops-visibility-service`, `credential-issuer-service`, `execution-control-service`, `configuration-service`, `config-event-publisher`, `config-cache-projector`, `consent-cache-projector`, `partner-api`, `backoffice-api`, `replay-service`, `backoffice-ui`) отдельный маленький ресурсный профиль (`CONTROL_PLANE_RESOURCES`: 500m/1Gi лимит, 250m/512Mi реквест) вместо плоского профиля на язык — см. `WEAK_HARDWARE_AUDIT.md` тир 1.1 для полного разбора. Для деплоя на слабом железе оба изменения имеет смысл применять вместе: маленькая Kafka-раскладка без пропорционально маленьких control-plane реквестов не даст полного эффекта, и наоборот.

## Минимальные требования к железу

**TODO: заполнить после реального прогона.** Пока не измерено — числа в этом документе (как и во всех остальных числах `capacity_model.md`) оценки, не измерения, требуют подтверждения нагрузочным тестом на реальном одноузловом стенде перед тем, как на них полагаться. Ожидаемый порядок величины после обоих изменений (control-plane tier + small Kafka-профиль) — заметно меньше текущего production-baseline (см. `WEAK_HARDWARE_AUDIT.md` тир 1.1: только control-plane тир до этого изменения требовал 44 vCPU/88Gi одних лишь реквестов), но точная цифра для одноузлового стенда не выведена аналитически, нужен реальный прогон `docker compose`/`kind`/`minikube` с этим профилем.

## Как проверить, что дефолтное поведение не изменилось

```bash
python3 infra/kafka/generate_kafka_topics.py   # без флага — production, как раньше
git status infra/kafka/rendered/               # rendered/ игнорируется git (**/rendered/ в .gitignore),
                                                 # поэтому сравнивать вывод нужно вручную/локально, не через git diff
```
