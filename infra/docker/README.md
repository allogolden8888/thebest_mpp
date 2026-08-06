# infra/docker — опциональный extra CA-сертификат для локальной сборки

`unitel-root-ca.crt` — корневой сертификат TLS-инспектирующего прокси этой
сети (`Unitel Root Certification authority`), тот же, что уже пришлось
импортировать в `cacerts` для Maven/JDK (см. `services/billing-service/README.md`,
`services/delivery-service/README.md`). Без него любой build-step, который
лезет в интернет из **внутри контейнера** (`go mod download`, `cargo fetch`,
`mvn`-зависимости, `npm ci`), падает с `x509: certificate signed by unknown
authority` — контейнер использует собственный, изолированный CA-truststore
базового образа, который ничего не знает про сертификаты Mac Keychain или
Docker Desktop.

**По умолчанию НЕ используется** — ни один Dockerfile не подключает его
безусловно. Он передаётся явно через BuildKit secret, только когда сам
попросишь:

```bash
DOCKER_BUILDKIT=1 docker build \
  --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
  -f services/<name>/Dockerfile <build-context> \
  -t mpp/<name>:local
```

Без `--secret` сборка ведёт себя ровно так же, как без этого файла вообще —
проверено регрессией (`docker build` того же Dockerfile без `--secret` даёт
идентичную ошибку `x509`, что и до этого изменения).

## Как это работает в каждом Dockerfile

Один `RUN --mount=type=secret,id=extra_ca_cert,required=false` перед первым
network-зависимым шагом:

```dockerfile
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates; \
    fi && \
    <дальше обычная команда — go mod download / cargo fetch / mvn ...>
```

`required=false` — секрет опциональный, `if [ -s ... ]` — no-op, если файл не
примонтирован (не создаётся даже пустой файл). Для Java/Maven-сервисов
дополнительно нужен `keytool -importcert` в `$JAVA_HOME/lib/security/cacerts`
— JDK не использует системный CA-store автоматически (тот же нюанс, что был
при первом обнаружении этой проблемы для `mvn`/JDK на хосте).

Для реального деплоя (CI/registry, за пределами этой сети) сертификат просто
не передаётся — Dockerfile'ы ничего не знают о нём и не требуют его.

**Только для локальной проверки, не для продакшен-сборки образов** — сам
сертификат специфичен для этой сети/песочницы, коммитится в репозиторий
только чтобы `docker build` можно было воспроизводимо гонять локально любому,
кто окажется за тем же прокси.

## docker-compose.yml — локальная инфраструктура (шаг 1: только база)

`docker-compose.yml` поднимает PostgreSQL/3×Redis/Kafka с именами
переменных окружения ровно как в `infra/secrets/generate_external_secrets.py`
(`SECRET_KEYS`) — сервисы (когда добавятся сюда же следующим шагом)
подключаются без склейки. Порты на хосте сдвинуты (5433, 6380-6382, 9094) —
на этой машине уже отдельно запущены свои postgres/redis на дефолтных
портах (brew services), конфликтовать с ними не должны.

```bash
cd infra/docker
docker compose up -d
docker compose ps          # все health checks должны стать healthy
docker compose logs -f kafka   # если что-то не поднимается за ~30-60с
```

Пока это только инфраструктура — сами 32 сервиса ещё не подключены к этому
compose-файлу, добавляются постепенно следующим шагом (development_plan.md
2.3/2.4, "разворот в staging" — сначала локальный docker-compose, потом уже
k8s/kind).
