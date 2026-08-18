# Credential Issuer Service

**Основание:** `luminous-hugging-charm.md` (12-фазный план закрытия API-пробелов, `/Users/Alisher/.claude/plans/luminous-hugging-charm.md`), Фаза 1 — живой выпуск/ротация partner credentials. `partner-rest-receiver/src/auth.rs` (`EnvAuthVerifier`) и `k8s/generate_manifests.py` — статическая, **deploy-time** связка `credential_ref` → env var; ротация ключа партнёром (Фаза 3, self-service) без этого сервиса потребовала бы ручного редеплоя. Terraform (`infra/terraform/vault-secrets.tf`) сеет в Vault только пять платформенных секретов (postgresql/redis×3/clickhouse) — пути `partners/*`, на которые уже ссылаются `credential_ref` в `partner.schema.json`, до этого сервиса не содержали в реальном Vault вообще никакого значения.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./... -race`, **25/25 тестов проходят**, включая полный end-to-end прогон (`grpcserver/integration_test.go`) против **реального локального Vault** (`vault server -dev`) И **реального локального PostgreSQL** одновременно — не только по отдельности замоканные слои.

```bash
brew install hashicorp/tap/vault
vault server -dev -dev-root-token-id=root -dev-listen-address=127.0.0.1:8200 &
export VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root
vault secrets enable -path=mpp -version=2 kv   # тот же mount, что infra/terraform/vault-secrets.tf vault_mount.mpp

brew services start postgresql@17   # если ещё не запущен
cd migrations && for f in V0*.sql; do psql postgresql://localhost:5432/mpp -v ON_ERROR_STOP=1 -f "$f"; done

export PATH="$PATH:$(go env GOPATH)/bin"
cd services/credential-issuer-service
go build ./... && go test ./... -race
```

## Первый реальный Vault-клиент в этой сессии

До этой фазы Vault был только читаемой стороной — `infra/secrets/generate_external_secrets.py` генерирует `ExternalSecret` YAML, который **External Secrets Operator** (не код этого репозитория) читает из Vault. Ни один сервис не писал/не читал Vault напрямую. `internal/vault/client.go` — прямой HTTP поверх Vault KV v2 API (не `github.com/hashicorp/vault/api` SDK — тот же принцип минимализма, что у остальных "маленьких Go control-plane" сервисов: три операции не оправдывают полноценный SDK):

* **`TokenSource`** — интерфейс, два реализующих типа:
  * **`KubernetesAuthTokenSource`** (production) — реальный `POST /v1/auth/kubernetes/login` с ролью `credential-issuer-service` (`infra/terraform/vault-secrets.tf`, добавлена этой фазой) и projected ServiceAccount JWT, кеширует client token до истечения TTL с запасом (`renewMargin`).
  * **`StaticTokenSource`** (local/dev/break-glass) — `VAULT_TOKEN` напрямую, тот же escape hatch, что `vault` CLI сам поддерживает через переменную окружения. `main.go` выбирает между ними: `VAULT_TOKEN` задан → static, иначе → Kubernetes auth.
* **`WriteKV2Field`** — read-modify-write (сначала читает существующие поля по пути, потом пишет merged-набор) — **не** слепой overwrite всего KV-entry: один Vault-путь может нести несколько полей (`vault_kv_path_and_property` группирует по `application`, не по отдельному credential), слепой PUT одного поля стёр бы остальные.
* **`ParseCredentialRef`** — байт-в-байт та же логика, что `infra/secrets/generate_external_secrets.py::vault_kv_path_and_property` (Python) — `vault://partners/click_uz/main/api_key` → (`partners/click_uz/main`, `api_key`). Обе реализации должны совпадать, иначе путь, по которому этот сервис пишет, разойдётся с путём, который `ExternalSecret`/`VaultAuthVerifier` читают.

## `RotateCredential` — порядок операций, не только happy path

`internal/grpcserver/server.go`: lookup `credential_ref` (Postgres) → сгенерировать секрет (`crypto/rand`, 32 байта) → **записать в Vault** → **только потом** зафиксировать в Postgres (`credentials.issued_secrets` + `credentials.rotation_audit`, одна транзакция). Если запись в Vault падает — Postgres вообще не трогается (`TestRotateCredentialVaultFailureDoesNotTouchPostgres`): не бывает состояния "Postgres думает, что секрет выпущен, а Vault его не содержит". Если Vault записался, а Postgres упал — явный `codes.Internal` с сообщением, что state рассинхронизирован (не откатываем Vault: предыдущее значение всё равно уже потеряно для читателей).

**Lookup `credential_ref` — прямое чтение `config.config_versions`, не gRPC.** `ConfigService.GetActiveVersion` (`internal_control.proto`) возвращает только метаданные (`entity_type/entity_id/version/status/created_at`) — **не несёт `payload_json` в ответе вообще**. Реальное содержимое партнёрского конфига (включая `applications[].auth.credential_ref`) живёт только в Postgres. `store.LookupCredentialRef` читает `config.config_versions` напрямую — тот же паттерн прямого cross-schema чтения, что `backoffice-api` уже использует для DLQ/reconciliation/audit browse, не новая gRPC-зависимость.

**`RotateCredential` не создаёт новые `application`/`credential_ref`** — тот уже должен существовать в PARTNER config (заведён через существующий config API при онбординге партнёра/приложения, `config.changes` не меняется, как и требует план). Первый вызов для только что заведённого приложения фактически "issue" (Vault-путь до этого не содержал значения, `rotation_audit.action='ISSUED'`), последующие — "rotate" (`action='ROTATED'`). Один RPC на оба случая — backoffice-ui одна кнопка "Rotate credential" вне зависимости от истории.

**Plaintext — show-once.** `RotateCredentialResponse.plaintext_secret` присутствует только в этом одном ответе — `credentials.issued_secrets` хранит metadata (`secret_version`/`status`/`issued_by`/`issued_at`), никогда сам секрет. Секрет живёт только в Vault.

## Что реально проверено

* **`internal/vault/client_test.go`** — реальный round-trip против `vault server -dev` (не мок HTTP): запись поля, слияние нескольких полей по одному пути (не стирают друг друга), перезапись того же поля при ротации, `Ping`/`sys/health`. `KubernetesAuthTokenSource` — `httptest.Server`, симулирующий `/v1/auth/kubernetes/login` (реального k8s ServiceAccount JWT в этой песочнице нет, тот же класс обхода, что `execution-control-service/internal/signals/prometheus_test.go`): кеширование токена (второй вызов до истечения TTL не логинится заново), релогин после истечения, ошибка при отсутствующем JWT-файле, ошибка при отклонённом login.
* **`internal/store/store_test.go`** — реальный Postgres: `LookupCredentialRef` реально парсит `config.config_versions.payload` (та же форма, что `partner.schema.json`), `ErrPartnerNotFound`/`ErrApplicationNotFound` на реальных пустых выборках, `RecordRotation` — версии инкрементируются, предыдущая активная становится `revoked`, `rotation_audit` несёт правильный `action` (`ISSUED` на первый выпуск, `ROTATED` на повторный).
* **`internal/grpcserver/server_test.go`** — фейковые `Store`/`VaultWriter`, порядок операций и маппинг ошибок в gRPC-коды (`NotFound`/`FailedPrecondition`/`Unavailable`/`Internal`).
* **`internal/grpcserver/integration_test.go`** — **ни одного фейка**: реальный `store.Postgres` + реальный `vault.Client` + настоящий `Server`, ровно та же сборка, что `main.go`. Два последовательных `RotateCredential` (issue → rotate), верифицировано независимым `vault.Client.ReadKV2` (не через тот же код-путь, что `WriteKV2Field`'s внутренний read), что Vault реально несёт новое значение после каждого вызова, не старое.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено в реальном k8s-кластере против реального `vault-helm`** — тестировано против `vault server -dev` (in-memory storage, unseal автоматический, TLS отключён) — те же явные упрощения, что сам `infra/terraform/vault.tf` документирует для dev-режима вообще.
* **`ListIssuedSecrets` не пагинирован** — для MVP-скоупа (один партнёр, разумное число приложений/ротаций) достаточно, отдельная работа, если понадобится.
* **Revoke без ротации не реализован** — только `RotateCredential` (всегда генерирует новое значение). "Отозвать credential, не выпуская новый" (полная деактивация приложения) не входило в план Фазы 1 буквально — если понадобится, отдельный RPC.
* **k8s-онбординг (`k8s/generate_manifests.py`, `infra/secrets/`) и Terraform Vault-роль — отдельный коммит**, не в этом.
* **`partner-rest-receiver`/`partner-smpp-gateway` (VaultAuthVerifier/аналог) — отдельный коммит**, читающая сторона этой же Фазы 1 (см. их README после соответствующих изменений).
