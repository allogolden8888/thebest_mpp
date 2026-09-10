# PDU Log Writer

**Основание:** `BACKOFFICE_DESIGN_SPEC.md` Экраны 38-40 (A2P/P2A/DLRs) — per-PDU диагностический след, ранее нигде не собирался (только момент submit, момент DLR, см. Блок C того же документа). `operator-smpp-session-manager` теперь публикует `OperatorPduLog` в `operator.pdu.log` на каждый реально пересечённый по проводу PDU (`submit_sm`/`submit_sm_resp`/`deliver_sm`/`deliver_sm_resp`); этот сервис — `operator.pdu.log` → ClickHouse batch insert, тот же `service_internal_methods.md` §6.2 паттерн (`on_event`/`batch_buffer`/`flush_batch`), что `analytics-writer`.

**Почему отдельный сервис, а не расширение `analytics-writer`** (см. также комментарий в `k8s/generate_manifests.py`): схема per-PDU лога (`operator_id`/`direction`/`pdu_type`/`sequence_number`/`smsc_message_id`) не пересекается со `analytics.stage_events` (`stage_name`/`outcome`/`lifecycle_status`), а объём на порядок выше — по PDU, не по сообщению. Общий консьюмер означал бы либо мешать две ClickHouse-схемы в одном `FlushBatch`, либо форкать batching-логику на две независимые ветки внутри одного сервиса. Пишет в ту же базу `analytics` (общий ClickHouse), отдельную таблицу — не отдельный кластер.

**Статус:** компилируется и тестируется — `go build ./... && go test ./...`. ClickHouse-путь (`internal/store/store_test.go`) — реальный `INSERT`/`SELECT` round-trip против локального ClickHouse (пропускается, если недоступен).

```bash
cd services/pdu-log-writer
go build ./...
go test ./...
```

## Схема

```sql
CREATE TABLE analytics.operator_pdu_log (
    operator_id, protocol, direction, pdu_type, sequence_number,
    message_id, stage_execution_id, smsc_message_id, segment_id, status,
    occurred_at, ingested_at
) ENGINE = ReplacingMergeTree() ORDER BY (occurred_at, operator_id, sequence_number, pdu_type)
```

`ReplacingMergeTree` — тот же dedup-принцип, что `analytics.stage_events`: at-least-once Kafka redelivery не должна задваивать строки. Читатели (`backoffice-api`) обязаны использовать `FINAL`.

## Корреляция с исходным сообщением

`message_id`/`stage_execution_id` известны для A2P-направления (`submit_sm`/`submit_sm_resp` — `SubmitRequest` уже несёт их на уровне `operator-smpp-session-manager`), но НЕ известны на уровне DLR PDU (`deliver_sm`/`deliver_sm_resp`) — единственный ключ там `smsc_message_id`, тот же барьер, что уже задокументирован у `OperatorDlr`/`dlr-correlation-writer`. `backoffice-api` резолвит DLR-строки per-message через `smsc_message_id`, полученный из `operator.submit.accepted`/`dlr.dlr_correlation`, не через прямой `message_id` в этой таблице.

## Что НЕ реализовано на этом шаге

* Партиционирование/TTL/codec ClickHouse — не настроены, схема минимальна (тот же выбор, что `analytics-writer`).
* `/metrics` — плейсхолдер (валидный 200), без реальных счётчиков.
* `docker build` — не собран здесь автоматически (Dockerfile следует 1:1 паттерну `analytics-writer/Dockerfile`), но проверен вручную при живой docker-compose верификации (см. коммит).
