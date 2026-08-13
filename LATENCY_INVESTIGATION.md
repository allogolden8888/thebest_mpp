# Latency investigation — 300 TPS load test, local docker-compose

Target SLA: end-to-end latency ≤300ms, DELIVERY success ≥99.5%. All numbers below are from real
load tests (`loadgen`, rate=300, concurrency=100, 40-60s) against the full pipeline
(`DESTINATION_RESOLUTION → POLICY → BILLING → ROUTING → DELIVERY`), measured from real Kafka
`stage.completed` broker timestamps, not inference.

## Timeline of findings (chronological, each one measured before/after)

| # | Root cause | Fix | p50 before → after |
|---|---|---|---|
| 1 | `pipeline-engine`'s Kafka producer (`rdkafka`) hit librdkafka's internal `QueueFull` backpressure under its own concurrency (up to 256 in-flight tasks sharing one producer) — the Rust client retries `QueueFull` every 100ms, so every `producer.send()` under load cost a consistent **~1700ms** (≈17×100ms retries). Since pipeline-engine sits between every pair of stages, this added ~1.7s to *every single hop*. | Explicit `queue.buffering.max.messages`/`.max.kbytes` set well above librdkafka defaults in `build_producer()`. | 22183ms → 538ms |
| 2 | Same bug, confirmed independently in the other 3 Rust stage services (`destination-resolution-service`, `policy-service`, `routing-service` — identical `rdkafka::FutureProducer` + concurrent-task-pool pattern). | Same fix applied to each. | (compounding with #1) |
| 3 | Same *class* of bug in the 2 Java stage services (`billing-service`, `delivery-service`) — `KafkaProducer.send()` can silently block a worker thread up to 60s (`max.block.ms` default) with **zero timeout protection**, since the block happens before the existing `.get()` timeout budget even starts. | Explicit `buffer.memory` (64MB) and `max.block.ms` (10s, matching the existing send-timeout) in `buildProducer()`. | (compounding with #1/#2) |
| 4 | Local Kafka broker: 33 topics / 365 partitions (partition counts computed from *production* replica-count formulas, not sized for one laptop) on the image's default 1GB heap. | `KAFKA_HEAP_OPTS=-Xmx2G` in `docker-compose.yml`. | secondary contributor to #1-3 |
| 5 | Self-inflicted: an earlier raw-Kafka benchmark (`kafka-producer-perf-test.sh`) was run directly against the **live** `stage.policy` topic instead of a throwaway one, injecting ~10k non-protobuf garbage records. `policy-service`'s offset-commit design (correctly, by design) refuses to commit past a permanently-undecodable record, so its commit watermark froze there for the rest of the session. | Reset the `policy-service` consumer-group offset on `stage.policy` past the contamination. **Lesson: never point ad-hoc Kafka benchmarks at a real workload topic.** | n/a (measurement artifact, not a real regression) |
| 6 | `redis-runtime` (single-threaded) was doing real ~7-second `BGSAVE` forks under load — the default RDB save policy (`60 10000`) triggers easily at ~19 Redis round-trips/message × 300 msg/s. Confirmed via live `docker stats`: consistently 90-165% CPU during load, disappeared entirely after the fix. | `--save ""` on `redis-runtime` in `docker-compose.yml` (pure ephemeral runtime state — nothing here needs point-in-time durability across a local restart). Also flushed ~1M accumulated test keys for a clean baseline. | 67.6s → 3.3s (this was measured on top of an unrelated regression below, see next row) |
| 7 | `partner-notification-service`: no attempt cap, only a 24h TTL on partner-webhook retries. In this environment the webhook target is a deliberately-fake domain (`partner-webhook.invalid`, RFC 2606), so *every* message ever sent this session (hundreds of thousands, across many hours of testing) has been retrying with exponential backoff ever since — a sustained ~185 req/sec background storm hammering `notification.retry`'s 18 Kafka partitions continuously, competing with real test traffic the whole time. Not a bug in the backoff logic itself (30s/5min jittered backoff is correct) — just no cap on top of the 24h TTL. | Added `MaxAttempts` (=2): after 2 failed attempts, publish to a new `notification.archived` topic (DLQ-style, 7-day retention, no consumer yet) instead of rescheduling. Backlog fully drained within ~30s of deploy. | 3.3s → 1.4s p50 |
| 8 | Deploying fix #7 initially appeared to do nothing — turned out `partner-notification-service`'s Dockerfile requires **its own directory** as build context (documented in the Dockerfile's own header comment), not the repo root used for every other service. Building from repo root silently produced a stale image (**6 days old**) with no error. | Rebuilt with the correct context + `--secret id=extra_ca_cert` (needed for `go mod download` through the corporate CA). **Worth checking whether any other Go service Dockerfile has the same footgun.** | n/a (was blocking fix #7 from taking effect) |
| 9 | Kafka broker `num.network.threads` at the image default (3) for 6 concurrently-connected services × 30 topics. | Bumped to 6 via `KAFKA_NUM_NETWORK_THREADS`. | Inconclusive once Kafka was warm (see below) — left in as a reasonable default, not proven harmful or clearly beneficial in isolation. |

## Current state (last clean measurement)

- **p50 = 1.66s, p95 = 5.1s, p99 = 5.7s, max = 6.3s**
- **DELIVERY success = 100%** (comfortably over the 99.5% target; this was already true even before the latency fixes — the 300ms target is what's unmet)
- Still well above the 300ms p50 target.

## Where it's currently bottlenecked

With every per-service producer bug and the Redis/notification issues fixed, **Kafka itself is now
the most visible CPU consumer** in live `docker stats` monitoring during load — consistently
100-200%+ CPU (1-2+ cores) — while every application service sits comfortably under 70%. This is
*not* a broker misconfiguration we've found evidence for: a raw `kafka-producer-perf-test.sh`
benchmark against a single topic in isolation showed the broker itself is fast (p50=1ms, p95=2ms).
The likely explanation is simply aggregate load: 6 services × 30 topics × real produce/consume
volume on a single-broker, RF=1 local instance is a meaningfully different workload than an
isolated single-topic benchmark.

Compounding factor: this machine is also running two unrelated projects concurrently
(`smsc-simulator-*` — your local SMSC, needed for testing — and `cloudstoragecontrolsystem-*`, left
running per your call) on the same 8 shared vCPUs. Direct math check on that: those two together
use at most ~1 core, i.e. ~12.5% of total capacity — not enough on its own to explain the gap to
300ms, which is why the investigation kept going past that point and found #6-#9 above.

**No further concrete lead beyond raw Kafka broker capacity under aggregate load** has been found
at time of writing. The natural next experiment is exactly what's planned: re-run the same load
test on hardware with more RAM (this box: single Kafka broker at 2GB heap, host total 7.75GB
Docker VM memory, 8 vCPUs, shared with the two other projects) and see whether p50/p95 close the
gap to 300ms, which would confirm this is a genuine local-capacity ceiling rather than a remaining
code defect.

## Reproducing the measurement

```bash
# from repo root, services/policy-service has the proto types already generated
cd services/policy-service
# stage_breakdown.rs (temporary diagnostic, not committed — see below) consumes stage.completed,
# groups by message_id, computes per-stage timestamp deltas and a DELIVERY success rate.
# Rebuild it from this document's methodology if you need to re-measure:
#   - BaseConsumer on "stage.completed", from earliest, decode StageCompletedEvent
#   - filter to the load test's own sent_at_unix_ms window (min-5s .. max+60s) — NOT "last N
#     seconds from now", which misses messages once wall-clock time has moved on, and NOT an
#     unfiltered from-beginning read, which mixes in stale data from earlier test runs
loadgen -rate 300 -concurrency 100 -duration 60s -msisdns 8000 -out /tmp/run.jsonl
```

Every fix above was verified with a real load test before/after, not by inspection alone.
