# Baseline run: 300 TPS, run01

- Machine: Ryzen 5 7600X, 12 CPU / 15.16GB Docker Desktop allocation, quiet (no other load during run)
- Start: 2026-09-13T15:20:30Z, End: 2026-09-13T15:24:31Z (240s send window)
- PIPELINE_COMMIT_INTERVAL_MS=0 (default, per-record commit)
- git commit: see git_commit.txt; uncommitted local diff: execution-control-service fix + docker-compose.yml wiring (see git_diff.txt)

## Load profile
- rate=300 TPS, concurrency=100 (safety-valve cap, not rate driver — open loop), duration=240s, warmup=45s excluded, tail=30s excluded
- msisdn pool: 100000, drawn only from beeline_uz number ranges (this stand's single operator-smpp-session-manager + SMSC simulator only serves beeline_uz; ucell_uz/uzmobile_uz have no live SMPP session)
- sender_id=Click (must match policy_ruleset.valid.json sender_validation.allowed_sender_ids exactly, case-sensitive)

## Completeness (message_id SET match, not counts)
- Sent (accepted, HTTP 202): 72000/72000, 0 http_errors, 0 non-202
- Matched against messaging.message_read_model: 72000/72000 (100%)
- Status breakdown: 71953 DELIVERED (99.93%), 47 REJECTED (0.07%, consistent with anti_spam max_messages=3/60s/msisdn/category — all loadgen traffic falls into one UNTEMPLATED category bucket)
- exec:* keys after run: 0 (full convergence, immediate drain)

## Container health
- 23/23 containers: status=running, OOM=false, restarts=0
- Kafka consumer group lag after run: negligible (<=1 per partition, expected tail)
- Kafka topology: verify_topics.sh OK, 32/32 topics match create_topics.sh

## E2E latency (steady-state window 15:21:15Z - 15:24:01Z, n=49801, cross-checked against loadgen sent_at_ms count=49800)
| Metric | Value |
|---|---|
| p50 | 228 ms |
| p95 | 368 ms |
| p99 | 422 ms |
| max | 586 ms |

Tight distribution (p99/p50 ratio ~1.85, max only 586ms) — consistent with a genuinely quiet host, unlike the Mac session's finding of host-CPU-contention-induced p99 variance of 587-19967ms on nominally identical config (PLATFORM_STATE_FOR_REVIEW.md §11). This run is a valid, comparable 300 TPS baseline.

## Fixes applied before this run (uncommitted, this session)
- execution-control-service: was entirely missing from docker-compose.yml; pipeline-engine/partner-rest-receiver fail-closed without a GLOBAL sentinel on execution.control, and the service itself only ever published after a successful Prometheus-driven tick (impossible on zero traffic — NaN forever). Fixed with a real code change (EnsureGlobalBootstrap in internal/kafkaio/publisher.go): publishes a safe ACTIVE default at startup only if no GLOBAL record exists yet, verified against an actually-emptied Kafka topic (not just the previously-running instance). Wired the service into docker-compose.yml and backoffice-api's EXECUTION_CONTROL_SERVICE_ADDR.
