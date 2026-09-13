# Run02: 500 TPS, 120s (degradation check vs 300 TPS baseline)

## Load profile
- rate=500 TPS, concurrency=150, duration=120s, warmup=20s / tail=15s excluded
- Same MSISDN pool (100000, beeline_uz ranges), sender_id=Click

## Completeness
- Sent: 60000/60000 accepted, 0 http_errors
- Matched against message_read_model: 60000/60000 (100%)
- 59877 DELIVERED, 123 REJECTED (0.20%, anti-spam — up from 0.07% at 300 TPS, expected: more collisions at higher rate against a fixed pool)
- exec:* drained to 0 immediately, 23/23 containers clean (0 OOM, 0 restarts)

## E2E latency comparison (steady-state window, message_id-set verified)
| Metric | 300 TPS (n=49801) | 500 TPS (n=42502) | Ratio |
|---|---|---|---|
| p50 | 228 ms | 410 ms | 1.8x |
| p95 | 368 ms | 2934 ms | 8.0x |
| p99 | 422 ms | 3145 ms | 7.5x |
| max | 586 ms | 3401 ms | 5.8x |

Rate went up 1.67x; p50 went up 1.8x (roughly proportional — fine), but p95/p99 went up 7.5-8x — clearly non-linear. This is degradation, not just "a bit slower": the tail blew past what proportional scaling would predict.

## Root cause evidence (per-hop stage latency breakdown, ClickHouse stage_events)
Incremental cost added at each pipeline hop (p95, cumulative-since-ingest deltas):

| Hop | 300 TPS Δp95 | 500 TPS Δp95 |
|---|---|---|
| incoming → destination_resolution | 20ms | 254ms |
| → policy | +22ms | +621ms |
| → billing | +34ms | +662ms |
| → routing | +19ms | +619ms |
| → delivery | +27ms | +587ms |

Every single hop's incremental cost jumped by roughly the same ~20-25x factor simultaneously — not concentrated in one service's business logic. That uniform, pipeline-wide pattern is the signature of a **shared resource approaching saturation**, not a slow individual stage.

## CPU during both runs (docker stats, steady state)
- 300 TPS: ~750-770% total; Kafka alone = 325% (42% of total, by far the largest single consumer); pipeline-engine ~86%; every other service 15-35%.
- 500 TPS: ~790-830% total (only ~5-8% more than 300 TPS, NOT proportional to the 67% rate increase); Kafka still dominant; operator-smpp-session-manager actually *lower* CPU than at 300 TPS (idle-waiting, not compute-bound).

Total CPU barely moved while tail latency exploded — rules out "just needs more cores" as the primary story. Combined with the uniform per-hop degradation, the leading candidate is the **single KRaft Kafka broker** (this stand's topology: ~10 topics per business message, `KAFKA_NUM_NETWORK_THREADS=6`, heap already reduced twice per docker-compose.yml's own history) hitting a queueing knee somewhere between 300 and 500 TPS — not any particular microservice's logic. `tps_limit` in routing_table.valid.json was checked and ruled out (parsed but `#[allow(dead_code)]`, never enforced in routing-service).

## Conclusion
300 TPS remains a valid, comparable baseline (tight distribution, no saturation signature). 500 TPS is measurably past a capacity knee on this single-broker local topology — not a hard failure (0 errors, 100% completeness, full convergence), but a real latency-degradation regime. Confirming Kafka specifically (vs. pipeline-engine's Redis CAS loop, the other shared-by-every-hop component) needs either broker-side request-queue metrics or an A/B with more Kafka network/IO threads — not yet done, flagged as next step.
