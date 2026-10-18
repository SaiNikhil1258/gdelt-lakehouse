# GDELT file-arrival profile

Slots 2026-08-22 13:00Z -> 2026-09-21 20:00Z; sources: ['censored', 'first_seen', 'last_modified']

Lag reference: slot stamp -15 min.

## 1. Availability and publish lag (minutes after the slot stamp)

| stream | slots | published | never published | p50 | p90 | p95 | p99 | p99.9 | max | negative lags |
|---|---|---|---|---|---|---|---|---|---|---|
| events | 2895 | 2882 | 0 (0.0%) | 4.2 | 5.3 | 5.5 | 6.1 | 8.2 | 13.3 | 0 |
| mentions | 2895 | 2882 | 0 (0.0%) | 4.2 | 5.3 | 5.5 | 6.1 | 8.2 | 13.3 | 0 |
| gkg | 2891 | 2881 | 0 (0.0%) | 4.2 | 5.4 | 5.7 | 6.2 | 8.5 | 13.3 | 0 |

## 2. Coverage vs wait: % of published files available within N minutes of the slot stamp

| wait (min) | events | mentions | gkg |
|---|---|---|---|
| 5 | 81.19% | 81.12% | 76.71% |
| 10 | 99.93% | 99.93% | 99.90% |
| 15 | 100.00% | 100.00% | 100.00% |
| 20 <- current asset timeout | 100.00% | 100.00% | 100.00% |
| 30 | 100.00% | 100.00% | 100.00% |
| 45 | 100.00% | 100.00% | 100.00% |
| 60 | 100.00% | 100.00% | 100.00% |
| 90 | 100.00% | 100.00% | 100.00% |
| 120 | 100.00% | 100.00% | 100.00% |
| 180 <- 3-4h fixed lag | 100.00% | 100.00% | 100.00% |
| 240 <- 3-4h fixed lag | 100.00% | 100.00% | 100.00% |
| 360 | 100.00% | 100.00% | 100.00% |
| 720 | 100.00% | 100.00% | 100.00% |
| 1440 | 100.00% | 100.00% | 100.00% |

## 3. Hour-of-day effect (UTC), p95 lag per stream

- **events**: worst hour 12:00Z p95=5.8 min, best 03:00Z p95=4.1 min (spread 1.7 min, x1.4). Mild: a single global schedule is fine.
- **mentions**: worst hour 12:00Z p95=5.8 min, best 03:00Z p95=4.1 min (spread 1.7 min, x1.4). Mild: a single global schedule is fine.
- **gkg**: worst hour 14:00Z p95=6.1 min, best 03:00Z p95=4.1 min (spread 2.0 min, x1.5). Mild: a single global schedule is fine.

## 4. Drift (last 7 complete days vs all earlier complete days)

- **events**: median 4.2 vs 4.2 min (delta +0.06, x1.01); p95 5.6 vs 5.5 min (delta +0.10, x1.02). Stable.
- **mentions**: median 4.2 vs 4.2 min (delta +0.05, x1.01); p95 5.6 vs 5.5 min (delta +0.10, x1.02). Stable.
- **gkg**: median 4.3 vs 4.2 min (delta +0.08, x1.02); p95 5.8 vs 5.7 min (delta +0.13, x1.02). Stable.

## 5. Slots never published (404 after the settle window)

- **events**: none
- **mentions**: none
- **gkg**: none

## 6. GKG vs events (same slot)

GKG arrives after events by a median of 0.0 min (p95 0.2, max 0.4). GKG trails events by >10 min in 0.0% of slots, >30 min in 0.0%, >60 min in 0.0%.

## 7. Cross-check: first-seen minus Last-Modified

n=2, median 55s, p95 55s. Close agreement: Last-Modified is a trustworthy publish time.

## 8. Strategy comparison

| stream | current 20-min wait | fixed 3h lag | fixed 4h lag | recommended polling |
|---|---|---|---|---|
| events | 100.00% ; staleness ~4m | 100.00% ; staleness 180m | 100.00% ; staleness 240m | 100.00% within 180m ; median staleness 4m, p95 6m |
| mentions | 100.00% ; staleness ~4m | 100.00% ; staleness 180m | 100.00% ; staleness 240m | 100.00% within 180m ; median staleness 4m, p95 6m |
| gkg | 100.00% ; staleness ~4m | 100.00% ; staleness 180m | 100.00% ; staleness 240m | 100.00% within 180m ; median staleness 4m, p95 6m |

(coverage = % of published files captured; staleness = how old the data is when it lands in bronze)

## 9. Recommended TIMING

```python
# paste into dagster/gdelt_pipeline/timing.py (replaces the placeholder TIMING dict)
TIMING = {
    "events":   StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
    "mentions": StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
    "gkg":      StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
}
```

Tiers are `(until_slot_age_min, poll_every_min)`; the last `until` is the give-up age after which a slot is alerted as not ingested.
