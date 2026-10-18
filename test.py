#!/usr/bin/env python3
"""GDELT v2 file-arrival profiler: when does each 15-minute file actually show up?

    collect   HEAD every slot x stream over the last N days and record when each
              file was published (HTTP Last-Modified). Fast history: ~3 x 96 x N
              requests. Run it from a machine that can reach data.gdeltproject.org.
    watch     Poll every minute from now on and record when each file is first
              SEEN. Ground truth if Last-Modified is absent or misleading, and a
              cross-check of it if present. Leave it running for a day or two.
    analyze   Turn either CSV (or both) into: percentiles, coverage-vs-wait curve,
              hour-of-day pattern, drift over time, missing-slot clusters, GKG-vs-
              events gap, a chart, and a ready-to-paste TIMING block for
              dagster/gdelt_pipeline/timing.py.

Examples
    python gdelt_arrival_analysis.py collect --days 30 --out arrivals.csv
    python gdelt_arrival_analysis.py analyze --in arrivals.csv --out-dir report/
    python gdelt_arrival_analysis.py watch --hours 48 --out first_seen.csv
    python gdelt_arrival_analysis.py analyze --in arrivals.csv first_seen.csv --out-dir report/

Lag is measured as (available_at - (slot stamp + --ref-offset-min)) in minutes. Slot stamps are UTC.
If analyze reports mostly NEGATIVE lags, files are appearing before their own stamp: the reference
time is wrong (or the server clock / Last-Modified is shifted). analyze then refuses to emit a TIMING
block. Re-run it with --ref-offset-min -15 if the stamp turns out to run one slot ahead of publication,
and run `watch` (which now also probes slots ahead of the clock) to find out which it is.
"""
import argparse
import csv
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

BASE_URL = "http://data.gdeltproject.org/gdeltv2"
STREAMS = ("events", "mentions", "gkg")
SUFFIX = {"events": "export.CSV.zip", "mentions": "mentions.CSV.zip", "gkg": "gkg.csv.zip"}
SLOT_FMT = "%Y%m%d%H%M%S"
COLUMNS = ["slot", "stream", "status", "available_at", "source", "last_modified"]
UTC = timezone.utc


# ------------------------------------------------------------------ slot helpers
def utcnow():
    return datetime.now(UTC)


def floor_slot(dt):
    dt = dt.astimezone(UTC)
    return dt.replace(minute=dt.minute - dt.minute % 15, second=0, microsecond=0)


def slots_between(start, end):
    cur, stop, out = floor_slot(start), floor_slot(end), []
    while cur <= stop:
        out.append(cur.strftime(SLOT_FMT))
        cur += timedelta(minutes=15)
    return out


# ------------------------------------------------------------------ probing
_local = threading.local()


def _session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
    return _local.s


def probe(base, slot, stream, retries=3):
    """-> (status, last_modified_iso). status: '200' | '404' | other HTTP code | 'error'."""
    url = f"{base}/{slot}.{SUFFIX[stream]}"
    for attempt in range(retries):
        try:
            r = _session().head(url, timeout=20, allow_redirects=True)
            if r.status_code in (405, 501):                       # HEAD refused -> 1-byte ranged GET
                r = _session().get(url, timeout=20, headers={"Range": "bytes=0-0"}, stream=True)
                r.close()
            if r.status_code == 404:
                return "404", ""
            if r.status_code in (200, 206):
                lm = r.headers.get("Last-Modified", "")
                return "200", (parsedate_to_datetime(lm).astimezone(UTC).isoformat() if lm else "")
            if r.status_code >= 500 or r.status_code == 429:
                raise requests.HTTPError(str(r.status_code))
            return str(r.status_code), ""
        except (requests.RequestException, ValueError, TypeError):
            time.sleep(1.5 * (attempt + 1))
    return "error", ""


def load_done(path):
    if not Path(path).exists():
        return set()
    with open(path, newline="") as f:
        return {(r["slot"], r["stream"]) for r in csv.DictReader(f)}


def open_writer(path):
    new = not Path(path).exists()
    f = open(path, "a", newline="")
    w = csv.writer(f)
    if new:
        w.writerow(COLUMNS)
    return f, w


# ------------------------------------------------------------------ collect
def cmd_collect(a):
    end = floor_slot(utcnow() - timedelta(hours=a.settle_hours)) if not a.end else datetime.strptime(a.end, SLOT_FMT).replace(tzinfo=UTC)
    start = end - timedelta(days=a.days)
    todo = [(s, st) for s in slots_between(start, end) for st in STREAMS]
    done = load_done(a.out)
    todo = [t for t in todo if t not in done]
    print(f"{len(todo)} probes to run ({a.days}d ending {end:%Y-%m-%d %H:%M}Z, {a.workers} workers, base {a.base_url})")

    f, w = open_writer(a.out)
    lock, n, counts = threading.Lock(), [0], {}

    def work(item):
        slot, stream = item
        status, lm = probe(a.base_url, slot, stream)
        with lock:
            w.writerow([slot, stream, status, lm, "last_modified" if lm else "", lm])
            counts[status] = counts.get(status, 0) + 1
            n[0] += 1
            if n[0] % 500 == 0:
                f.flush()
                print(f"  {n[0]}/{len(todo)}  {counts}")

    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        list(ex.map(work, todo))
    f.close()
    print(f"done: {counts} -> {a.out}")
    if counts.get("200"):
        with open(a.out, newline="") as fh:
            ok = [r for r in csv.DictReader(fh) if r["status"] == "200"]
        if ok and not any(r["available_at"] for r in ok):
            print("WARNING: server returned no Last-Modified header. Use `watch` to measure arrival instead.")


# ------------------------------------------------------------------ watch
def cmd_watch(a):
    f, w = open_writer(a.out)
    done = load_done(a.out)
    tracking, first_tick = set(), True
    stop_at = utcnow() + timedelta(hours=a.hours) if a.hours else None
    print(f"watching last {a.lookback_hours}h and next {a.ahead_min:g}min of slots every {a.interval}s -> {a.out} (Ctrl-C to stop)")
    try:
        while stop_at is None or utcnow() < stop_at:
            tick_start = time.monotonic()
            now = utcnow()
            window = slots_between(now - timedelta(hours=a.lookback_hours), now + timedelta(minutes=a.ahead_min))
            for slot in window:
                for stream in STREAMS:
                    key = (slot, stream)
                    if key in done:
                        continue
                    tracking.add(key)
                    status, lm = probe(a.base_url, slot, stream, retries=1)
                    if status == "200":
                        # files already present when we started have unknown arrival -> 'censored'
                        w.writerow([slot, stream, "200", utcnow().isoformat(),
                                    "censored" if first_tick else "first_seen", lm])
                        done.add(key)
                        tracking.discard(key)
            # slots that aged out of the window unresolved are recorded as missing
            oldest = window[0]
            for key in sorted(k for k in tracking if k[0] < oldest):
                w.writerow([key[0], key[1], "404", "", "", ""])
                done.add(key)
                tracking.discard(key)
            f.flush()
            first_tick = False
            time.sleep(max(0, a.interval - (time.monotonic() - tick_start)))
    except KeyboardInterrupt:
        print("stopped")
    finally:
        f.close()


# ------------------------------------------------------------------ analyze
NEG_TOL = 0.02              # more than 2% negative lags => the lag reference is wrong, don't trust/tune from it
DRIFT_MIN_DELTA_MIN = 1.0   # a drift flag needs an absolute move of at least this many minutes


def drift_check(recent, prior):
    """-> (delta_min, drifting, ratio_or_None). Ratios only make sense when both lags are positive."""
    recent, prior = float(recent), float(prior)
    delta = recent - prior
    ratio = recent / prior if (prior > 0.1 and recent > 0) else None
    moved = abs(delta) > DRIFT_MIN_DELTA_MIN and (ratio is None or ratio > 1.25 or ratio < 0.8)
    return delta, moved, ratio


def ceil_to(x, m):
    return int(math.ceil(x / m) * m)


DEADLINES = [5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 720, 1440]


def recommend(lag):
    """Derive a StreamTiming from one stream's lag series (minutes)."""
    p05, p95, p99, p999, mx = (lag.quantile(q) for q in (0.05, 0.95, 0.99, 0.999, 1.0))
    p05, p95, p99, p999, mx = (max(0.0, float(v)) for v in (p05, p95, p99, p999, mx))
    first = max(0, int(math.floor(p05)) - 1)                       # nothing arrives before ~p05: don't poll earlier
    t1 = max(15, ceil_to(p95 * 1.25, 5))                           # poll every minute until ~all normal arrivals are in
    t2 = max(2 * t1, ceil_to(p99 * 1.5, 15))                       # then every 5 min through the tail
    give_up = min(2880, max(180, ceil_to(max(p999 * 1.5, mx * 1.1), 60)))   # then every 15 min; floor 3h, cap 48h
    give_up = max(give_up, t2 + 60)
    return {"first_probe_min": first, "tiers": [[t1, 1], [t2, 5], [give_up, 15]]}


def cmd_analyze(a):
    import numpy as np
    import pandas as pd

    df = pd.concat([pd.read_csv(p, dtype=str, keep_default_na=False) for p in a.inp], ignore_index=True)
    df = df.drop_duplicates(["slot", "stream"], keep="last")
    df["slot_dt"] = pd.to_datetime(df["slot"], format=SLOT_FMT, utc=True)
    df["avail"] = pd.to_datetime(df["available_at"].replace("", None), utc=True, errors="coerce", format="ISO8601")
    df["lm"] = pd.to_datetime(df["last_modified"].replace("", None), utc=True, errors="coerce", format="ISO8601")
    ok = df[(df["status"] == "200") & df["avail"].notna() & (df["source"] != "censored")].copy()
    ok["lag"] = (ok["avail"] - ok["slot_dt"]).dt.total_seconds() / 60.0 - a.ref_offset_min
    if ok.empty:
        sys.exit("no usable rows (status 200 with a timestamp). Did the server send Last-Modified? Try `watch`.")
    neg_share = float((ok["lag"] < 0).mean())
    invalid = neg_share > NEG_TOL

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    md, stats = [], {}
    span = f"{df['slot_dt'].min():%Y-%m-%d %H:%M}Z -> {df['slot_dt'].max():%Y-%m-%d %H:%M}Z"
    md += [f"# GDELT file-arrival profile", "", f"Slots {span}; sources: {sorted(df['source'].replace('', 'n/a').unique())}", ""]

    if a.ref_offset_min:
        md += [f"Lag reference: slot stamp {a.ref_offset_min:+g} min.", ""]
    if invalid:
        warn = (f"WARNING: {neg_share * 100:.1f}% of publish lags are negative (median {ok['lag'].median():.1f} min): files appear "
                f"BEFORE their own slot stamp. The lag reference is wrong or Last-Modified is shifted, so the numbers below are "
                f"not usable for tuning. Recommendations are skipped. Run `watch`, or try --ref-offset-min -15.")
        md += [f"> **{warn}**", ""]
        print(warn, file=sys.stderr)
    stats["lag_reference"] = {"ref_offset_min": a.ref_offset_min, "negative_share": round(neg_share, 4), "valid": not invalid}

    # ---- 1. availability + percentiles
    md += ["## 1. Availability and publish lag (minutes after the slot stamp)", "",
           "| stream | slots | published | never published | p50 | p90 | p95 | p99 | p99.9 | max | negative lags |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for s in STREAMS:
        allrows, good = df[df["stream"] == s], ok[ok["stream"] == s]
        if good.empty:
            continue
        lag = good["lag"]
        q = {k: float(lag.quantile(v)) for k, v in (("p50", .5), ("p90", .9), ("p95", .95), ("p99", .99), ("p99.9", .999))}
        n404 = int((allrows["status"] == "404").sum())
        stats[s] = {"slots": len(allrows), "published": len(good), "never_published": n404,
                    "pct_never_published": round(100 * n404 / max(1, len(allrows)), 3),
                    "lag_min": {**{k: round(v, 2) for k, v in q.items()}, "max": round(float(lag.max()), 2)},
                    "negative_lags": int((lag < 0).sum())}
        md.append(f"| {s} | {len(allrows)} | {len(good)} | {n404} ({stats[s]['pct_never_published']}%) | "
                  + " | ".join(f"{q[k]:.1f}" for k in ("p50", "p90", "p95", "p99", "p99.9"))
                  + f" | {lag.max():.1f} | {stats[s]['negative_lags']} |")
        if len(good) < 2000:
            md.append(f"| | | | | *n={len(good)}: p99/p99.9 rest on very few points -- collect more days before trusting the tail* | | | | | | |")
    md.append("")

    # ---- 2. coverage vs deadline: the "20 min vs 4 hours" question, answered from data
    md += ["## 2. Coverage vs wait: % of published files available within N minutes of the slot stamp", "",
           "| wait (min) | " + " | ".join(STREAMS) + " |", "|---|" + "---|" * len(STREAMS)]
    cov = {s: [(d, float((ok[ok['stream'] == s]['lag'] <= d).mean() * 100)) for d in DEADLINES] for s in STREAMS if s in stats}
    for i, d in enumerate(DEADLINES):
        tag = " <- current asset timeout" if d == 20 else (" <- 3-4h fixed lag" if d in (180, 240) else "")
        md.append(f"| {d}{tag} | " + " | ".join(f"{cov[s][i][1]:.2f}%" if s in cov else "-" for s in STREAMS) + " |")
    md.append("")
    stats["coverage_pct"] = {s: {str(d): round(v, 3) for d, v in cov[s]} for s in cov}

    # ---- 3. hour of day
    ok["hour"] = ok["slot_dt"].dt.hour
    hourly = ok.groupby(["stream", "hour"])["lag"].agg(p50=lambda x: x.quantile(.5), p95=lambda x: x.quantile(.95), n="count").reset_index()
    hourly.to_csv(out / "hourly.csv", index=False)
    md += ["## 3. Hour-of-day effect (UTC), p95 lag per stream", ""]
    for s in STREAMS:
        h = hourly[hourly["stream"] == s]
        if h.empty:
            continue
        hi, lo = h.loc[h["p95"].idxmax()], h.loc[h["p95"].idxmin()]
        spread = float(hi["p95"] - lo["p95"])
        ratio = float(hi["p95"] / lo["p95"]) if lo["p95"] > 0.1 else None
        strong = spread > 2 and (ratio is None or ratio > 2)
        md.append(f"- **{s}**: worst hour {int(hi['hour']):02d}:00Z p95={hi['p95']:.1f} min, best {int(lo['hour']):02d}:00Z p95={lo['p95']:.1f} min "
                  f"(spread {spread:.1f} min" + (f", x{ratio:.1f}" if ratio else "") + "). "
                  + ("Strong diurnal pattern: consider hour-specific schedules." if strong else "Mild: a single global schedule is fine."))
    md.append("")

    # ---- 4. drift: last 7 COMPLETE days vs all earlier complete days, pooled.
    # (A daily p95 over ~96 slots is just the 5th-highest value -- far too noisy -- and partial edge days
    #  would skew it, so compare pooled percentiles over complete days instead.)
    ok["day"] = ok["slot_dt"].dt.floor("D")
    per_day = ok.groupby(["stream", "day"])["lag"].agg(n="count", p50=lambda x: x.quantile(.5), p95=lambda x: x.quantile(.95)).reset_index()
    per_day.to_csv(out / "daily.csv", index=False)
    okf = ok.merge(per_day[per_day["n"] >= 72][["stream", "day"]], on=["stream", "day"])   # >=75% of 96 slots
    md += ["## 4. Drift (last 7 complete days vs all earlier complete days)", ""]
    for s in STREAMS:
        g = okf[okf["stream"] == s]
        days = sorted(g["day"].unique())
        if len(days) < 10:
            md.append(f"- **{s}**: fewer than 10 complete days, drift not assessed")
            continue
        cut = days[-7]
        recent, prior = g[g["day"] >= cut]["lag"], g[g["day"] < cut]["lag"]
        d50, f50, r50 = drift_check(recent.quantile(.5), prior.quantile(.5))
        d95, f95, r95 = drift_check(recent.quantile(.95), prior.quantile(.95))
        fmt = lambda r: f", x{r:.2f}" if r is not None else ""
        md.append(f"- **{s}**: median {recent.quantile(.5):.1f} vs {prior.quantile(.5):.1f} min (delta {d50:+.2f}{fmt(r50)}); "
                  f"p95 {recent.quantile(.95):.1f} vs {prior.quantile(.95):.1f} min (delta {d95:+.2f}{fmt(r95)}). "
                  + ("Latency is drifting -- re-run this analysis periodically." if (f50 or f95) else "Stable."))
        stats[s]["drift"] = {"p50_delta_min": round(d50, 3), "p95_delta_min": round(d95, 3),
                             "p50_ratio": None if r50 is None else round(r50, 3), "p95_ratio": None if r95 is None else round(r95, 3)}
    md.append("")

    # ---- 5. missing slots
    md += ["## 5. Slots never published (404 after the settle window)", ""]
    for s in STREAMS:
        miss = df[(df["stream"] == s) & (df["status"] == "404")].sort_values("slot_dt")
        if miss.empty:
            md.append(f"- **{s}**: none")
            continue
        idx = ((miss["slot_dt"] - miss["slot_dt"].min()).dt.total_seconds() // 900).astype(int).tolist()
        runs, cur = [], [miss["slot"].iloc[0], 1]
        for prev, nxt, slot in zip(idx, idx[1:], miss["slot"].iloc[1:]):
            if nxt == prev + 1:
                cur[1] += 1
            else:
                runs.append(tuple(cur)); cur = [slot, 1]
        runs.append(tuple(cur))
        big = sorted(runs, key=lambda r: -r[1])[:5]
        md.append(f"- **{s}**: {len(miss)} slots in {len(runs)} runs; longest: " + ", ".join(f"{sl} x{n}" for sl, n in big)
                  + (" -- isolated single gaps" if big[0][1] == 1 else " -- clustered outages, not random misses"))
        stats[s]["missing_runs"] = {"runs": len(runs), "longest": [[sl, n] for sl, n in big]}
    md.append("")

    # ---- 6. GKG relative to events
    piv = ok.pivot(index="slot", columns="stream", values="lag")
    if {"gkg", "events"} <= set(piv.columns):
        gap = (piv["gkg"] - piv["events"]).dropna()
        if len(gap):
            md += ["## 6. GKG vs events (same slot)", "",
                   f"GKG arrives after events by a median of {gap.median():.1f} min (p95 {gap.quantile(.95):.1f}, max {gap.max():.1f}). "
                   f"GKG trails events by >10 min in {(gap > 10).mean() * 100:.1f}% of slots, >30 min in {(gap > 30).mean() * 100:.1f}%, "
                   f">60 min in {(gap > 60).mean() * 100:.1f}%.", ""]
            stats["gkg_minus_events_min"] = {"p50": round(float(gap.median()), 2), "p95": round(float(gap.quantile(.95)), 2)}

    # ---- 7. Last-Modified vs first-seen (only if `watch` data present)
    both = ok[(ok["source"] == "first_seen") & ok["lm"].notna()]
    if len(both):
        d = (both["avail"] - both["lm"]).dt.total_seconds()
        md += ["## 7. Cross-check: first-seen minus Last-Modified", "",
               f"n={len(both)}, median {d.median():.0f}s, p95 {d.quantile(.95):.0f}s. "
               + ("Close agreement: Last-Modified is a trustworthy publish time." if d.quantile(.95) < 120 else
                  "Large gaps: files may be re-written after first publish; prefer the first-seen data."), ""]

    # ---- 8/9. strategy comparison + recommendation (only when the lag reference is sane)
    if invalid:
        md += ["## 8-9. Strategy comparison and TIMING recommendation: SKIPPED", "",
               "Publish lags failed the negative-lag sanity check above, so coverage, staleness and polling tiers computed from them "
               "would be meaningless. Fix the lag reference (see the warning at the top) and re-run.", ""]
        stats["recommended_timing"] = None
    else:
        # ---- 8. strategy comparison + recommendation
        md += ["## 8. Strategy comparison", "",
               "| stream | current 20-min wait | fixed 3h lag | fixed 4h lag | recommended polling |", "|---|---|---|---|---|"]
        rec = {}
        for s in STREAMS:
            if s not in stats:
                continue
            r = recommend(ok[ok["stream"] == s]["lag"])
            rec[s] = r
            gu = r["tiers"][-1][0]
            c = lambda d: float((ok[ok["stream"] == s]["lag"] <= d).mean() * 100)
            p50, p95 = stats[s]["lag_min"]["p50"], stats[s]["lag_min"]["p95"]
            md.append(f"| {s} | {c(20):.2f}% ; staleness ~{p50:.0f}m | {c(180):.2f}% ; staleness 180m | {c(240):.2f}% ; staleness 240m | "
                      f"{c(gu):.2f}% within {gu}m ; median staleness {p50:.0f}m, p95 {p95:.0f}m |")
        md += ["", "(coverage = % of published files captured; staleness = how old the data is when it lands in bronze)", ""]

        snippet = ["# paste into dagster/gdelt_pipeline/timing.py (replaces the placeholder TIMING dict)", "TIMING = {"]
        for s, r in rec.items():
            tiers = ", ".join(f"({u}, {e})" for u, e in r["tiers"])
            snippet.append(f'    "{s}":{" " * (9 - len(s))}StreamTiming(first_probe_min={r["first_probe_min"]}, tiers=({tiers})),')
        snippet.append("}")
        (out / "timing_snippet.py").write_text("\n".join(snippet) + "\n")
        md += ["## 9. Recommended TIMING", "", "```python", *snippet, "```", "",
               "Tiers are `(until_slot_age_min, poll_every_min)`; the last `until` is the give-up age after which a slot is alerted as not ingested.", ""]
        stats["recommended_timing"] = rec

    (out / "summary.md").write_text("\n".join(md))
    (out / "stats.json").write_text(json.dumps(stats, indent=2, default=str))
    print("\n".join(md))

    # ---- chart
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("(matplotlib not installed: skipping chart)")
        return
    col = {"events": "#2a6fdb", "mentions": "#1a9e6f", "gkg": "#d9822b"}
    fig, ax = plt.subplots(2, 2, figsize=(14, 9))
    if invalid:
        fig.suptitle("WARNING: negative publish lags - lag reference is wrong, do not use for tuning", color="red", fontweight="bold")
    for s in STREAMS:
        lag = np.sort(ok[ok["stream"] == s]["lag"].clip(lower=0.1).values)
        if not len(lag):
            continue
        ax[0, 0].plot(lag, np.arange(1, len(lag) + 1) / len(lag) * 100, label=s, color=col[s])
        h = hourly[hourly["stream"] == s]
        ax[0, 1].plot(h["hour"], h["p95"], color=col[s], label=f"{s} p95")
        ax[0, 1].plot(h["hour"], h["p50"], color=col[s], ls="--", alpha=.6, label=f"{s} p50")
        g = okf[okf["stream"] == s]
        days = sorted(g["day"].unique())
        roll = [(days[i], g[(g["day"] >= days[i - 6]) & (g["day"] <= days[i])]["lag"].quantile(.95)) for i in range(6, len(days))]
        if roll:
            ax[1, 0].plot([r[0] for r in roll], [r[1] for r in roll], color=col[s], label=f"{s} rolling 7d p95")
        dd = per_day[(per_day["stream"] == s) & (per_day["n"] >= 72)]
        ax[1, 0].scatter(dd["day"], dd["p50"], color=col[s], s=8, alpha=.6)
        miss = df[(df["stream"] == s) & (df["status"] == "404")]
        ax[1, 1].scatter(miss["slot_dt"], [s] * len(miss), color=col[s], s=14)
    ax[0, 0].set_xscale("log")
    for x, lab in ((20, "20 min (current)"), (180, "3h"), (240, "4h")):
        ax[0, 0].axvline(x, color="grey", ls=":", lw=1)
        ax[0, 0].text(x, 2, lab, rotation=90, fontsize=8, color="grey", va="bottom", ha="right")
    ax[0, 0].set(title="Coverage vs wait (CDF of publish lag)", xlabel="minutes after slot stamp (log)", ylabel="% of published files", ylim=(0, 101))
    ax[0, 1].set(title="Hour of day (UTC): p50 / p95 lag", xlabel="hour", ylabel="minutes", xticks=range(0, 24, 3))
    ax[1, 0].set(title="Drift: rolling 7-day p95 (line), daily median (dots)", ylabel="minutes")
    ax[1, 1].set(title="Slots never published")
    if not (df["status"] == "404").any():
        ax[1, 1].text(.5, .5, "none missing", ha="center", va="center", transform=ax[1, 1].transAxes)
        ax[1, 1].set_xticks([]); ax[1, 1].set_yticks([])
    for x in (ax[0, 0], ax[0, 1], ax[1, 0]):
        x.grid(alpha=.25); x.legend(fontsize=8)
    from matplotlib.ticker import FixedLocator, FuncFormatter
    ax[0, 0].xaxis.set_major_locator(FixedLocator([1, 2, 5, 10, 20, 30, 60, 120, 240, 480, 1440]))
    ax[0, 0].xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v)}"))
    ax[0, 0].xaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
    for x in (ax[1, 0], ax[1, 1]):
        x.tick_params(axis="x", labelrotation=30)
    fig.tight_layout(rect=(0, 0, 1, 0.96 if invalid else 1))
    fig.savefig(out / "arrival_report.png", dpi=130)
    print(f"\nwrote {out}/summary.md, stats.json, " + ("" if invalid else "timing_snippet.py, ") + "hourly.csv, daily.csv, arrival_report.png")


# ------------------------------------------------------------------ CLI
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="probe historic slots via HEAD / Last-Modified")
    c.add_argument("--days", type=int, default=30)
    c.add_argument("--end", help="YYYYMMDDHHMMSS; default = now minus --settle-hours")
    c.add_argument("--settle-hours", type=float, default=6, help="ignore the newest N hours: they may simply not be published yet")
    c.add_argument("--workers", type=int, default=8)
    c.add_argument("--base-url", default=BASE_URL)
    c.add_argument("--out", default="arrivals.csv")
    c.set_defaults(fn=cmd_collect)

    w = sub.add_parser("watch", help="record first-seen time going forward")
    w.add_argument("--hours", type=float, default=0, help="stop after N hours (default: until Ctrl-C)")
    w.add_argument("--lookback-hours", type=float, default=3)
    w.add_argument("--ahead-min", type=float, default=30,
                   help="also probe slots stamped this many minutes in the FUTURE, to catch files published before their stamp")
    w.add_argument("--interval", type=int, default=60)
    w.add_argument("--base-url", default=BASE_URL)
    w.add_argument("--out", default="first_seen.csv")
    w.set_defaults(fn=cmd_watch)

    z = sub.add_parser("analyze", help="stats, chart, TIMING recommendation")
    z.add_argument("--in", dest="inp", nargs="+", required=True)
    z.add_argument("--out-dir", default="arrival_report")
    z.add_argument("--ref-offset-min", type=float, default=0.0,
                   help="lag = available_at - (slot stamp + OFFSET). Use -15 if files are published ~one slot before the stamp implies")
    z.set_defaults(fn=cmd_analyze)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()