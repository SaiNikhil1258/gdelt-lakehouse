# paste into dagster/gdelt_pipeline/timing.py (replaces the placeholder TIMING dict)
TIMING = {
    "events":   StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
    "mentions": StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
    "gkg":      StreamTiming(first_probe_min=2, tiers=((15, 1), (30, 5), (180, 15))),
}
