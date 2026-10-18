import json

import requests

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DefaultSensorStatus,
    SkipReason,
)

from .gdelt_fetch import BASE_URL, get_latest_timestamps


@sensor(
    job_name="gdelt_gkg_ingest",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def gkg_availability_sensor(context: SensorEvaluationContext):
    """
    GKG availability sensor.

    Behavior:

    1. Discover the latest GDELT GKG timestamp.
    2. Once a timestamp becomes the pending timestamp, keep retrying
       that timestamp until the file is available.
    3. Do NOT move to a newer timestamp while the pending timestamp
       is still unavailable.
    4. Once the pending file is available, request the ingestion run.
    5. Only after the run request is created do we clear the pending
       timestamp.
    """

    # ---------------------------------------------------------
    # 1. Read sensor state
    # ---------------------------------------------------------

    raw_cursor = context.cursor

    processed_ts = None
    pending_ts = None

    if raw_cursor:
        try:
            state = json.loads(raw_cursor)
            if not isinstance(state, dict):
                raise ValueError("legacy cursor")

            processed_ts = state.get("processed")
            pending_ts = state.get("pending")

        except (json.JSONDecodeError, ValueError):
            processed_ts = raw_cursor.removesuffix(":alerted")
            pending_ts = None

    context.log.info(
        f"GKG sensor state: "
        f"processed={processed_ts}, "
        f"pending={pending_ts}"
    )

    # ---------------------------------------------------------
    # 2. Get latest timestamp from GDELT
    # ---------------------------------------------------------

    try:
        latest = get_latest_timestamps()
        latest_gkg_ts = latest["gkg"]

    except requests.RequestException as exc:
        context.log.warning(
            f"Could not reach GDELT manifest: {exc}"
        )

        return SkipReason(
            "GDELT manifest temporarily unavailable; "
            "will retry on the next sensor tick."
        )

    except KeyError:
        context.log.error(
            f"GDELT manifest did not contain a GKG timestamp: {latest}"
        )

        return SkipReason(
            "GDELT manifest did not contain a GKG timestamp; "
            "will retry on the next sensor tick."
        )

    context.log.info(
        f"GDELT latest GKG timestamp: {latest_gkg_ts}"
    )

    # ---------------------------------------------------------
    # 3. Determine which timestamp we should check
    # ---------------------------------------------------------
    #
    # IMPORTANT:
    #
    # If we already have a pending timestamp, KEEP checking it.
    #
    # Example:
    #
    # pending = 17:45
    # latest  = 18:00
    #
    # We still check 17:45.
    #

    if pending_ts:
        target_ts = pending_ts

        context.log.info(
            f"Retrying pending GKG timestamp: {target_ts}"
        )

    else:

        # Nothing pending.
        #
        # If latest == processed, we've already handled it.

        if processed_ts == latest_gkg_ts:
            return SkipReason(
                f"GKG {latest_gkg_ts} already processed/requested; "
                "waiting for next GDELT update."
            )

        # Start tracking the latest timestamp.
        target_ts = latest_gkg_ts

        pending_ts = target_ts

        context.log.info(
            f"New pending GKG timestamp: {target_ts}"
        )

    # ---------------------------------------------------------
    # 4. Check the target GKG file
    # ---------------------------------------------------------

    url = f"{BASE_URL}/{target_ts}.gkg.csv.zip"

    context.log.info(
        f"Checking GKG file: {url}"
    )

    try:
        response = requests.get(
            url,
            stream=True,
            allow_redirects=True,
            timeout=15,
        )

        status_code = response.status_code
        response.close()

    except requests.RequestException as exc:

        context.log.warning(
            f"Could not reach GDELT GKG file: {exc}"
        )

        # IMPORTANT:
        #
        # Keep pending_ts in the cursor so we retry the SAME
        # timestamp next time.

        context.update_cursor(
            json.dumps(
                {
                    "processed": processed_ts,
                    "pending": target_ts,
                }
            )
        )

        return SkipReason(
            f"GDELT GKG file check failed temporarily for "
            f"{target_ts}; will retry."
        )

    # ---------------------------------------------------------
    # 5. File is available
    # ---------------------------------------------------------

    if status_code == 200:

        context.log.info(
            f"GKG file is available: {target_ts}"
        )

        # Store this timestamp as processed.
        #
        # Clear pending because we are now creating the run.

        context.update_cursor(
            json.dumps(
                {
                    "processed": target_ts,
                    "pending": None,
                }
            )
        )

        return RunRequest(
            run_key=target_ts,
            run_config={
                "ops": {
                    "bronze_gkg": {
                        "config": {
                            "timestamp": target_ts,
                        }
                    }
                }
            },
        )

    # ---------------------------------------------------------
    # 6. File is not published yet
    # ---------------------------------------------------------

    if status_code == 404:

        context.log.info(
            f"GKG {target_ts} is not published yet. "
            f"Keeping it as pending."
        )

        # VERY IMPORTANT:
        #
        # Do NOT replace target_ts with the newer GDELT timestamp.
        #
        # Keep the current timestamp as pending.

        context.update_cursor(
            json.dumps(
                {
                    "processed": processed_ts,
                    "pending": target_ts,
                }
            )
        )

        return SkipReason(
            f"GKG {target_ts} is not published yet; "
            f"will keep retrying this timestamp."
        )

    # ---------------------------------------------------------
    # 7. Other HTTP status
    # ---------------------------------------------------------

    context.log.warning(
        f"GKG {target_ts} returned HTTP {status_code}; "
        f"keeping it as pending."
    )

    context.update_cursor(
        json.dumps(
            {
                "processed": processed_ts,
                "pending": target_ts,
            }
        )
    )

    return SkipReason(
        f"GKG {target_ts} returned HTTP {status_code}; "
        f"will retry."
    )