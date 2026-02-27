import azure.functions as func
import logging
import json
import requests
import time
import io
import os
from datetime import datetime, date
from decimal import Decimal

import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# =============================================
# CONFIG
# =============================================
APIFY_TOKEN       = os.environ.get("APIFY_API_TOKEN", "")
CONNECTION_STRING = os.environ.get("AzureWebJobsStorage", "")
CONTAINER_NAME    = os.environ.get("AZURE_STORAGE_CONTAINER", "cruiseprice")

ACTORS = {
    "norwegian": {
        "actor_id": "5LxvytMZhR1sPPBg5",
        "config": {
            "locale": "en-US", "maxRows": 20, "enrichDetails": False,
            "destinations": ["CARIBBEAN", "ALASKA", "BAHAMAS", "MEDITERRANEAN"]
        }
    },
    "royal_caribbean": {
        "actor_id": "6kvWSNtwHm9j9VJTy",
        "config": {
            "locale": "en-US", "maxRows": 20, "enrichDetails": False,
            "destinations": ["CARIB", "ALASK", "BAHAM", "EUROP"]
        }
    },
    "princess": {
        "actor_id": "7h26crxEJ38pqqZnz",
        "config": {
            "locale": "en-US", "maxRows": 20, "enrichDetails": False,
            "destinations": ["Caribbean", "Alaska", "Mediterranean", "Mexico"]
        }
    }
}

COLUMN_MAP = {
    "norwegian": {
        "cruiseId":           "cruiseId",
        "title":              "title",
        "shipName":           "ship",
        "nightTitle":         "duration_label",
        "departurePort":      "departurePort",
        "arrivalPort":        "arrivalPort",
        "departureDate":      "departureDate",
        "arrivalDate":        "arrivalDate",
        "availabilityStatus": "availabilityStatus",
        "destinationNames":   "destination",
        "price_EUR_INSIDE":   "interior_price",
        "price_EUR_OCEANVIEW":"oceanview_price",
        "price_EUR_BALCONY":  "balcony_price",
        "price_EUR_MINISUITE":"minisuite_price",
        "price_EUR_SUITE":    "suite_price",
        "date":               "scraped_date",
    },
    "royal_caribbean": {
        "cruiseId":           "cruiseId",
        "title":              "title",
        "shipName":           "ship",
        "nightTitle":         "duration_label",
        "departurePort":      "departurePort",
        "arrivalPort":        "arrivalPort",
        "departureDate":      "departureDate",
        "arrivalDate":        "arrivalDate",
        "availabilityStatus": "availabilityStatus",
        "destinationNames":   "destination",
        "price_USD_I":        "interior_price",
        "price_USD_O":        "oceanview_price",
        "price_USD_B":        "balcony_price",
        "price_USD_D":        "suite_price",
        "rcBookingLink":      "booking_url",
        "currency":           "currency",
        "date":               "scraped_date",
    },
    "princess": {
        "cruiseId":           "cruiseId",
        "title":              "title",
        "shipName":           "ship",
        "nightTitle":         "duration_label",
        "departurePort":      "departurePort",
        "arrivalPort":        "arrivalPort",
        "departureDate":      "departureDate",
        "arrivalDate":        "arrivalDate",
        "availabilityStatus": "availabilityStatus",
        "destinationNames":   "destination",
        "price_USD_IE":       "interior_price",
        "price_USD_O6":       "oceanview_price",
        "price_USD_BF":       "balcony_price",
        "price_USD_MF":       "minisuite_price",
        "price_USD_S6":       "suite_price",
        "source_url":         "booking_url",
        "currency":           "currency",
        "date":               "scraped_date",
    }
}

CURRENCY_MAP = {
    "norwegian":       "EUR",
    "royal_caribbean": "USD",
    "princess":        "USD",
}

def cors_headers():
    return {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type":                 "application/json",
    }

def serialize(obj):
    if isinstance(obj, (date, datetime)): return obj.isoformat()
    if isinstance(obj, Decimal): return float(obj)
    return str(obj)


# =============================================
# BLOB HELPERS
# =============================================
def get_blob_client():
    return BlobServiceClient.from_connection_string(CONNECTION_STRING)

def get_latest_blob(line: str) -> str | None:
    client    = get_blob_client()
    container = client.get_container_client(CONTAINER_NAME)
    prefix    = f"{line}/"
    blobs     = [b.name for b in container.list_blobs(name_starts_with=prefix) if b.name.endswith(".csv")]
    return sorted(blobs)[-1] if blobs else None

def read_csv_from_blob(blob_name: str) -> pd.DataFrame:
    client = get_blob_client()
    blob   = client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    data   = blob.download_blob().readall()
    return pd.read_csv(io.BytesIO(data), low_memory=False)

def normalize_df(df: pd.DataFrame, line: str) -> list:
    col_map  = COLUMN_MAP.get(line, {})
    currency = CURRENCY_MAP.get(line, "USD")
    results  = []

    for _, row in df.iterrows():
        status = str(row.get("availabilityStatus", "")).upper()
        if status in ("SOLD_OUT", "WAITLIST", "CLOSED"):
            continue

        rec = {"cruise_line": line, "currency": currency}
        for src_col, dst_col in col_map.items():
            val = row.get(src_col)
            try:
                is_nan = pd.isna(val)
            except Exception:
                is_nan = False
            rec[dst_col] = None if is_nan else val

        price_cols = ["interior_price", "oceanview_price", "balcony_price"]
        prices = []
        for pc in price_cols:
            try:
                v = float(rec.get(pc) or 0)
                if v > 0: prices.append(v)
            except Exception:
                pass
        rec["best_price"] = min(prices) if prices else None

        if line == "norwegian" and rec.get("cruiseId"):
            rec["booking_url"] = f"https://www.ncl.com/booking/cruise/{rec['cruiseId']}"

        dep = rec.get("departurePort", "")
        arr = rec.get("arrivalPort", dep)
        rec["route"] = f"🔄 Round trip · {dep}" if dep == arr else f"{dep} → {arr}"

        if rec.get("best_price"):
            results.append(rec)

    return results


# =============================================
# APIFY HELPERS
# =============================================
def fire_apify_run(line: str) -> dict:
    """
    Trigger an Apify actor run and return immediately with run_id.
    Does NOT wait for completion — fire and forget.
    """
    details  = ACTORS[line]
    run_url  = f"https://api.apify.com/v2/acts/{details['actor_id']}/runs"
    response = requests.post(
        run_url,
        json=details["config"],
        params={"token": APIFY_TOKEN},
        timeout=30
    )
    run_data = response.json()

    if "data" not in run_data:
        raise ValueError(f"Apify start failed for {line}: {run_data}")

    run   = run_data["data"]
    return {
        "run_id":    run["id"],
        "actor_id":  details["actor_id"],
        "status":    run["status"],
        "started_at": datetime.utcnow().isoformat(),
    }

def check_apify_run(actor_id: str, run_id: str) -> dict:
    """
    Poll a single Apify run for its current status.
    Returns status + dataset_id if SUCCEEDED.
    """
    url      = f"https://api.apify.com/v2/acts/{actor_id}/runs/{run_id}"
    response = requests.get(url, params={"token": APIFY_TOKEN}, timeout=15)
    data     = response.json().get("data", {})
    return {
        "status":     data.get("status", "UNKNOWN"),
        "dataset_id": data.get("defaultDatasetId"),
    }

def fetch_and_save_dataset(line: str, dataset_id: str, timestamp: str) -> dict:
    """
    Download Apify dataset items and save as CSV to blob storage.
    Returns result metadata.
    """
    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    items     = requests.get(items_url, params={"token": APIFY_TOKEN}, timeout=30).json()

    if not items:
        return {"status": "empty", "rows": 0}

    df        = pd.DataFrame(items)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob_name = f"{line}/{line}cruises{timestamp}.csv"

    get_blob_client().get_blob_client(
        container=CONTAINER_NAME, blob=blob_name
    ).upload_blob(csv_bytes, overwrite=True)

    logging.info(f"✅ {line}: {len(df)} rows saved → {blob_name}")
    return {"status": "success", "rows": len(df), "blob": blob_name}

def save_run_state(run_info: dict):
    """Persist in-flight run IDs to blob so /api/refresh-status can read them."""
    blob_name = "refresh_state/current_runs.json"
    data      = json.dumps(run_info, default=serialize).encode("utf-8")
    get_blob_client().get_blob_client(
        container=CONTAINER_NAME, blob=blob_name
    ).upload_blob(data, overwrite=True)

def load_run_state() -> dict:
    """Read persisted run state from blob storage."""
    blob_name = "refresh_state/current_runs.json"
    try:
        blob = get_blob_client().get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        return json.loads(blob.download_blob().readall())
    except Exception:
        return {}


# =============================================
# POST /api/refresh
# Fires all 3 Apify scrapers and returns in ~3s
# Does NOT wait for scraping to complete
# UI should poll /api/refresh-status to track progress
# =============================================
@app.route(route="refresh", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST", "GET", "OPTIONS"])
def trigger_refresh(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🔄 /api/refresh called — fire and forget mode")
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=200, headers=cors_headers())

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_info  = {
        "timestamp": timestamp,
        "started_at": datetime.utcnow().isoformat(),
        "overall_status": "running",   # running | done | partial | failed
        "runs": {}
    }

    errors = {}

    # Fire all 3 scrapers in parallel — just trigger, don't wait
    for line in ACTORS:
        try:
            result = fire_apify_run(line)
            run_info["runs"][line] = {
                "run_id":    result["run_id"],
                "actor_id":  result["actor_id"],
                "status":    "RUNNING",
                "csv_saved": False,
            }
            logging.info(f"🚀 {line} run started: {result['run_id']}")
        except Exception as e:
            logging.error(f"❌ Failed to start {line}: {e}")
            errors[line] = str(e)
            run_info["runs"][line] = {"status": "FAILED_TO_START", "error": str(e), "csv_saved": False}

    # Persist run state so /api/refresh-status can check it
    try:
        save_run_state(run_info)
    except Exception as e:
        logging.warning(f"Could not save run state: {e}")

    started = [l for l in ACTORS if run_info["runs"].get(l, {}).get("status") == "RUNNING"]

    return func.HttpResponse(
        json.dumps({
            "status":     "started" if started else "failed",
            "message":    f"Scrapers started for: {', '.join(started)}. Poll /api/refresh-status for updates.",
            "timestamp":  timestamp,
            "started":    started,
            "errors":     errors,
        }, default=serialize),
        status_code=200,
        headers=cors_headers(),
    )


# =============================================
# GET /api/refresh-status
# Polls Apify run statuses, saves CSVs when done
# Called by the UI every 30 seconds after refresh
# =============================================
@app.route(route="refresh-status", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "OPTIONS"])
def refresh_status(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("📡 /api/refresh-status called")
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=200, headers=cors_headers())

    try:
        run_info = load_run_state()
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "no_refresh", "message": "No active refresh found. Click Refresh to start."}),
            status_code=200, headers=cors_headers()
        )

    if not run_info or not run_info.get("runs"):
        return func.HttpResponse(
            json.dumps({"status": "no_refresh", "message": "No active refresh found."}),
            status_code=200, headers=cors_headers()
        )

    timestamp  = run_info.get("timestamp", datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
    runs       = run_info.get("runs", {})
    any_change = False

    for line, info in runs.items():
        # Skip if already finished or failed to start
        if info.get("csv_saved") or info.get("status") in ("FAILED_TO_START", "FAILED", "ABORTED", "TIMED-OUT"):
            continue

        try:
            result = check_apify_run(info["actor_id"], info["run_id"])
            apify_status = result["status"]
            runs[line]["status"] = apify_status
            any_change = True

            if apify_status == "SUCCEEDED" and not info.get("csv_saved"):
                # Download dataset and save CSV to blob
                save_result = fetch_and_save_dataset(line, result["dataset_id"], timestamp)
                runs[line].update(save_result)
                runs[line]["csv_saved"] = save_result["status"] == "success"
                logging.info(f"✅ {line}: CSV saved")

            elif apify_status in ("FAILED", "ABORTED", "TIMED-OUT"):
                runs[line]["csv_saved"] = False
                logging.warning(f"⚠️ {line} run {apify_status}")

        except Exception as e:
            logging.error(f"❌ Status check failed for {line}: {e}")
            runs[line]["error"] = str(e)

    # Determine overall status
    statuses    = [r.get("status") for r in runs.values()]
    csv_saved   = [r.get("csv_saved", False) for r in runs.values()]
    terminal    = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT", "FAILED_TO_START"}

    all_done    = all(s in terminal for s in statuses)
    all_saved   = all(csv_saved)
    any_saved   = any(csv_saved)

    if all_done and all_saved:
        overall = "done"
    elif all_done and any_saved:
        overall = "partial"
    elif all_done and not any_saved:
        overall = "failed"
    else:
        overall = "running"

    run_info["runs"]           = runs
    run_info["overall_status"] = overall

    # Persist updated state
    if any_change:
        try:
            save_run_state(run_info)
        except Exception as e:
            logging.warning(f"Could not persist updated run state: {e}")

    # Build per-line summary for UI
    summary = {}
    for line, info in runs.items():
        summary[line] = {
            "status":    info.get("status", "UNKNOWN"),
            "csv_saved": info.get("csv_saved", False),
            "rows":      info.get("rows", 0),
            "blob":      info.get("blob", ""),
            "error":     info.get("error", info.get("message", "")),
        }

    return func.HttpResponse(
        json.dumps({
            "status":       overall,          # running | done | partial | failed | no_refresh
            "timestamp":    timestamp,
            "started_at":   run_info.get("started_at"),
            "lines":        summary,
            "message": (
                "✅ All prices updated!" if overall == "done"
                else "⚠️ Some lines updated." if overall == "partial"
                else "❌ Scraping failed." if overall == "failed"
                else "⏳ Scrapers still running, check back shortly…"
            )
        }, default=serialize),
        status_code=200,
        headers=cors_headers(),
    )


# =============================================
# GET /api/cruises
# Returns latest scraped data from blob storage
# =============================================
@app.route(route="cruises", auth_level=func.AuthLevel.ANONYMOUS)
def get_cruises(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚢 /api/cruises called")
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=200, headers=cors_headers())

    try:
        line = req.params.get("line", "all").lower().strip()
        sort = req.params.get("sort", "price").lower().strip()

        lines_to_fetch = ["norwegian", "royal_caribbean", "princess"] if line == "all" else [line]

        all_data     = []
        last_scraped = {}

        for ln in lines_to_fetch:
            blob_name = get_latest_blob(ln)
            if not blob_name:
                logging.warning(f"No CSV found for {ln}")
                continue
            try:
                ts_part = blob_name.split("cruises")[-1].replace(".csv", "")
                last_scraped[ln] = ts_part
            except Exception:
                last_scraped[ln] = None

            df      = read_csv_from_blob(blob_name)
            records = normalize_df(df, ln)
            all_data.extend(records)
            logging.info(f"✅ {ln}: {len(records)} cruises from {blob_name}")

        if sort == "price":
            all_data.sort(key=lambda x: float(x.get("best_price") or 9999999))
        elif sort == "date":
            all_data.sort(key=lambda x: str(x.get("departureDate") or ""))
        elif sort == "expensive":
            all_data.sort(key=lambda x: float(x.get("best_price") or 0), reverse=True)

        return func.HttpResponse(
            json.dumps({
                "status":       "success",
                "count":        len(all_data),
                "cruise_line":  line,
                "last_scraped": last_scraped,
                "data":         all_data,
            }, default=serialize),
            status_code=200,
            headers=cors_headers(),
        )

    except Exception as e:
        logging.error(f"❌ /api/cruises error: {e}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500, headers=cors_headers()
        )


# =============================================
# GET /api/stats
# =============================================
@app.route(route="stats", auth_level=func.AuthLevel.ANONYMOUS)
def get_stats(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("📊 /api/stats called")
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=200, headers=cors_headers())
    try:
        stats = []
        for line in ["norwegian", "royal_caribbean", "princess"]:
            blob_name = get_latest_blob(line)
            if not blob_name:
                continue
            df      = read_csv_from_blob(blob_name)
            records = normalize_df(df, line)
            prices  = [float(r["best_price"]) for r in records if r.get("best_price")]
            if prices:
                stats.append({
                    "cruise_line":   line,
                    "total_cruises": len(records),
                    "min_price":     min(prices),
                    "max_price":     max(prices),
                    "avg_price":     round(sum(prices) / len(prices), 2),
                    "currency":      CURRENCY_MAP[line],
                    "latest_file":   blob_name,
                })
        return func.HttpResponse(
            json.dumps({"status": "success", "data": stats}, default=serialize),
            status_code=200, headers=cors_headers()
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500, headers=cors_headers()
        )


# =============================================
# GET /api/health
# =============================================
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🏥 /api/health called")
    try:
        blob_client = get_blob_client()
        container   = blob_client.get_container_client(CONTAINER_NAME)
        blobs       = list(container.list_blobs())
        latest      = {}
        for line in ["norwegian", "royal_caribbean", "princess"]:
            b = get_latest_blob(line)
            latest[line] = b if b else "no file"
        return func.HttpResponse(
            json.dumps({
                "status":       "healthy",
                "storage":      True,
                "total_blobs":  len(blobs),
                "latest_files": latest,
                "timestamp":    datetime.utcnow().isoformat(),
            }),
            status_code=200, headers=cors_headers()
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "unhealthy", "storage": False, "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()}),
            status_code=500, headers=cors_headers()
        )


# =============================================
# TIMER — Daily 8 AM UTC
# =============================================
@app.schedule(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def cruise_price_timer(myTimer: func.TimerRequest) -> None:
    logging.info(f"⏰ Daily timer triggered at {datetime.utcnow().isoformat()}")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    for line, details in ACTORS.items():
        try:
            run_url  = f"https://api.apify.com/v2/acts/{details['actor_id']}/runs"
            response = requests.post(run_url, json=details["config"], params={"token": APIFY_TOKEN}, timeout=30)
            run_data = response.json()
            if "data" not in run_data:
                logging.error(f"❌ {line}: {run_data}"); continue

            run_id     = run_data["data"]["id"]
            status_url = f"https://api.apify.com/v2/acts/{details['actor_id']}/runs/{run_id}"
            waited, status = 0, "RUNNING"

            while status not in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT") and waited < 480:
                time.sleep(10); waited += 10
                status = requests.get(status_url, params={"token": APIFY_TOKEN}, timeout=15).json()["data"]["status"]

            if status != "SUCCEEDED":
                logging.error(f"❌ {line} timer failed: {status}"); continue

            dataset_id = requests.get(status_url, params={"token": APIFY_TOKEN}, timeout=15).json()["data"]["defaultDatasetId"]
            items      = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                                      params={"token": APIFY_TOKEN}, timeout=30).json()
            df        = pd.DataFrame(items)
            blob_name = f"{line}/{line}cruises{timestamp}.csv"
            get_blob_client().get_blob_client(
                container=CONTAINER_NAME, blob=blob_name
            ).upload_blob(df.to_csv(index=False).encode(), overwrite=True)
            logging.info(f"✅ {line}: {len(df)} rows → {blob_name}")
        except Exception as e:
            logging.error(f"❌ {line} timer error: {e}")
