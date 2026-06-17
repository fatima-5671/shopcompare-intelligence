"""
Apache Airflow DAG — Cross-Platform E-Commerce Pipeline
Orchestrates: Scraping (all platforms at once) → KNIME ETL (via Flask API)
              → Entity Resolution → Alert Check → n8n Notification

Schedule: Every 6 hours

Changes from original:
  - Scraping is now ONE unified task (Daraz + Amazon + Alibaba in parallel,
    exactly like run_pipeline() in ecommerce_scraper.py does it).
  - Added `run_knime_workflow` task that POSTs to the Flask bridge on the
    Windows host (flask_knime_api.py) so KNIME cleans the raw CSV before ETL.
  - Removed the three separate scrape_telemart / scrape_ebay / scrape_daraz
    tasks that referenced non-existent scraper modules.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# ---------------------------------------------------------------------------
# DAG DEFAULT ARGS
# ---------------------------------------------------------------------------

default_args = {
    "owner":             "data_engineering_team",
    "depends_on_past":   False,
    "email":             ["alerts@yourproject.com"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=90),   # longer — scraper + KNIME
}

# ---------------------------------------------------------------------------
# FLASK / KNIME BRIDGE CONFIG
#   flask_knime_api.py runs on the Windows host.
#   From inside Docker, reach it via host.docker.internal (or your host IP).
# ---------------------------------------------------------------------------

FLASK_API_BASE = "http://host.docker.internal:8005"   # ← update if needed
FLASK_API_KEY  = "my-secret-key"                      # ← must match flask_knime_api.py

# ---------------------------------------------------------------------------
# SCRAPER CONFIG  (mirrors ecommerce_scraper.py defaults)
# ---------------------------------------------------------------------------

KEYWORDS = [
    "wireless earbuds",
    "smart watch",
    "men shirt",
    "women kurta",
    "electric drill",
    "screwdriver set",
]
PAGES_PER_KEYWORD = 2
PLATFORMS         = ["daraz", "amazon", "alibaba"]
OUTPUT_DIR        = "/opt/airflow/project/data/raw"

# ---------------------------------------------------------------------------
# TASK FUNCTIONS
# ---------------------------------------------------------------------------

# ── 1. Unified scraper ──────────────────────────────────────────────────────

def task_scrape_all(**context):
    """
    Run all three platform scrapers in one shot, exactly the way
    ecommerce_scraper.py's run_pipeline() works:
      • Daraz   — hidden JSON API (no browser needed)
      • Amazon  — Selenium
      • Alibaba — hidden JSON API (no browser needed)

    Pushes the saved CSV path and total record count to XCom.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/project")

    # Import the scraper's pipeline function directly
    from ecommerce_scraper import run_pipeline

    df = run_pipeline(
        keywords   = KEYWORDS,
        max_pages  = PAGES_PER_KEYWORD,
        output_dir = OUTPUT_DIR,
        platforms  = PLATFORMS,
    )

    total = len(df) if not df.empty else 0
    context["ti"].xcom_push(key="scraped_total", value=total)

    # Push per-platform counts for the summary log
    if not df.empty:
        counts = df.groupby("platform").size().to_dict()
    else:
        counts = {p: 0 for p in PLATFORMS}
    context["ti"].xcom_push(key="platform_counts", value=counts)

    # Push the latest CSV path so the KNIME task can find it
    import os
    from pathlib import Path
    raw_files = sorted(Path(OUTPUT_DIR).glob("ecommerce_data_*.csv"), reverse=True)
    latest_csv = str(raw_files[0]) if raw_files else ""
    context["ti"].xcom_push(key="latest_csv", value=latest_csv)

    print(f"\n✅  Scraping complete — {total} products across {PLATFORMS}")
    print(f"    Platform breakdown: {counts}")
    print(f"    Latest CSV: {latest_csv}")
    return total


# ── 2. KNIME ETL via Flask bridge ───────────────────────────────────────────

def task_run_knime_workflow(**context):
    """
    POST to flask_knime_api.py running on the Windows host.
    The Flask bridge triggers the KNIME batch workflow that cleans the
    raw CSV and writes ecommerce_clean.csv to the matched/ directory.

    Endpoint:  POST {FLASK_API_BASE}/run-knime
    Auth:      X-API-Key header
    """
    import requests

    ti            = context["ti"]
    latest_csv    = ti.xcom_pull(key="latest_csv", task_ids="scrape_all") or ""
    scraped_total = ti.xcom_pull(key="scraped_total", task_ids="scrape_all") or 0

    if not latest_csv:
        raise ValueError("No raw CSV path received from scrape_all task — cannot run KNIME.")

    print(f"🔧  Calling Flask/KNIME bridge at {FLASK_API_BASE}/run-knime")
    print(f"    Raw CSV : {latest_csv}")
    print(f"    Records : {scraped_total}")

    # Optional: send the dynamic CSV path in the request body so flask_knime_api
    # can pass it to KNIME as a workflow variable (requires a small update to the
    # Flask route to read body["raw_csv"] — see note at bottom of this file).
    payload = {
        "raw_csv":       latest_csv,
        "scraped_total": scraped_total,
        "run_time":      datetime.utcnow().isoformat(),
    }

    headers = {
        "X-API-Key":    FLASK_API_KEY,
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            f"{FLASK_API_BASE}/run-knime",
            json    = payload,
            headers = headers,
            timeout = 3700,   # slightly over KNIME's own 1-hour timeout
        )
        resp.raise_for_status()
        result = resp.json()
    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            f"Cannot reach Flask bridge at {FLASK_API_BASE}. "
            "Make sure flask_knime_api.py is running on the Windows host "
            "and that host.docker.internal resolves correctly inside Docker."
        )
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Flask bridge returned an error: {e} — {resp.text}")

    # Surface KNIME output details in XCom
    context["ti"].xcom_push(key="knime_output_file", value=result.get("output_file", ""))
    context["ti"].xcom_push(key="knime_output_rows", value=result.get("output_rows", 0))
    context["ti"].xcom_push(key="knime_duration_sec", value=result.get("duration_sec", 0))

    print(f"✅  KNIME finished in {result.get('duration_sec')}s")
    print(f"    Clean file : {result.get('output_file')}")
    print(f"    Clean rows : {result.get('output_rows')}")
    return result


# ── 3. Entity Resolution ────────────────────────────────────────────────────

def task_entity_resolution(**context):
    """Match products across platforms using the cleaned KNIME output."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.entity_resolution import run

    df = run()
    cross_matches = int((df.get("group_size", 1) > 1).sum()) if not df.empty else 0
    context["ti"].xcom_push(key="cross_matches", value=cross_matches)

    print(f"✅  Entity resolution — {cross_matches} cross-platform product groups found")
    return cross_matches


# ── 4. Price Drop Detection ─────────────────────────────────────────────────

def task_check_price_drops(**context):
    """
    Compare the two most recent matched CSV files.
    Flag products whose price dropped ≥ 10 % since the previous run.
    """
    import json
    import pandas as pd
    from pathlib import Path

    matched_dir = Path("/opt/airflow/project/data/matched")
    files = sorted(matched_dir.glob("matched_*.csv"), reverse=True)

    if len(files) < 2:
        print("ℹ️   Not enough historical runs to compare — skipping alert check.")
        context["ti"].xcom_push(key="price_drop_alerts", value=[])
        return []

    current  = pd.read_csv(files[0])
    previous = pd.read_csv(files[1])

    merged = current.merge(
        previous[["source", "title_clean", "live_price_pkr"]],
        on       = ["source", "title_clean"],
        suffixes = ("_now", "_prev"),
    )
    merged["drop_pct"] = (
        (merged["live_price_pkr_prev"] - merged["live_price_pkr_now"])
        / merged["live_price_pkr_prev"] * 100
    ).round(2)

    drops  = merged[merged["drop_pct"] >= 10].sort_values("drop_pct", ascending=False)
    alerts = drops[[
        "source", "title_clean",
        "live_price_pkr_now", "live_price_pkr_prev", "drop_pct",
    ]].to_dict("records")

    context["ti"].xcom_push(key="price_drop_alerts", value=alerts[:20])   # top 20
    print(f"⚠️   Found {len(alerts)} price drops ≥ 10 %")
    return alerts


# ── 5. n8n Alert ────────────────────────────────────────────────────────────

def task_send_n8n_alert(**context):
    """POST price-drop data to n8n webhook for downstream notifications."""
    import requests

    alerts = context["ti"].xcom_pull(key="price_drop_alerts", task_ids="check_price_drops") or []
    if not alerts:
        print("ℹ️   No price drops to alert.")
        return

    N8N_WEBHOOK_URL = "http://n8n:5678/webhook/price-drop-alert"   # ← update with your n8n URL

    payload = {
        "run_time":   datetime.utcnow().isoformat(),
        "drop_count": len(alerts),
        "alerts":     alerts,
    }

    try:
        resp = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        print(f"✅  n8n alert sent — HTTP {resp.status_code}")
    except requests.RequestException as e:
        # Non-critical — log and continue; don't fail the whole DAG
        print(f"⚠️   n8n alert failed (non-critical): {e}")


# ── 6. Summary Log ──────────────────────────────────────────────────────────

def task_log_summary(**context):
    """Print a formatted summary of the full pipeline run."""
    ti = context["ti"]

    scraped_total    = ti.xcom_pull(key="scraped_total",    task_ids="scrape_all")         or 0
    platform_counts  = ti.xcom_pull(key="platform_counts",  task_ids="scrape_all")         or {}
    knime_rows       = ti.xcom_pull(key="knime_output_rows", task_ids="run_knime_workflow") or 0
    knime_duration   = ti.xcom_pull(key="knime_duration_sec", task_ids="run_knime_workflow") or 0
    cross_matches    = ti.xcom_pull(key="cross_matches",    task_ids="entity_resolution")  or 0
    alerts_count     = len(
        ti.xcom_pull(key="price_drop_alerts", task_ids="check_price_drops") or []
    )

    daraz_count   = platform_counts.get("Daraz",   0)
    amazon_count  = platform_counts.get("Amazon",  0)
    alibaba_count = platform_counts.get("Alibaba", 0)

    summary = f"""
╔══════════════════════════════════════════════════╗
║            PIPELINE RUN SUMMARY                  ║
╠══════════════════════════════════════════════════╣
║  ── Scraping ──────────────────────────────────  ║
║  Daraz records:          {daraz_count:<6}                  ║
║  Amazon records:         {amazon_count:<6}                  ║
║  Alibaba records:        {alibaba_count:<6}                  ║
║  Total scraped:          {scraped_total:<6}                  ║
║  ── KNIME Cleaning ────────────────────────────  ║
║  Clean rows produced:    {knime_rows:<6}                  ║
║  KNIME duration (s):     {knime_duration:<6}                  ║
║  ── Matching & Alerts ─────────────────────────  ║
║  Cross-platform matches: {cross_matches:<6}                  ║
║  Price-drop alerts sent: {alerts_count:<6}                  ║
╚══════════════════════════════════════════════════╝
    """
    print(summary)


# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------

with DAG(
    dag_id      = "ecommerce_price_intelligence",
    description = "Full pipeline: scrape all platforms at once → KNIME ETL → match → alert",
    default_args = default_args,
    schedule     = "0 */6 * * *",          # every 6 hours
    start_date   = datetime(2026, 6, 16),
    catchup      = False,
    tags         = ["ecommerce", "scraping", "etl", "knime", "price-intelligence"],
    max_active_runs = 1,
) as dag:

    # ── Single unified scraping task (Daraz + Amazon + Alibaba) ─────────────
    scrape_all = PythonOperator(
        task_id         = "scrape_all",
        python_callable = task_scrape_all,
        execution_timeout = timedelta(minutes=60),   # scraper can be slow
    )

    # ── KNIME cleaning via Flask bridge on Windows host ──────────────────────
    run_knime_workflow = PythonOperator(
        task_id           = "run_knime_workflow",
        python_callable   = task_run_knime_workflow,
        execution_timeout = timedelta(minutes=70),   # KNIME timeout is 60 min
    )

    # ── Entity Resolution ────────────────────────────────────────────────────
    entity_resolution = PythonOperator(
        task_id         = "entity_resolution",
        python_callable = task_entity_resolution,
    )

    # ── Price Drop Detection ─────────────────────────────────────────────────
    check_price_drops = PythonOperator(
        task_id         = "check_price_drops",
        python_callable = task_check_price_drops,
    )

    # ── n8n Alert ────────────────────────────────────────────────────────────
    send_n8n_alert = PythonOperator(
        task_id         = "send_n8n_alert",
        python_callable = task_send_n8n_alert,
    )

    # ── Summary Log ──────────────────────────────────────────────────────────
    log_summary = PythonOperator(
        task_id         = "log_summary",
        python_callable = task_log_summary,
    )

    # ── DEPENDENCIES ─────────────────────────────────────────────────────────
    #
    #   scrape_all  →  run_knime_workflow  →  entity_resolution
    #                                               │
    #                                        check_price_drops
    #                                               │
    #                                        send_n8n_alert
    #                                               │
    #                                          log_summary
    #
    scrape_all >> run_knime_workflow >> entity_resolution
    entity_resolution >> check_price_drops >> send_n8n_alert >> log_summary


# ---------------------------------------------------------------------------
# NOTE — Optional: Make Flask accept a dynamic CSV path
# ---------------------------------------------------------------------------
# By default, flask_knime_api.py has RAW_CSV hardcoded.
# To let this DAG pass the dynamic CSV path each run, add this to the
# /run-knime route in flask_knime_api.py, right after the @require_api_key
# decorator:
#
#   body    = request.get_json(silent=True) or {}
#   raw_csv = body.get("raw_csv") or RAW_CSV   # fallback to hardcoded path
#
# Then replace all references to RAW_CSV in that route with `raw_csv`.
# ---------------------------------------------------------------------------