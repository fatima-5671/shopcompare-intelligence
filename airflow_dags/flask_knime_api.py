"""
flask_knime_api.py
==================
Flask API running on WINDOWS HOST that receives requests from
Airflow (inside Docker) and executes the KNIME batch workflow.

Architecture:
  Airflow (Docker) ──POST──► flask_knime_api.py (Windows :8005)
                                    │
                                    ▼
                             KNIME batch process
                                    │
                                    ▼
                             ecommerce_clean.csv

HOW TO RUN (on Windows):
  1. Install: pip install flask pandas
  2. Update KNIME_EXECUTABLE and WORKFLOW_DIR below to match your PC paths
  3. Run: python flask_knime_api.py
  4. Flask listens on http://0.0.0.0:8005
"""

import os
import subprocess
import logging
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify

# ─────────────────────────────────────────────────────────────────────────────
# Configuration  ← UPDATE THESE PATHS TO MATCH YOUR WINDOWS MACHINE
# ─────────────────────────────────────────────────────────────────────────────

# Full path to knime.exe on your Windows system
KNIME_EXECUTABLE = r"C:\Users\fatim\AppData\Local\Programs\KNIME\knime.exe"

# Path to your specific E-Commerce KNIME workflow directory
KNIME_WORKFLOW_DIR = r"C:\Users\fatim\Tools and Tecniques for DS\ecommerce_pipeline\ecommerce_cleaning_workflow"

# Input and output paths for your E-Commerce source data
# NOTE: These must be standard Windows paths accessible by your local machine
RAW_CSV       = r"C:\Users\fatim\Tools and Tecniques for DS\ecommerce_pipeline\data\raw\ecommerce_data_20260616_195617.csv"
PROCESSED_DIR = r"C:\Users\fatim\Tools and Tecniques for DS\ecommerce_pipeline\data\matched"

# API security key — Must match the FLASK_API_KEY inside your Airflow DAG code!
API_KEY = "my-secret-key"

# Flask server port configuration
PORT = 8005

# ─────────────────────────────────────────────────────────────────────────────
# Flask App Setup
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Auth Decorator
# ─────────────────────────────────────────────────────────────────────────────

def require_api_key(f):
    """Reject requests that don't include the correct X-API-Key header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        provided = request.headers.get("X-API-Key", "")
        if provided != API_KEY:
            log.warning(f"Unauthorized request attempt blocked from {request.remote_addr}")
            return jsonify({"error": "Unauthorized", "message": "Invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check — Let's you confirm the backend bridge is reachable."""
    return jsonify({
        "status"     : "ok",
        "service"    : "flask_ecommerce_knime_api",
        "timestamp"  : str(datetime.utcnow()),
        "knime_exe"  : KNIME_EXECUTABLE,
        "knime_found": os.path.exists(KNIME_EXECUTABLE),
    })


@app.route("/run-knime", methods=["POST"])
@require_api_key
def run_knime():
    """
    Trigger the E-Commerce KNIME batch workflow node engine.
    Called by Airflow's run_knime_workflow task.

    Steps:
    1. Verify raw scraped input CSV file exists
    2. Build and run the KNIME e-commerce configuration script
    3. Verify ecommerce_clean.csv was produced safely
    4. Return result summary JSON data to the Airflow task engine
    """
    log.info("=" * 60)
    log.info(f"Received E-Commerce /run-knime request from {request.remote_addr}")
    log.info("=" * 60)

    # ── Verify input source file existence ───────────────────────────────────
    if not os.path.exists(RAW_CSV):
        msg = f"Raw source e-commerce file not found: {RAW_CSV}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 400

    # ── Ensure output destination directory layout structure ──────────────────
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    clean_output = os.path.join(PROCESSED_DIR, "ecommerce_clean.csv")

    # ── Build KNIME batch orchestration execution execution tree ─────────────
    cmd = [
        KNIME_EXECUTABLE,
        "-consoleLog",
        "-noexit",
        "-nosplash",
        "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
        f"-workflowDir={KNIME_WORKFLOW_DIR}",
        f"-workflow.variable=inputFile,{RAW_CSV},String",
        f"-workflow.variable=outputDir,{PROCESSED_DIR},String",
    ]

    log.info(f"Executing KNIME Workspace Command Line String:\n  {' '.join(cmd)}")
    start_time = datetime.utcnow()

    # ── Run the batch subsystem ──────────────────────────────────────────────
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,   # 1 hour processing boundary max limit
            cwd=PROCESSED_DIR,
        )
    except subprocess.TimeoutExpired:
        msg = "KNIME batch instance timed out after 1 hour allocation"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 500
    except FileNotFoundError:
        msg = f"KNIME executable not found at designated path: {KNIME_EXECUTABLE}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 500

    duration = (datetime.utcnow() - start_time).seconds

    # Print out standard KNIME streaming outputs to logs
    if result.stdout:
        log.info(f"KNIME Node Log Output:\n{result.stdout[:3000]}")
    if result.stderr:
        log.warning(f"KNIME Standard Diagnostic Messages:\n{result.stderr[:2000]}")

    # ── Check return code status verification tracking flags ─────────────────
    if result.returncode != 0:
        msg = f"KNIME batch runner collapsed with structural process exit code {result.returncode}"
        log.error(msg)
        return jsonify({
            "status"     : "error",
            "message"    : msg,
            "returncode" : result.returncode,
            "stderr"     : result.stderr[:1000],
        }), 500

    # ── Confirm the target artifact table output matches expected location ───
    if not os.path.exists(clean_output):
        msg = f"KNIME process terminated successfully (code 0) but target table file missing: {clean_output}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 500

    # Read row tracking metadata from output tables safely
    output_rows = 0
    try:
        import pandas as pd
        df_check = pd.read_csv(clean_output)
        output_rows = len(df_check)
    except Exception as parse_err:
        log.warning(f"Could not compute precise final output row counts: {str(parse_err)}")

    log.info(f"KNIME batch operation finalized successfully in {duration} seconds.")
    log.info(f"Clean Target Matrix: {clean_output} ({output_rows} processing rows parsed)")
    log.info("=" * 60)

    return jsonify({
        "status"        : "success",
        "message"       : "E-Commerce KNIME cleaning workflow executed successfully",
        "output_file"   : clean_output,
        "output_rows"   : output_rows,
        "duration_sec"  : duration,
        "returncode"    : result.returncode,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Execution Initializer Main Hook Block
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  🚀 Flask KNIME Bridge Engine Running [E-COMMERCE MODE]")
    log.info(f"  Target Local Endpoint: http://0.0.0.0:{PORT}")
    log.info(f"  KNIME Bin Binary Path: {KNIME_EXECUTABLE}")
    log.info(f"  Workflow Module Directory Path: {KNIME_WORKFLOW_DIR}")
    log.info(f"  Input Vector CSV Source: {RAW_CSV}")
    log.info(f"  Output Landing Directory Location: {PROCESSED_DIR}")
    log.info("=" * 60)

    # Sanity health checking for KNIME engine pathing references on boot up
    if not os.path.exists(KNIME_EXECUTABLE):
        log.warning(f"⚠️  CRITICAL: KNIME engine executable target missing at: {KNIME_EXECUTABLE}")
        log.warning("   Please update the local KNIME_EXECUTABLE configuration path directly.")

    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
    )