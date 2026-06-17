#!/usr/bin/env python3
"""
run_pipeline.py — Run the full pipeline locally (no Docker/Airflow needed)
Useful for testing, demos, and manual runs.

Usage:
    python run_pipeline.py                  # full pipeline
    python run_pipeline.py --step scrape    # scraping only
    python run_pipeline.py --step etl       # ETL only
    python run_pipeline.py --step match     # entity resolution only
    python run_pipeline.py --step dashboard # launch Streamlit dashboard
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RUNNER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent


def banner(text: str):
    line = "═" * (len(text) + 4)
    print(f"\n╔{line}╗")
    print(f"║  {text}  ║")
    print(f"╚{line}╝\n")


def step_scrape():
    banner("STEP 1 — WEB SCRAPING")

    from scrapers.ecommerce_scraper import run_pipeline

    df = run_pipeline(
        keywords=[
            "wireless earbuds",
            "smart watch",
            "men shirt",
            "women kurta",
            "electric drill",
            "screwdriver set",
        ],
        max_pages=2,
        output_dir="data/raw",
        platforms=["daraz", "amazon"],
    )

    log.info("✓ Total records scraped: %d", len(df))
    return df


def step_etl():
    banner("STEP 2 — ETL (CLEAN & NORMALIZE)")
    from knime_etl import run as run_etl
    df = run_etl(mode="latest")
    log.info("✓ ETL complete: %d clean records", len(df))
    log.info("Sources: %s", df["source"].value_counts().to_dict())
    return df


def step_match():
    banner("STEP 3 — ENTITY RESOLUTION & MATCHING")
    from knime_etl import run as run_match
    df = run_match()
    if not df.empty and "group_size" in df.columns:
        cross = df[df["group_size"] > 1]
        log.info("✓ Matching complete: %d cross-platform pairs", len(cross))
    return df


def step_dashboard():
    banner("STEP 4 — LAUNCH STREAMLIT DASHBOARD")
    log.info("Starting dashboard on http://localhost:8501 …")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(BASE_DIR / "dashboard" / "app.py")],
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Pipeline Runner")
    parser.add_argument(
        "--step",
        choices=["scrape", "etl", "match", "dashboard", "all"],
        default="all",
        help="Pipeline step to run (default: all)",
    )
    args = parser.parse_args()

    banner("Cross-Platform E-Commerce Price Intelligence Pipeline")
    log.info("Step: %s", args.step)

    if args.step in ("scrape", "all"):
        step_scrape()

    if args.step in ("etl", "all"):
        step_etl()

    if args.step in ("match", "all"):
        step_match()

    if args.step in ("dashboard", "all"):
        step_dashboard()

    if args.step == "all":
        banner("PIPELINE COMPLETE ✓")
        log.info("Dashboard → http://localhost:8501")
        log.info("Airflow   → http://localhost:8080  (admin / admin)")
        log.info("n8n       → http://localhost:5678  (admin / admin123)")


if __name__ == "__main__":
    main()
