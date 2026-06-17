# Cross-Platform E-Commerce Price Intelligence System

## Overview

This project is a scalable data engineering pipeline that collects product information from multiple e-commerce platforms, cleans and standardizes the data using KNIME, performs cross-platform product matching through entity resolution, and identifies significant price changes for automated notifications.

The system is designed to support price intelligence, market analysis, and product comparison across multiple online marketplaces.

---

# Objectives

The project aims to:

* Collect product data from multiple e-commerce platforms.
* Standardize and clean inconsistent product information.
* Match identical products listed on different platforms.
* Monitor product price fluctuations over time.
* Generate automated alerts for significant price drops.
* Build a production-style ETL pipeline using modern data engineering tools.

---

# Supported Platforms

The system currently collects product information from:

* Daraz
* Amazon


---

# Technology Stack

| Component              | Technology                         |
| ---------------------- | ---------------------------------- |
| Web Scraping           | Python                             |
| Browser Automation     | Selenium                           |
| Data Processing        | Pandas                             |
| Workflow Orchestration | Apache Airflow                     |
| Data Cleaning          | KNIME                              |
| API Integration        | Flask                              |
| Product Matching       | Fuzzy Matching / Entity Resolution |
| Notifications          | n8n                                |
| Containerization       | Docker                             |

---

# System Architecture

```
                 +----------------------+
                 |      Airflow DAG     |
                 |   Pipeline Trigger   |
                 +----------+-----------+
                            |
                            v
      +------------------------------------------+
      | Unified Scraping Pipeline                |
      |------------------------------------------|
      | Ecommerce Scraper                            |                        |
      +------------------+-----------------------+
                         |
                         v
             Raw Product Dataset (CSV)
                         |
                         v
      +------------------------------------------+
      | Flask API Bridge                         |
      |------------------------------------------|
      | Receives Airflow Request                 |
      | Invokes KNIME Workflow                   |
      +------------------+-----------------------+
                         |
                         v
      +------------------------------------------+
      | KNIME Data Cleaning & Transformation     |
      +------------------+-----------------------+
                         |
                         v
                Cleaned Product Data
                         |
                         v
      +------------------------------------------+
      | KNIME ETL Pipeline               |
      |------------------------------------------|
      | Product Title Cleaning                   |
      | Similarity Matching                      |
      | Product Grouping                         |
      +------------------+-----------------------+
                         |
                         v
              Matched Product Dataset
                         |
                         v
      +------------------------------------------+
      | Price Monitoring Module                  |
      |------------------------------------------|
      | Historical Comparison                    |
      | Price Drop Detection                     |
      +------------------+-----------------------+
                         |
                         v
      +------------------------------------------+
      | n8n Notification Workflow                |
      |------------------------------------------|
      | Email Alerts                             |
      | Webhook Notifications                    |
      +------------------------------------------+
```

---

# Pipeline Workflow

## Step 1: Data Collection

Airflow triggers the unified scraping pipeline.

The scraper simultaneously collects product data from:

* Daraz
* Amazon

Collected attributes include:

* Product Name
* Price
* Rating
* Reviews
* Product URL
* Seller Information
* Platform Name

Output:

raw/ecommerce_data_TIMESTAMP.csv

---

## Step 2: Data Cleaning

Airflow sends the generated CSV file to the Flask API.

The Flask API triggers the KNIME workflow which:

* Removes duplicates
* Handles missing values
* Standardizes product titles
* Cleans price values
* Normalizes text fields

Output:

cleaned/ecommerce_clean.csv

---

## Step 3: Entity Resolution

The cleaned dataset is processed through the entity resolution engine.

Matching techniques include:

* String similarity
* Fuzzy matching
* Product title normalization
* Cross-platform comparison

Output:

matched/matched_TIMESTAMP.csv

---

## Step 4: Price Monitoring

The latest matched dataset is compared with previous runs.

The system identifies:

* Price reductions
* Percentage decrease
* Historical trends

Default Alert Rule:

Price Drop ≥ 10%

---

## Step 5: Notification Service

Detected price drops are sent to n8n.

n8n can trigger:

* Email alerts
* Slack notifications
* Webhook integrations
* Future dashboard updates

---

# Project Structure

project/

├── airflow_dags/
│   ├── ecommerce_pipeline_dag.py
│   └── flask_knime_api.py
│
├── scrapers/
│   └── ecommerce_scraper.py
│
├── etl/
│   ├── entity_resolution.py
│   └── knime_workflow.knwf
│
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── matched/
│
├── run_pipeline.py
│
├── requirements.txt
│
└── README.md

---

# Future Enhancements

* Real-time dashboard
* Machine learning based product matching
* Product recommendation system
* Price prediction model
* Additional e-commerce platform support
* Automated trend analysis

---

# Expected Outcomes

The system provides:

* Cross-platform product intelligence
* Automated ETL workflow
* Product matching across marketplaces
* Historical price tracking
* Automated price-drop notifications
* Scalable data engineering architecture
