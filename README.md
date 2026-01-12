# Thrive Market Assessment

A data engineering project for Thrive Market that processes Thrive Cash transactions and generates financial reports using dbt, DuckDB, and Apache Airflow.

## Project Structure

```
thrive_assessment/
├── dags/                          # Airflow DAGs
│   └── thrive_cash_processing_dag.py
├── dbt_project/                   # dbt project for data transformation
│   ├── models/
│   │   ├── staging/              # Raw data models
│   │   ├── intermediate/         # Transformed data (FIFO matching)
│   │   └── marts/                # Final business models
│   └── profiles.yml
├── scripts/                       # Python utility scripts
│   ├── ingest.py                 # Data ingestion logic
│   └── fifo_logic.py             # FIFO transaction matching
├── data/                          # Database files
│   └── thrive.duckdb
├── customer_balance_query.ipynb   # Jupyter notebook for balance queries
├── docker-compose.yml            # Docker services configuration
├── requirements.txt              # Python dependencies
└── .gitignore
```

## Setup

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- dbt-core

### Quick Start

1. **Start Docker services:**
```bash
docker compose up --build
```

2. **Trigger Airflow DAG:**
   - Navigate to `http://localhost:8080`
   - Log in and trigger the `thrive_cash_processing_dag`
   - This will run data ingestion and dbt transformations

3. **Set up Python environment and run business queries:**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
jupyter notebook
```
   - Open `customer_balance_query.ipynb` to run all business questions and view key metrics
   - This notebook contains queries for customer balances, transaction analysis, and financial insights


## Models Overview

### Staging Layer (stg_*)
- `stg_customers.sql` - Customer dimension data
- `stg_sales.sql` - Sales/order data
- `stg_tc_data.sql` - Thrive Cash transaction data

### Intermediate Layer (int_*)
- `int_fifo_matched.sql` - FIFO matched earned/redeemed transactions

### Marts Layer
- `finance_report.sql` - Customer balance snapshots with cumulative metrics

## Business Questions & Metrics

**Run `customer_balance_query.ipynb` to answer key business questions:**
- Customer Thrive Cash balances and transaction history
- Earned vs. Redeemed transaction analysis
- FIFO-matched transaction tracking
- Financial metrics and customer insights

Execute this notebook after data ingestion to view all results and metrics.

## Data Dictionary

See `dbt_project/models/*/schema.yml` for detailed column definitions.
