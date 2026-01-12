# Technical Explanation - Thrive Market Assessment

## Architecture Overview

This project implements a modern data stack for processing Thrive Cash (TC) transactions and generating financial reports. The architecture follows the medallion/lakehouse pattern with staging, intermediate, and marts layers.

## Data Flow

```
Raw Data (DuckDB) 
    ↓
Staging Layer (stg_*)
    ├── stg_tc_data (transactions)
    ├── stg_customers
    └── stg_sales
    ↓
Intermediate Layer (int_*)
    └── int_fifo_matched (FIFO matching logic)
    ↓
Marts Layer
    └── finance_report (business-ready balances)
    ↓
Reporting (Jupyter Notebooks, BI Tools)
```

## Key Components

### 1. Data Ingestion (`scripts/ingest.py`)
- Loads raw data into DuckDB database
- Performs initial data validation
- Handles incremental loads

### 2. FIFO Matching Logic (`scripts/fifo_logic.py`)
- Implements First-In-First-Out matching algorithm
- Matches earned Thrive Cash transactions with redemptions
- Calculates expiration dates and expired balances

### 3. dbt Transformations

#### Staging Layer
- Minimal transformations applied
- Column renaming and type casting
- Source data validation

#### Intermediate Layer (`int_fifo_matched`)
- Core business logic: FIFO matching
- Tracks which redemption matches which earned TC
- Handles expired balances

#### Marts Layer (`finance_report`)
- Rolling balance calculations
- Cumulative metrics:
  - `cumulative_earned` - Total TC earned by date
  - `cumulative_spent` - Total TC redeemed by date
  - `cumulative_expired` - Total TC expired by date
  - `current_balance` - Running balance (earned - spent - expired)
- One row per customer per transaction date

### 4. Airflow Orchestration (`dags/thrive_cash_processing_dag.py`)
- Schedules data ingestion
- Triggers dbt transformation runs
- Handles dependencies and error handling
- Maintains data freshness

## Database Design

### Key Tables

**stg_tc_data**
- `trans_id` - Unique transaction identifier
- `trans_type` - 'EARN' or 'REDEEM'
- `created_at` - Transaction creation date
- `expired_at` - Expiration date (for earned TC)
- `customer_id` - Customer identifier
- `amount` - TC amount
- `reason` - Transaction reason

**int_fifo_matched**
- Extends stg_tc_data with FIFO matching results
- `redeem_id` - Links redemptions to earned transactions
- Enables tracking of TC lifecycle

**finance_report**
- Aggregated customer balances
- Snapshot per customer per date
- Enables historical balance queries

## Query Examples

### Get Current Balance for Customer
```sql
SELECT current_balance 
FROM finance_report 
WHERE customer_id = 23306353
  AND transaction_date = CURRENT_DATE
QUALIFY ROW_NUMBER() OVER (ORDER BY transaction_date DESC) = 1
```

### Get Historical Balance at Specific Date
```sql
SELECT customer_id, transaction_date, current_balance
FROM finance_report
WHERE customer_id IN (23306353, 16161481)
  AND transaction_date <= '2023-03-21'
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date DESC) = 1
```

### Track TC Lifecycle
```sql
SELECT 
  earned.trans_id as earned_id,
  earned.amount as earned_amount,
  redeemed.trans_id as redeemed_id,
  redeemed.amount as redeemed_amount,
  earned.expired_at
FROM int_fifo_matched earned
LEFT JOIN int_fifo_matched redeemed 
  ON earned.redeem_id = redeemed.trans_id
WHERE earned.trans_type = 'EARN'
```

## Testing Strategy

dbt tests are defined in `schema.yml` files:
- **Uniqueness tests** - Ensures key uniqueness
- **Not null tests** - Validates required columns
- **Referential integrity tests** - Validates foreign keys
- **Custom SQL tests** - Business logic validation

Run tests:
```bash
dbt test
```

## Performance Considerations

- DuckDB is used for analytical queries (fast for columnar operations)
- FIFO matching is computed once during dbt transformation
- Finance report provides pre-aggregated snapshots
- Incremental models reduce compute time for repeated runs

## Development Workflow

1. **Update source data** → `scripts/ingest.py`
2. **Modify staging models** → `dbt_project/models/staging/`
3. **Update transformation logic** → `dbt_project/models/intermediate/` or `scripts/fifo_logic.py`
4. **Rebuild marts** → `dbt_project/models/marts/`
5. **Run tests** → `dbt test`
6. **Query results** → `customer_balance_query.ipynb`

## Troubleshooting

### Data Quality Issues
- Check `dbt test` output for failing tests
- Review data freshness in staging tables
- Validate FIFO matching logic with sample data

### Performance Issues
- Monitor DuckDB memory usage
- Check for large incremental loads
- Review dbt compilation logs for inefficient queries

### Historical Balance Queries
- Use the parameterized query in `customer_balance_query.ipynb`
- Adjust `target_date` parameter to query any historical point
- The QUALIFY clause ensures latest balance on or before that date
