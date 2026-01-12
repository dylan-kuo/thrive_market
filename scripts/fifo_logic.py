import duckdb
import pandas as pd
from collections import deque
import logging

logging.basicConfig(level=logging.INFO)
DB_PATH = '/opt/airflow/data/thrive.duckdb'

def run_fifo_matching():
    con = duckdb.connect(DB_PATH)
    
    # 1. Read Cleaned Staging Data
    logging.info("Reading staging data...")
    df = con.query("""
        SELECT trans_id, customer_id, trans_type, created_at, amount
        FROM stg_tc_data 
        WHERE trans_type IN ('earned', 'spent', 'expired')
        ORDER BY created_at ASC
    """).df()
    
    # 2. FIFO Algorithm
    customer_queues = {}
    results = []
    
    for _, row in df.iterrows():
        cust = row['customer_id']
        tid = row['trans_id']
        ttype = row['trans_type']
        created = row['created_at']
        amt = row['amount']
        
        if cust not in customer_queues:
            customer_queues[cust] = deque()
            
        redeem_id = None
        
        if ttype == 'earned':
            customer_queues[cust].append(tid)
        elif ttype in ['spent', 'expired']:
            if customer_queues[cust]:
                redeem_id = customer_queues[cust].popleft()
        
        results.append({
            'trans_id': tid, 
            'trans_type': ttype,
            'created_at': created,
            'customer_id': cust,
            'amount': amt,
            'redeem_id': redeem_id
        })

    # 3. Write Output to 'python_fifo_output'
    # This acts as the "Source" table for the downstream dbt model
    result_df = pd.DataFrame(results)
    logging.info("Writing results to 'python_fifo_output'...")
    con.execute("CREATE OR REPLACE TABLE python_fifo_output AS SELECT * FROM result_df")
    con.close()