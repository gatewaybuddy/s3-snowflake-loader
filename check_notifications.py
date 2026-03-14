#!/usr/bin/env python3
"""
Check the NOTIFICATIONS table structure and content.
"""

import snowflake.connector

# Snowflake connection parameters — use environment variables
import os
ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
USER = os.environ.get("SNOWFLAKE_USER", "")
TOKEN = os.environ.get("SNOWFLAKE_TOKEN", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "LOADING_WH")
ROLE = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

def connect_snowflake():
    """Connect to Snowflake using PAT (preferred) or password fallback"""
    params = dict(account=ACCOUNT, user=USER, warehouse=WAREHOUSE, role=ROLE)
    if TOKEN:
        params["token"] = TOKEN
        params["authenticator"] = "programmatic_access_token"
    elif PASSWORD:
        params["password"] = PASSWORD
    else:
        raise ValueError("Set SNOWFLAKE_TOKEN (PAT) or SNOWFLAKE_PASSWORD")
    return snowflake.connector.connect(**params)

def main():
    conn = connect_snowflake()
    cur = conn.cursor()
    
    try:
        # Check if notifications table exists and describe it
        print("Checking NOTIFICATIONS table structure...")
        cur.execute("DESCRIBE TABLE ADMIN_DB._PIPELINE.NOTIFICATIONS")
        columns = cur.fetchall()
        
        print(f"NOTIFICATIONS table has {len(columns)} columns:")
        for col in columns:
            print(f"  {col[0]} ({col[1]})")
        
        # Get sample records
        print(f"\nGetting sample records...")
        cur.execute("SELECT * FROM ADMIN_DB._PIPELINE.NOTIFICATIONS LIMIT 10")
        records = cur.fetchall()
        
        print(f"Found {len(records)} notification records")
        
        if records:
            column_names = [desc[0] for desc in cur.description]
            print(f"\nColumn names: {column_names}")
            
            for i, record in enumerate(records):
                print(f"\nRecord {i+1}:")
                for j, value in enumerate(record):
                    print(f"  {column_names[j]}: {value}")
                    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()