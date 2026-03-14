#!/usr/bin/env python3
"""
Audit checker for ETL pipeline stress testing.
Checks LOAD_HISTORY and NOTIFICATIONS tables in Snowflake.
"""

import snowflake.connector
import sys
from datetime import datetime

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

def check_load_history():
    """Check LOAD_HISTORY audit table"""
    print("=" * 60)
    print("CHECKING LOAD_HISTORY TABLE")
    print("=" * 60)
    
    conn = connect_snowflake()
    cur = conn.cursor()
    
    try:
        # Get all records from LOAD_HISTORY
        cur.execute("""
            SELECT 
                LOAD_ID,
                S3_BUCKET,
                S3_KEY,
                TARGET_DATABASE,
                TARGET_SCHEMA, 
                TARGET_TABLE,
                STATUS,
                ROWS_LOADED,
                ROWS_PARSED,
                ERRORS_SEEN,
                STARTED_AT,
                COMPLETED_AT,
                DURATION_SECONDS,
                ERROR_MESSAGE
            FROM ADMIN_DB._PIPELINE.LOAD_HISTORY
            ORDER BY STARTED_AT DESC
        """)
        
        records = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        
        print(f"\nTotal Load History Records: {len(records)}")
        
        if len(records) == 0:
            print("No load history records found.")
            return
        
        # Summary statistics
        status_counts = {}
        total_rows_loaded = 0
        total_errors = 0
        total_duration = 0
        successful_loads = 0
        
        print("\nDetailed Load History:")
        print("-" * 120)
        print(f"{'LOAD_ID':<36} {'TABLE':<20} {'STATUS':<12} {'ROWS':<10} {'ERRORS':<8} {'DURATION':<10} {'STARTED_AT':<20}")
        print("-" * 120)
        
        for record in records:
            load_id = record[0]
            table = f"{record[4]}.{record[5]}" if record[4] and record[5] else "N/A"
            status = record[6]
            rows_loaded = record[7] or 0
            errors_seen = record[9] or 0
            duration = record[12] or 0
            started_at = record[10].strftime('%Y-%m-%d %H:%M:%S') if record[10] else "N/A"
            
            print(f"{load_id:<36} {table:<20} {status:<12} {rows_loaded:<10} {errors_seen:<8} {duration:<10.2f} {started_at:<20}")
            
            # Accumulate stats
            status_counts[status] = status_counts.get(status, 0) + 1
            total_rows_loaded += rows_loaded
            total_errors += errors_seen
            total_duration += duration
            
            if status == 'SUCCESS':
                successful_loads += 1
        
        print("-" * 120)
        print(f"\nSUMMARY STATISTICS:")
        print(f"Total Loads: {len(records)}")
        print(f"Successful Loads: {successful_loads}")
        print(f"Status Breakdown:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
        print(f"Total Rows Loaded: {total_rows_loaded:,}")
        print(f"Total Errors: {total_errors}")
        print(f"Total Duration: {total_duration:.2f} seconds")
        print(f"Average Duration: {total_duration/len(records):.2f} seconds per load")
        
        # Show any failed loads
        failed_loads = [r for r in records if r[6] != 'SUCCESS']
        if failed_loads:
            print(f"\nFAILED LOADS ({len(failed_loads)}):")
            for record in failed_loads:
                load_id = record[0]
                table = f"{record[4]}.{record[5]}" if record[4] and record[5] else "N/A"
                error_msg = record[13] or "No error message"
                print(f"  {load_id} - {table}: {error_msg}")
        
    except Exception as e:
        print(f"Error checking LOAD_HISTORY: {e}")
    finally:
        cur.close()
        conn.close()

def check_notifications():
    """Check NOTIFICATIONS table"""
    print("\n" + "=" * 60)
    print("CHECKING NOTIFICATIONS TABLE")
    print("=" * 60)
    
    conn = connect_snowflake()
    cur = conn.cursor()
    
    try:
        # Check if notifications table exists
        cur.execute("""
            SELECT COUNT(*) FROM ADMIN_DB.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '_PIPELINE' 
            AND TABLE_NAME = 'NOTIFICATIONS'
        """)
        
        table_exists = cur.fetchone()[0] > 0
        
        if not table_exists:
            print("NOTIFICATIONS table does not exist yet.")
            return
            
        # Get all notification records
        cur.execute("""
            SELECT 
                NOTIFICATION_ID,
                LOAD_ID,
                NOTIFICATION_TYPE,
                RECIPIENT,
                MESSAGE,
                STATUS,
                SENT_AT,
                ERROR_MESSAGE
            FROM ADMIN_DB._PIPELINE.NOTIFICATIONS
            ORDER BY SENT_AT DESC
        """)
        
        records = cur.fetchall()
        
        print(f"\nTotal Notification Records: {len(records)}")
        
        if len(records) == 0:
            print("No notification records found.")
            return
        
        print(f"\nNotification Details:")
        print("-" * 100)
        print(f"{'NOTIFICATION_ID':<36} {'TYPE':<15} {'RECIPIENT':<25} {'STATUS':<10} {'SENT_AT':<20}")
        print("-" * 100)
        
        notification_types = {}
        status_counts = {}
        
        for record in records:
            notif_id = record[0]
            notif_type = record[2]
            recipient = record[3]
            status = record[5]
            sent_at = record[6].strftime('%Y-%m-%d %H:%M:%S') if record[6] else "N/A"
            
            print(f"{notif_id:<36} {notif_type:<15} {recipient:<25} {status:<10} {sent_at:<20}")
            
            notification_types[notif_type] = notification_types.get(notif_type, 0) + 1
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print("-" * 100)
        print(f"\nNOTIFICATION SUMMARY:")
        print(f"Total Notifications: {len(records)}")
        print(f"Notification Types:")
        for ntype, count in notification_types.items():
            print(f"  {ntype}: {count}")
        print(f"Status Breakdown:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
            
    except Exception as e:
        print(f"Error checking NOTIFICATIONS: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    print("ETL Pipeline Audit Check")
    print("=" * 60)
    print(f"Connecting to Snowflake account: {ACCOUNT}")
    print(f"User: {USER}")
    print(f"Warehouse: {WAREHOUSE}")
    print(f"Role: {ROLE}")
    
    check_load_history()
    check_notifications()

if __name__ == "__main__":
    main()