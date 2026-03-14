# Notifications Guide

The pipeline sends SNS notifications on every load event (success, failure, or warning). This guide covers how to subscribe, what the messages look like, and how to add custom channels.

---

## How It Works

Each database pipeline has its own SNS topic:
- `etl-notifications-analytics-db` for ANALYTICS_DB loads
- `etl-notifications-reporting-db` for REPORTING_DB loads

The Lambda publishes to the topic after every COPY INTO operation. The message is dual-format:
- **Email subscribers** receive human-readable plaintext
- **SQS/Lambda/HTTPS subscribers** receive structured JSON

---

## Subscribing

### Email

The simplest way. Set the `notifications.email` field in `config.yaml`:

```yaml
notifications:
  email: team@example.com
```

Then regenerate and redeploy:
```bash
python generate.py
# Deploy the updated CloudFormation stack
```

Or subscribe manually via AWS Console:
1. Go to SNS > Topics > `etl-notifications-analytics-db`
2. Click "Create subscription"
3. Protocol: Email
4. Endpoint: your-email@example.com
5. Confirm the subscription email

### Slack (via AWS Chatbot)

1. Go to AWS Chatbot in the AWS Console
2. Configure a new Slack channel
3. Subscribe the Slack channel to your SNS topic
4. Pipeline notifications will appear in Slack with formatting

### PagerDuty / Opsgenie

1. Get the HTTPS endpoint URL from your incident management tool
2. Create an SNS subscription:
   ```bash
   aws sns subscribe \
     --topic-arn arn:aws:sns:us-east-1:152924643524:etl-notifications-analytics-db \
     --protocol https \
     --notification-endpoint https://events.pagerduty.com/integration/...
   ```

### SQS (for custom processing)

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:152924643524:etl-notifications-analytics-db \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:152924643524:my-processing-queue
```

### Lambda (for custom logic)

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:152924643524:etl-notifications-analytics-db \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:152924643524:function:my-custom-handler
```

---

## Message Format Reference

### SUCCESS

**Subject:** `[SUCCESS] Loaded CUSTOMERS - 1,000 rows`

**Email body:**
```
Pipeline Load Completed Successfully
=============================================

Table:        ANALYTICS_DB.CUSTOMERS
Source:        analytics/CUSTOMERS.T/data.csv
Rows loaded:  1,000
Rows parsed:  1,000
Duration:     5s
Load ID:      a1b2c3d4-...
```

**JSON (SQS/Lambda/HTTPS):**
```json
{
  "event_type": "SUCCESS",
  "timestamp": "2026-03-14T12:00:00.000Z",
  "load_id": "a1b2c3d4-...",
  "s3_key": "analytics/CUSTOMERS.T/data.csv",
  "target_database": "ANALYTICS_DB",
  "target_table": "CUSTOMERS",
  "rows_loaded": 1000,
  "rows_parsed": 1000,
  "duration_seconds": 5,
  "table_created": false
}
```

### FAILURE

**Subject:** `[FAILED] Load failed for CUSTOMERS`

**Email body:**
```
Pipeline Load FAILED
=============================================

Table:        ANALYTICS_DB.CUSTOMERS
Source:        analytics/CUSTOMERS.T/bad-data.csv
Error:        Table CUSTOMERS does not exist and auto_create_table is disabled
Load ID:      a1b2c3d4-...

Troubleshooting:
  1. Check CloudWatch Logs for the full stack trace
  2. Query ADMIN_DB._PIPELINE.LOAD_HISTORY for load_id = 'a1b2c3d4-...'
  3. Verify the source file format matches expectations
  4. Check ADMIN_DB._PIPELINE.CONTROL_TABLE for overrides
  5. Table may need to be created manually or auto_create_table enabled
```

**JSON (SQS/Lambda/HTTPS):**
```json
{
  "event_type": "FAILURE",
  "timestamp": "2026-03-14T12:00:00.000Z",
  "load_id": "a1b2c3d4-...",
  "s3_key": "analytics/CUSTOMERS.T/bad-data.csv",
  "target_database": "ANALYTICS_DB",
  "target_table": "CUSTOMERS",
  "error_message": "Table CUSTOMERS does not exist and auto_create_table is disabled"
}
```

### WARNING

**Subject:** `[WARNING] Partial Load — CUSTOMERS`

**Email body:**
```
Pipeline Load Warning
=============================================

Warning:      Partial Load
Table:        ANALYTICS_DB.CUSTOMERS
Source:        analytics/CUSTOMERS.T/messy-data.csv
Detail:       5 of 1005 rows rejected
Load ID:      a1b2c3d4-...
Rows loaded:  1,000
Errors seen:  5

Action required:
  - Review rejected rows in ADMIN_DB._PIPELINE.LOAD_ERRORS
  - Query: SELECT * FROM ADMIN_DB._PIPELINE.LOAD_ERRORS WHERE LOAD_ID = 'a1b2c3d4-...'
```

**Warning types:**
| Type | Trigger |
|------|---------|
| Partial Load | COPY INTO loaded some rows but rejected others |
| Format Mismatch | File extension doesn't match detected content type |
| Duplicate Detected | File with same ETag already loaded (future) |
| Schema Drift | Columns in file don't match existing table (future) |

---

## Notification Audit Trail

Every notification sent is logged to `ADMIN_DB._PIPELINE.NOTIFICATIONS`:

```sql
SELECT
    NOTIFICATION_TYPE,
    MESSAGE_SUBJECT,
    DELIVERY_STATUS,
    SENT_AT,
    SNS_MESSAGE_ID
FROM ADMIN_DB._PIPELINE.NOTIFICATIONS
WHERE LOAD_ID = 'a1b2c3d4-...'
ORDER BY SENT_AT DESC;
```

**Delivery statuses:**
- `SENT` — SNS accepted the message (doesn't guarantee email delivery)
- `SKIPPED` — SNS_TOPIC_ARN not configured (notifications disabled)
- `FAILED` — SNS publish call failed (check Lambda logs)

---

## Adding Custom Notification Channels

### Option 1: SNS Subscriptions

Add any protocol supported by SNS (email, SQS, Lambda, HTTPS, SMS). The structured JSON payload makes it easy to parse in automation.

### Option 2: Custom Lambda Subscriber

Create a Lambda function subscribed to the SNS topic that forwards to any service:

```python
def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        data = json.loads(message)  # Structured JSON

        if data['event_type'] == 'FAILURE':
            # Post to Microsoft Teams, Discord, etc.
            send_to_teams(data)
```

### Option 3: Extend notifier.py

Add a new notification function in `lambda/loader/notifier.py` for direct integration with services that don't support SNS (e.g., direct Slack webhook without AWS Chatbot).

---

## Disabling Notifications

- **Per-deployment:** Don't set `SNS_TOPIC_ARN` environment variable on the Lambda
- **Per-topic:** Delete the SNS topic (Lambda will log a warning and continue)
- **Per-subscriber:** Unsubscribe from the SNS topic in AWS Console
