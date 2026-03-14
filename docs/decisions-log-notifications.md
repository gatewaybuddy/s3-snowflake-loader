# Decisions Log — Notifications & Dashboard

Record of design decisions for the feedback loop: SNS notifications, notification audit trail, and status dashboard.

---

## 2026-03-14: Feedback Loop Implementation

### D12: SNS Topic Per Database Pipeline

**Decision:** Create one SNS topic per database pipeline (`etl-notifications-{db_name}`) rather than a single shared topic.

**Reasoning:** Each database pipeline has different stakeholders. The analytics team cares about `ANALYTICS_DB` loads; the reporting team cares about `REPORTING_DB` loads. Per-database topics allow selective subscription — you only get alerts for the pipelines you own. A shared topic would force everyone to receive all notifications and filter client-side.

**Alternatives considered:**
- Single shared topic with message attributes for filtering (more complex, SNS filter policies are limited)
- One topic per table (too granular, topic proliferation)

---

### D13: Dual-Format SNS Messages (Email + JSON)

**Decision:** Use SNS MessageStructure `json` to send different formats per protocol. Email subscribers get human-readable plaintext. SQS/Lambda/HTTPS subscribers get structured JSON.

**Reasoning:** Email is the primary channel for human operators — they need readable messages with troubleshooting hints. Automation consumers (Slack bots, PagerDuty, custom alerting) need structured JSON they can parse. SNS natively supports per-protocol message formatting via `MessageStructure: json`, so we get both without sending two messages.

**Message structure:**
```json
{
  "default": "human-readable body",
  "email": "human-readable body",
  "sqs": "{\"event_type\": \"SUCCESS\", ...}",
  "lambda": "{\"event_type\": \"SUCCESS\", ...}",
  "https": "{\"event_type\": \"SUCCESS\", ...}"
}
```

---

### D14: Notifications as Non-Blocking Side Effect

**Decision:** Notification failures never block the load pipeline. All `notify_*` calls are wrapped in try/except in handler.py.

**Reasoning:** The primary mission is loading data into Snowflake. If SNS is down, misconfigured, or the topic doesn't exist yet, the load must still succeed. Notification is a best-effort side effect. The load record in LOAD_HISTORY is the source of truth; notifications are a convenience layer.

**Implementation:** Every notification call site in handler.py catches exceptions and logs them as errors without re-raising.

---

### D15: SNS_TOPIC_ARN as Environment Variable

**Decision:** The Lambda reads the SNS topic ARN from the `SNS_TOPIC_ARN` environment variable. If unset, notifications are silently skipped.

**Reasoning:** This allows gradual rollout — deploy the Lambda without the variable to keep existing behavior, then add the variable once the SNS topic is created. No code change or redeployment required to enable/disable notifications. The topic ARN is injected by CloudFormation (`!Ref NotificationTopic...`).

---

### D16: Three Notification Types — SUCCESS, FAILURE, WARNING

**Decision:** Emit three distinct notification types rather than a single "load complete" notification.

**Reasoning:**
- **SUCCESS**: Load completed with zero errors. Informational, low urgency.
- **FAILURE**: Load could not complete (exception thrown). High urgency, actionable.
- **WARNING**: Load completed but with issues:
  - `Partial Load`: Some rows rejected (status PARTIAL)
  - `Format Mismatch`: Extension doesn't match detected content type
  - Future: `Duplicate Detected`, `Schema Drift`

Each type includes context-specific troubleshooting hints. WARNING is especially useful because partial loads succeed (ON_ERROR = CONTINUE) but still need attention.

---

### D17: NOTIFICATIONS Audit Table in Snowflake

**Decision:** Log every notification sent to `ADMIN_DB._PIPELINE.NOTIFICATIONS` with delivery status and SNS message ID.

**Reasoning:** Audit trail for compliance and debugging. When someone asks "was I notified about this failure?", we can query the table. The `delivery_status` field tracks whether the notification was `SENT` (SNS accepted it), `SKIPPED` (topic not configured), or `FAILED` (SNS error). The `sns_message_id` enables correlation with CloudWatch Logs for delivery troubleshooting.

**Schema:**
| Column | Purpose |
|--------|---------|
| NOTIFICATION_ID | UUID primary key |
| LOAD_ID | FK to LOAD_HISTORY |
| NOTIFICATION_TYPE | SUCCESS / FAILURE / WARNING |
| CHANNEL | SNS (extensible to SLACK, PAGERDUTY) |
| RECIPIENT | Topic ARN or email (for future use) |
| MESSAGE_SUBJECT | SNS subject line |
| MESSAGE_BODY | Summary of what was sent |
| SENT_AT | When the notification was sent |
| DELIVERY_STATUS | SENT / SKIPPED / FAILED |
| SNS_MESSAGE_ID | For delivery troubleshooting |

---

### D18: Dashboard — Flask over FastAPI

**Decision:** Use Flask for the status dashboard rather than FastAPI.

**Reasoning:** Flask is the simpler choice for a read-only dashboard with server-rendered templates. The dashboard has exactly 5 routes (1 HTML page + 4 JSON API endpoints). FastAPI's async capabilities and auto-generated OpenAPI docs are unnecessary here. Flask has fewer dependencies and a smaller Docker image. The team is familiar with Flask.

**Alternatives considered:**
- FastAPI (async not needed for 5 read-only endpoints)
- Streamlit (too opinionated, harder to customize styling)
- Static HTML + cron-generated JSON (no real-time refresh)

---

### D19: Dashboard — Password Auth (Not RSA)

**Decision:** The dashboard connects to Snowflake using password authentication, not RSA key pairs.

**Reasoning:** The dashboard is a read-only monitoring tool, not the Lambda loader. Using the same RSA key auth would require packaging the private key into the dashboard container. Password auth via environment variables is simpler and sufficient for a dashboard that only runs SELECT queries. The dashboard user should have a dedicated Snowflake role with only SELECT grants on the `_PIPELINE` tables.

**Security note:** For production, add application-level auth (see dashboard-guide.md). The Snowflake password should be passed via environment variable, never hardcoded.

---

### D20: Dashboard — Read-Only Design Constraint

**Decision:** The dashboard NEVER writes to Snowflake or AWS. It is explicitly a window, not a control panel.

**Reasoning:** Read-only dashboards are safe to deploy without auth (for demo/testing). They can't accidentally modify data, trigger loads, or change configuration. This separation of concerns means the dashboard can be given to anyone without worrying about granting write access. The control panel (retry loads, modify control table) is planned for Tier 3.

**Implementation:** All queries are SELECT-only. No POST/PUT/DELETE endpoints exist. The Flask app has no write methods on the Snowflake connection.

---

### D21: Dashboard — Auto-Refresh at 30-Second Interval

**Decision:** The dashboard auto-refreshes every 30 seconds via client-side JavaScript polling.

**Reasoning:** The dashboard needs to show near-real-time pipeline status without manual page refreshes. 30 seconds balances freshness with Snowflake query cost (each refresh runs 2 queries against LOAD_HISTORY). WebSocket-based real-time updates would be overengineering for a monitoring dashboard.

**Alternatives considered:**
- Manual refresh only (poor UX for monitoring)
- WebSocket/SSE (unnecessary complexity)
- 5-second interval (too aggressive on Snowflake queries)
- 60-second interval (too stale for active monitoring)

---

### D22: Dashboard — Dark Theme

**Decision:** Default to a dark theme rather than light.

**Reasoning:** Dashboard will primarily be viewed on monitoring screens and by engineers. Dark themes reduce eye strain during extended monitoring sessions and look more professional on demo screens. Status colors (green/red/orange) have better contrast against dark backgrounds.

---

### D23: Notification Infrastructure in Main Stack

**Decision:** Add SNS resources (topic, subscription, IAM permission) to the main `infrastructure.yaml.j2` template rather than a separate `notifications.yaml` stack.

**Reasoning:** The SNS topic is tightly coupled to the Lambda function — the Lambda needs `sns:Publish` permission and the topic ARN as an environment variable. Keeping them in the same CloudFormation stack ensures they're created together and cross-references work naturally via `!Ref`. A separate stack would require stack exports/imports, adding complexity without benefit.

**Also created:** A standalone `notifications.yaml.j2` template for users who need to add notifications to an existing deployment without redeploying the full stack.

---

### D24: Troubleshooting Hints in Failure Notifications

**Decision:** Include context-specific troubleshooting steps in FAILURE notification bodies.

**Reasoning:** When someone receives a failure email at 2 AM, they shouldn't have to think about where to look first. The notification body includes:
1. Common first steps (check CloudWatch, query LOAD_HISTORY)
2. Context-specific hints based on error message keywords:
   - "does not exist" → check auto_create_table setting
   - "access"/"permission" → check IAM and Snowflake grants
   - "timeout" → increase Lambda timeout or split file

This reduces mean time to resolution (MTTR) for on-call engineers.
