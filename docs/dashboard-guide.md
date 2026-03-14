# Dashboard Guide

A lightweight, read-only status dashboard for monitoring the S3-to-Snowflake ETL pipeline. Displays recent loads, pipeline health metrics, and error details — all from `ADMIN_DB._PIPELINE.LOAD_HISTORY`.

**Important:** This dashboard is READ-ONLY. It never writes to Snowflake or AWS. It's a window, not a control panel.

---

## What It Shows

### Pipeline Health Summary (top cards)

Eight cards showing today's metrics:

| Card | Description |
|------|-------------|
| **Loads Today** | Total COPY INTO operations since midnight |
| **Success Rate** | Percentage of loads with SUCCESS or PARTIAL status |
| **Rows Loaded** | Total rows loaded across all tables today |
| **Avg Duration** | Average load duration in seconds |
| **Succeeded** | Count of fully successful loads |
| **Partial** | Count of loads with some rejected rows |
| **Failed** | Count of loads that threw exceptions |
| **Total Errors** | Total rejected rows across all loads today |

Color coding:
- Green: Success rate >= 95%
- Yellow: Success rate 80-94%
- Red: Success rate < 80%

### Recent Loads Table

The last 50 loads, sorted by start time (newest first):

| Column | Description |
|--------|-------------|
| **Status** | Color-coded badge: SUCCESS (green), PARTIAL (orange), FAILED (red), LOADING (purple) |
| **Table** | Target Snowflake table name |
| **Database** | Target database |
| **Source File** | Original file name from S3 |
| **Mode** | TRUNCATE, APPEND, or MERGE |
| **Rows** | Number of rows successfully loaded |
| **Errors** | Number of rejected rows (red if > 0) |
| **Duration** | Load time |
| **Started** | Start timestamp (shows time only for today, date + time for older) |

### Expandable Error Details

Click any row to expand and see:
- Load ID, full S3 key, file format detected
- Whether the table was auto-created
- Error message (for FAILED/PARTIAL loads)
- Rejected row details (line number, error message, record preview)

### Auto-Refresh

The dashboard auto-refreshes every 30 seconds. The green dot in the header pulses to indicate the refresh cycle. The "Updated" timestamp shows when data was last fetched.

---

## Running Locally

### Prerequisites

- Python 3.10+
- Snowflake account with read access to `ADMIN_DB._PIPELINE`

### Setup

```bash
cd dashboard

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Configure connection
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### Run

```bash
python app.py
```

Open http://localhost:5000 in your browser.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWFLAKE_ACCOUNT` | | Snowflake account identifier |
| `SNOWFLAKE_USER` | | Snowflake username |
| `SNOWFLAKE_PASSWORD` | | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | LOADING_WH | Warehouse for queries |
| `SNOWFLAKE_DATABASE` | ADMIN_DB | Database containing pipeline tables |
| `SNOWFLAKE_SCHEMA` | _PIPELINE | Schema containing LOAD_HISTORY |
| `SNOWFLAKE_ROLE` | (user default) | Optional role to use |

---

## Deploying with Docker

### Build

```bash
cd dashboard
docker build -t etl-dashboard .
```

### Run

```bash
docker run -d \
  --name etl-dashboard \
  -p 5000:5000 \
  -e SNOWFLAKE_ACCOUNT=YOUR-ACCOUNT-ID \
  -e SNOWFLAKE_USER=YOUR_USERNAME \
  -e SNOWFLAKE_PASSWORD='your-password-here' \
  -e SNOWFLAKE_WAREHOUSE=LOADING_WH \
  -e SNOWFLAKE_DATABASE=ADMIN_DB \
  -e SNOWFLAKE_SCHEMA=_PIPELINE \
  etl-dashboard
```

### Deploy to EC2

1. SSH into your EC2 instance
2. Install Docker if not already installed:
   ```bash
   sudo yum install -y docker
   sudo systemctl start docker
   sudo usermod -aG docker ec2-user
   ```
3. Copy the `dashboard/` directory to the instance
4. Build and run:
   ```bash
   cd dashboard
   docker build -t etl-dashboard .
   docker run -d --restart unless-stopped \
     --name etl-dashboard \
     -p 5000:5000 \
     -e SNOWFLAKE_ACCOUNT=YOUR-ACCOUNT-ID \
     -e SNOWFLAKE_USER=YOUR_USERNAME \
     -e SNOWFLAKE_PASSWORD='your-password-here' \
     etl-dashboard
   ```
5. Open security group port 5000 (or use nginx reverse proxy on port 80)

### Docker Compose (optional)

```yaml
version: '3.8'
services:
  dashboard:
    build: ./dashboard
    ports:
      - "5000:5000"
    environment:
      - SNOWFLAKE_ACCOUNT=YOUR-ACCOUNT-ID
      - SNOWFLAKE_USER=YOUR_USERNAME
      - SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
    restart: unless-stopped
```

---

## API Endpoints

All endpoints return JSON. Use them for custom integrations.

| Endpoint | Description |
|----------|-------------|
| `GET /` | HTML dashboard page |
| `GET /api/recent-loads` | Last 50 loads with full details |
| `GET /api/health-summary` | Today's aggregate metrics |
| `GET /api/load-errors/<load_id>` | Rejected rows for a specific load |
| `GET /api/notifications/<load_id>` | Notification history for a load |

---

## Production Considerations

### Authentication

The dashboard has NO authentication by default. For production:

1. **nginx basic auth** (simplest):
   ```nginx
   location / {
       auth_basic "ETL Dashboard";
       auth_basic_user_file /etc/nginx/.htpasswd;
       proxy_pass http://localhost:5000;
   }
   ```

2. **AWS ALB + Cognito**: Put the dashboard behind an Application Load Balancer with Cognito authentication.

3. **IP whitelisting**: Restrict the security group to office/VPN IP ranges.

### Dedicated Snowflake User

Create a read-only user for the dashboard:

```sql
CREATE ROLE DASHBOARD_READER;
GRANT USAGE ON DATABASE ADMIN_DB TO ROLE DASHBOARD_READER;
GRANT USAGE ON SCHEMA ADMIN_DB._PIPELINE TO ROLE DASHBOARD_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA ADMIN_DB._PIPELINE TO ROLE DASHBOARD_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ADMIN_DB._PIPELINE TO ROLE DASHBOARD_READER;
GRANT USAGE ON WAREHOUSE LOADING_WH TO ROLE DASHBOARD_READER;

CREATE USER DASHBOARD_USER
    PASSWORD = 'strong-password-here'
    DEFAULT_ROLE = DASHBOARD_READER
    DEFAULT_WAREHOUSE = LOADING_WH;
GRANT ROLE DASHBOARD_READER TO USER DASHBOARD_USER;
```

### HTTPS

Use an nginx reverse proxy or AWS ALB for TLS termination. Never expose the Flask dev server directly to the internet.
