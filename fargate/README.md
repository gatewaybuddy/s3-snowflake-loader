# Fargate Module for Large File Processing

This module extends the S3-Snowflake loader to handle large files (20-50GB+) that exceed Lambda's processing capabilities due to `/tmp` storage limits (10GB) and execution timeout (15 minutes).

## When Fargate is Triggered vs Lambda

### Lambda Handles:
- Files < 5GB (configurable via `FARGATE_SIZE_THRESHOLD`)
- PGP-encrypted files < 1GB (configurable via `PGP_FARGATE_THRESHOLD`) 
- All files in `_chunks/` subdirectories (Fargate output)
- `_COMPLETE` and `_FAILED` markers (triggers COPY INTO or error handling)

### Fargate Handles:
- Files ≥ 5GB regardless of encryption
- PGP-encrypted files ≥ 1GB
- Any file that Lambda cannot process due to size/complexity

## Architecture Flow

### Large File Processing:
1. **S3 Event** → Lambda function
2. **Lambda** checks file size and PGP encryption
3. **If large**: Lambda triggers Fargate task and returns immediately
4. **Fargate** streams, decrypts, chunks, and uploads to `s3://bucket/path/_chunks/`
5. **Fargate** writes `_COMPLETE` marker when done
6. **S3 Event** → Lambda detects `_COMPLETE` marker
7. **Lambda** executes `COPY INTO` from all chunks

### Chunk Organization:
```
s3://bucket/data/table1/large-file.csv.pgp.gz
                        ↓ (Fargate processing)
s3://bucket/data/table1/_chunks/chunk-000001.csv.gz
s3://bucket/data/table1/_chunks/chunk-000002.csv.gz
s3://bucket/data/table1/_chunks/chunk-000003.csv.gz
s3://bucket/data/table1/_chunks/_manifest.json
s3://bucket/data/table1/_chunks/_COMPLETE
```

## Deployment

### 1. Deploy Infrastructure

```bash
aws cloudformation deploy \
  --template-file cloudformation.yaml \
  --stack-name s3-snowflake-fargate \
  --parameter-overrides \
    VpcId=vpc-12345678 \
    SubnetIds=subnet-12345678,subnet-87654321 \
    S3BucketArn=arn:aws:s3:::your-data-bucket \
    SnowflakeSecretArn=arn:aws:secretsmanager:region:account:secret:snowflake-config-abc123 \
    PGPSecretArn=arn:aws:secretsmanager:region:account:secret:pgp-keys-def456 \
  --capabilities CAPABILITY_NAMED_IAM
```

### 2. Build and Push Container Image

```bash
# Get ECR repository URI from CloudFormation output
ECR_REPO=$(aws cloudformation describe-stacks \
  --stack-name s3-snowflake-fargate \
  --query 'Stacks[0].Outputs[?OutputKey==`ECRRepositoryUri`].OutputValue' \
  --output text)

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_REPO

# Build and push image
docker build -t fargate-splitter .
docker tag fargate-splitter:latest $ECR_REPO:latest
docker push $ECR_REPO:latest
```

### 3. Update Lambda Configuration

Add these environment variables to your Lambda function:

```bash
FARGATE_CLUSTER_ARN=arn:aws:ecs:region:account:cluster/s3-snowflake-loader-fargate-cluster
FARGATE_TASK_DEFINITION_ARN=arn:aws:ecs:region:account:task-definition/s3-snowflake-loader-fargate-splitter
FARGATE_SUBNET_IDS=subnet-12345678,subnet-87654321
FARGATE_SECURITY_GROUP_IDS=sg-12345678
FARGATE_SIZE_THRESHOLD=5368709120          # 5GB in bytes
PGP_FARGATE_THRESHOLD=1073741824          # 1GB in bytes for PGP files
FARGATE_CHUNK_SIZE_MB=1024                # 1GB chunks
FARGATE_EXECUTE_COPY_INTO=false           # Let Lambda handle COPY INTO
```

### 4. Update Lambda IAM Role

Attach the Lambda Fargate invoke role or add these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask",
        "ecs:DescribeTasks"
      ],
      "Resource": [
        "arn:aws:ecs:*:*:task-definition/s3-snowflake-loader-fargate-splitter:*",
        "arn:aws:ecs:*:*:task/s3-snowflake-loader-fargate-cluster/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "ecs:DescribeClusters",
      "Resource": "arn:aws:ecs:*:*:cluster/s3-snowflake-loader-fargate-cluster"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::*:role/s3-snowflake-loader-fargate-execution-role",
        "arn:aws:iam::*:role/s3-snowflake-loader-fargate-task-role"
      ]
    }
  ]
}
```

## How Retry/Manifest Works

### Chunking Process:
1. **Stream Processing**: Files are never loaded fully into memory
2. **Line Boundary Splitting**: Chunks respect CSV/JSON line boundaries
3. **Header Preservation**: Each CSV chunk includes the header row
4. **Incremental Upload**: Chunks uploaded as created (not waiting for completion)
5. **Manifest Tracking**: `_manifest.json` updated after each chunk upload

### Retry Logic:
- **S3 Uploads**: 3 retries with exponential backoff per chunk
- **PGP Decryption**: 3 retries with different GPG flags on failure
- **Fargate Task**: Lambda retries Fargate trigger once, then falls back to Lambda processing
- **Individual Chunks**: Failed chunks marked in manifest, don't fail entire job

### Manifest Format:
```json
{
  "original_file": "data/table1/large-file.csv.gz",
  "original_size_bytes": 53687091200,
  "processing_started": "2023-10-15T14:30:00Z",
  "processing_completed": "2023-10-15T14:45:30Z",
  "chunk_size_bytes": 1073741824,
  "total_chunks": 52,
  "total_rows": 125000000,
  "total_size_bytes": 15728640000,
  "chunks": [
    {
      "chunk_name": "chunk-000001.csv.gz",
      "s3_key": "data/table1/_chunks/chunk-000001.csv.gz",
      "row_count": 2400000,
      "size_bytes": 301989888,
      "md5_hash": "d41d8cd98f00b204e9800998ecf8427e",
      "status": "UPLOADED"
    }
  ]
}
```

## Cost Estimates

### Fargate Pricing (us-east-1, as of 2023):
- **vCPU**: $0.04048/hour × 4 vCPUs = $0.162/hour
- **Memory**: $0.004445/GB/hour × 8GB = $0.036/hour
- **Ephemeral Storage**: $0.000111/GB/hour × 20GB = $0.002/hour
- **Total**: ~$0.20/hour

### Example Cost for 50GB File:
- **Processing Time**: ~15-20 minutes (depends on compression/complexity)
- **Fargate Cost**: $0.20 × (20/60) = ~$0.067
- **Data Transfer**: Negligible (same region S3 ↔ Fargate)
- **S3 Operations**: ~$0.005 (PUT requests for chunks)
- **Total**: ~$0.07 per 50GB file

### Cost Comparison:
- **Lambda**: Cannot process 50GB files (would timeout/fail)
- **EC2**: $0.096/hour (t3.large) but requires management/auto-scaling
- **Fargate**: $0.067 per file + no management overhead

## Configuration Reference

### Environment Variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | - | Source S3 bucket (set by Lambda) |
| `S3_KEY` | - | Source object key (set by Lambda) |
| `CHUNK_SIZE_MB` | 1024 | Chunk size in MB |
| `SECRET_NAME` | - | Snowflake config secret name |
| `PGP_SECRET_NAME` | - | PGP private key secret name |
| `EXECUTE_COPY_INTO` | false | Execute COPY INTO from Fargate |
| `DRY_RUN` | false | Simulate processing without uploads |

### Lambda Environment Variables:

| Variable | Description |
|----------|-------------|
| `FARGATE_CLUSTER_ARN` | ECS cluster ARN |
| `FARGATE_TASK_DEFINITION_ARN` | Task definition ARN |
| `FARGATE_SUBNET_IDS` | Comma-separated subnet IDs |
| `FARGATE_SECURITY_GROUP_IDS` | Comma-separated security group IDs |
| `FARGATE_SIZE_THRESHOLD` | File size threshold for Fargate (bytes) |
| `PGP_FARGATE_THRESHOLD` | PGP file size threshold (bytes) |
| `FARGATE_CHUNK_SIZE_MB` | Default chunk size (MB) |
| `FARGATE_EXECUTE_COPY_INTO` | Whether Fargate should execute COPY INTO |

## Monitoring

### CloudWatch Logs:
- **Log Group**: `/aws/ecs/s3-snowflake-loader-fargate-splitter`
- **Retention**: 30 days
- **Search Patterns**:
  - `"Processing complete"` - Successful completions
  - `"ERROR"` - Error messages
  - `"Chunk.*uploaded"` - Chunk upload progress

### CloudWatch Metrics:
- **ECS Service Metrics**: Task count, CPU/memory utilization
- **Custom Metrics**: Log-based metrics for processing time, chunk counts
- **Lambda Metrics**: Fargate trigger success/failure rates

### Troubleshooting:

1. **Fargate Task Fails to Start**:
   - Check subnet routing (NAT gateway for private subnets)
   - Verify security group allows HTTPS egress
   - Check IAM role permissions for secrets access

2. **PGP Decryption Fails**:
   - Verify PGP secret format in Secrets Manager
   - Check GPG installation in container
   - Validate private key can decrypt test files

3. **Chunks Upload but COPY INTO Fails**:
   - Check Snowflake connectivity from Lambda
   - Verify table schema matches CSV chunks
   - Check S3 file pattern in COPY statement

4. **High Processing Time**:
   - Increase Fargate CPU/memory allocation
   - Reduce chunk size for better parallelization
   - Check S3 request rate (may need request rate optimization)

## Security Considerations

- **PGP Keys**: Stored in AWS Secrets Manager with rotation capability
- **Network**: Fargate tasks run in private subnets with NAT gateway
- **IAM**: Least-privilege roles with specific resource ARNs
- **Container**: Non-root user, minimal base image, security scanning enabled
- **Data**: Temporary files cleaned up, no data persistence in containers
- **Audit**: All operations logged to CloudWatch with structured logging