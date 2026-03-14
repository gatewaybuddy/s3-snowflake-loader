# S3-Snowflake Loader Security Audit Report

**Date:** 2026-03-14  
**Project:** s3-snowflake-loader  
**Target:** AAA Enterprise Security Review  
**Auditor:** Kael (Subagent Security Review)

## Executive Summary

The s3-snowflake-loader pipeline demonstrates good architectural security principles with proper use of IAM roles, AWS Secrets Manager, and RSA key-pair authentication. However, critical vulnerabilities exist that must be addressed before enterprise deployment. Most concerning are hardcoded production credentials in debug scripts and overly broad S3 permissions in Lambda roles. The core ETL components follow security best practices, but the peripheral tooling exposes unacceptable risk. With the identified fixes, this pipeline can meet enterprise security standards.

## Security Findings

| Severity | Finding | Location | Status | Priority |
|----------|---------|----------|---------|----------|
| **CRITICAL** | Hardcoded production credentials in source code | `audit_check.py`, `check_notifications.py` | Open | P0 |
| **HIGH** | Lambda S3 permissions too broad (wildcard) | `templates/infrastructure.yaml.j2` | **FIXED** | P1 |
| **HIGH** | PGP private key caching without timeout | `lambda/loader/pgp_handler.py` | **FIXED** | P1 |
| **MEDIUM** | No CloudWatch log retention limits | `templates/infrastructure.yaml.j2` | **FIXED** | P2 |
| **MEDIUM** | Error messages may leak S3 key paths | `lambda/loader/handler.py`, `validator.py` | Open | P2 |
| **MEDIUM** | Dashboard uses password auth instead of RSA keys | `dashboard/app.py` | Open | P3 |
| **LOW** | No VPC configuration for Lambda | `templates/infrastructure.yaml.j2` | Acceptable | P3 |
| **LOW** | SNS topic not encrypted | `templates/infrastructure.yaml.j2` | **FIXED** | P3 |
| **INFO** | Manual CloudFormation template generation | `generate.py` | Acceptable | - |

## Detailed Findings

### CRITICAL: Hardcoded Production Credentials

**Files:** `audit_check.py`, `check_notifications.py`

```python
# audit_check.py (BEFORE fix — now uses env vars)
ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
USER = os.environ.get("SNOWFLAKE_USER")
# Previously hardcoded — now requires SNOWFLAKE_TOKEN or SNOWFLAKE_PASSWORD env var
```

**Impact:** Production Snowflake credentials committed to version control. Any developer with repository access can authenticate as ACCOUNTADMIN.

**Recommendation:** Immediately remove these files or move credentials to environment variables/AWS Secrets Manager. Rotate the exposed password.

**Enterprise Risk:** Fails SOX compliance, violates least-privilege, creates insider threat surface.

---

### HIGH: Overly Broad Lambda S3 Permissions

**File:** `templates/infrastructure.yaml.j2` lines ~150-160

**Status:** **FIXED** ✅

**Original Issue:** Lambda could access entire bucket, not just designated prefix. ListBucket permission on bucket root allowed enumeration of all keys.

**Resolution:** Updated CloudFormation template to implement prefix-based conditions:
```yaml
- Effect: Allow
  Action: s3:GetObject
  Resource: !Sub 'arn:aws:s3:::${S3BucketName}/${prefix}*'
- Effect: Allow  
  Action: s3:ListBucket
  Resource: !Sub 'arn:aws:s3:::${S3BucketName}'
  Condition:
    StringLike:
      's3:prefix': '${prefix}*'
```

**Impact:** Lambda functions now have least-privilege access to only their designated S3 prefix, preventing cross-database data access.

---

### HIGH: PGP Key Caching Without Timeout

**File:** `lambda/loader/pgp_handler.py` lines 15-17

**Status:** **FIXED** ✅

**Original Issue:** Private keys cached indefinitely in Lambda memory across invocations. If Lambda container is compromised, keys persist.

**Resolution:** Implemented 15-minute TTL for PGP key cache:
```python
_cached_pgp_key: Optional[str] = None
_cached_pgp_passphrase: Optional[str] = None
_cache_timestamp: Optional[float] = None
_CACHE_TTL_SECONDS = 15 * 60  # 15 minutes
```

**Impact:** PGP private keys now automatically expire from memory after 15 minutes, limiting exposure window while maintaining performance benefits of caching.

---

### MEDIUM: No CloudWatch Log Retention

**File:** `templates/infrastructure.yaml.j2`

**Status:** **FIXED** ✅

**Original Issue:** Lambda logs retained indefinitely by default. Potentially sensitive data in logs accumulates cost and compliance issues.

**Resolution:** Added explicit CloudWatch LogGroup with 30-day retention:

```yaml
LogGroup{{ safe }}:
  Type: AWS::Logs::LogGroup
  Properties:
    LogGroupName: !Sub '/aws/lambda/{{ naming.lambda_prefix }}-{{ db_lower }}-loader'
    RetentionInDays: 30
```

**Impact:** Lambda logs now automatically expire after 30 days, reducing storage costs and limiting exposure of potentially sensitive data in logs.

---

### MEDIUM: Error Messages Leak Path Information

**File:** `lambda/loader/validator.py` lines 67-71

```python
result.error = (
    f"File size ({size_gb:.2f} GB) exceeds maximum allowed size "
    f"({max_gb:.2f} GB). Adjust MAX_FILE_SIZE_BYTES to increase the limit."
)
```

**Issue:** Error messages sent to SNS may contain S3 key paths, exposing file structure to notification subscribers.

**Recommendation:** Sanitize error messages to remove paths while preserving troubleshooting value.

---

### MEDIUM: Dashboard Password Authentication  

**File:** `dashboard/app.py`

**Issue:** Dashboard uses password auth instead of RSA key-pair auth used by Lambda functions.

**Recommendation:** Align dashboard to use RSA key authentication for consistency and eliminate password attack surface.

---

### LOW: Lambda Not in VPC

**File:** `templates/infrastructure.yaml.j2`

**Issue:** Lambda functions run in AWS-managed VPC, not customer VPC. May conflict with enterprise network security policies.

**Status:** Acceptable for S3-to-Snowflake workload (both are internet services), but flag for enterprise network team review.

---

### LOW: SNS Topic Encryption

**File:** `templates/infrastructure.yaml.j2`

**Status:** **FIXED** ✅

**Original Issue:** SNS notification topics not encrypted at rest.

**Resolution:** Added KMS encryption using AWS-managed SNS key:

```yaml
NotificationTopic{{ safe }}:
  Type: AWS::SNS::Topic  
  Properties:
    TopicName: {{ naming.lambda_prefix }}-notifications-{{ db_lower }}
    DisplayName: 'ETL Pipeline - {{ db_name }}'
    KmsMasterKeyId: alias/aws/sns
```

**Impact:** SNS topics now encrypted at rest using AWS-managed keys, meeting enterprise encryption requirements.

## Architecture Security Assessment

### ✅ Strengths

1. **Proper IAM Role Separation**: Distinct roles for Lambda execution vs. Snowflake integration
2. **RSA Key-Pair Authentication**: No password-based Snowflake auth in production code
3. **AWS Secrets Manager Integration**: Credentials properly externalized
4. **Least-Privilege Snowflake RBAC**: Service users have minimal required grants
5. **Input Validation**: Comprehensive file validation before processing
6. **Audit Trail**: Full LOAD_HISTORY tracking in Snowflake
7. **PGP Decryption Support**: Proper handling of encrypted files
8. **Configuration Security**: `config.yaml` properly `.gitignore`d

### ⚠️ Areas for Improvement

1. **Secrets Rotation**: No automated rotation strategy documented
2. **Network Security**: No PrivateLink usage (internet-dependent)
3. **Dependency Scanning**: No automated security scanning of Python packages
4. **Error Handling**: Some error paths leak implementation details

## Security Controls Matrix

| Control Domain | Implementation | Status | Notes |
|----------------|----------------|---------|--------|
| **Authentication** | RSA Keys + Secrets Manager | ✅ Strong | Dashboard exception noted |
| **Authorization** | IAM + Snowflake RBAC | ⚠️ Good | Needs S3 condition tightening |
| **Encryption in Transit** | TLS (Snowflake + S3) | ✅ Strong | - |
| **Encryption at Rest** | S3 SSE + Snowflake | ✅ Strong | SNS encryption gap |
| **Input Validation** | File format + size validation | ✅ Strong | PGP, compression aware |
| **Logging** | CloudWatch + Snowflake | ⚠️ Good | No retention policy |
| **Secrets Management** | AWS Secrets Manager | ✅ Strong | Rotation strategy needed |
| **Network Security** | Internet-facing | ⚠️ Acceptable | No VPC/PrivateLink |
| **Audit Trail** | LOAD_HISTORY table | ✅ Strong | Full file lineage |
| **Error Handling** | Structured error codes | ⚠️ Good | Path disclosure risk |

## Recommendations by Priority

### P0 - Immediate (Before Any Deployment)
1. **Remove hardcoded credentials** from `audit_check.py` and `check_notifications.py`
2. **Rotate exposed Snowflake password** for user `YOUR_USERNAME`
3. **Review git history** for credential exposure in commits

### P1 - Before Enterprise Review
1. **Implement S3 prefix conditions** in Lambda IAM policies
2. **Add PGP key cache TTL** to prevent indefinite memory persistence
3. **Document secrets rotation procedures** and schedule

### P2 - Before Production
1. **Configure CloudWatch log retention** (recommend 30 days)
2. **Sanitize error messages** to prevent path disclosure in notifications
3. **Add SNS topic encryption** with KMS

### P3 - Enhancement
1. **Migrate dashboard to RSA authentication** for consistency
2. **Consider PrivateLink** for Snowflake connectivity (if available)
3. **Implement automated dependency scanning** in CI/CD pipeline

## Enterprise Readiness Checklist

### Security Requirements
- [ ] **Credentials Audit**: All hardcoded credentials removed
- [ ] **Least Privilege**: IAM policies scoped to minimum required access  
- [ ] **Encryption Standards**: All data encrypted in transit and at rest
- [ ] **Audit Logging**: Complete audit trail with appropriate retention
- [ ] **Secrets Management**: Centralized secrets with rotation strategy
- [ ] **Network Security**: VPC configuration documented and approved
- [ ] **Input Validation**: All user inputs validated and sanitized
- [ ] **Error Handling**: Error messages sanitized to prevent information disclosure

### Operational Requirements  
- [ ] **Monitoring**: CloudWatch dashboards and alerts configured
- [ ] **Backup/Recovery**: Data recovery procedures documented
- [ ] **Incident Response**: Security incident playbook created
- [ ] **Documentation**: All security controls documented
- [ ] **Training**: Operations team trained on security procedures

### Compliance Requirements
- [ ] **Access Control Matrix**: User roles and permissions documented
- [ ] **Data Classification**: PII/sensitive data handling documented
- [ ] **Change Management**: Security review process for code changes
- [ ] **Vulnerability Management**: Regular security scanning schedule
- [ ] **Business Continuity**: Disaster recovery plan including security controls

### Pre-Deployment Actions for AAA
1. **Security Design Review** with AAA Security Architecture team
2. **Penetration Testing** of deployed pipeline (recommend third-party)
3. **Compliance Audit** against AAA security standards 
4. **Incident Response Testing** with simulated security events
5. **Documentation Handoff** to AAA Operations team

## Conclusion

The s3-snowflake-loader pipeline has a fundamentally secure architecture with proper separation of concerns and modern security practices. The critical findings (hardcoded credentials) are easily remediated and appear to be development artifacts rather than design flaws. 

After addressing the P0 and P1 findings, this pipeline will meet enterprise security standards and can be confidently presented to AAA security teams. The core ETL logic demonstrates security-conscious development with input validation, proper authentication, and comprehensive audit trails.

**Recommendation**: Fix critical and high-severity findings, then proceed with AAA enterprise security review process.

---

**Report Generated:** 2026-03-14 by Kael Security Audit  
**Next Review:** After P0/P1 remediation complete