# Data Quality & Control Validation Tests

## Overview

The `tests` folder contains automated validation checks used to ensure the reliability, integrity, and compliance of datasets produced in the Enterprise ITGC Monitoring platform.

These tests serve two primary purposes:

1. **Data Quality Assurance**
   Ensuring that transformed datasets meet expected structural and logical standards.

2. **Security Control Validation**
   Verifying that IT General Controls (ITGC) are functioning correctly and detecting violations when they occur.

By integrating automated tests into the data pipeline, the platform ensures that **security monitoring logic operates on accurate and trustworthy datasets**.

---

# Why Testing is Important

In data-driven security monitoring platforms, incorrect or incomplete data can lead to:

* false security alerts
* missed control violations
* inaccurate compliance reporting
* unreliable audit evidence

Automated testing helps prevent these issues by validating data at multiple stages of the pipeline.

Testing enables the platform to detect:

* missing or invalid records
* duplicate entries
* broken relationships between datasets
* violations of expected control conditions

---

# Types of Tests Used

The project uses a combination of **data quality tests** and **control validation tests**.

## 1. Schema Tests

Schema tests validate structural integrity of datasets.

Examples include:

* **Not Null Tests**
  Ensure critical columns such as identifiers and timestamps are always populated.

* **Unique Tests**
  Validate that primary identifiers such as event IDs or control results are unique.

* **Accepted Values Tests**
  Ensure status fields contain only expected values.

These tests help maintain **dataset reliability and structural consistency**.

---

## 2. Relationship Tests

Relationship tests validate dependencies between datasets.

Example checks include:

* verifying that privileged accounts correspond to valid employees
* ensuring deployment records reference valid change tickets
* validating identity mappings across systems

These tests ensure that **cross-system correlations used in security monitoring are valid**.

---

## 3. Custom Control Tests

In addition to standard schema tests, this project implements **custom tests designed to detect ITGC control violations**.

These tests simulate real-world security monitoring logic by identifying events that violate defined control policies.

Examples include:

Unauthorized privileged access
Production deployments without approved change requests
Backup failures exceeding remediation thresholds

When such conditions occur, the tests produce results that represent **control exceptions requiring investigation**.

---

# Example Control Validation Test

The following example detects **critical control exceptions that remain unresolved beyond their remediation SLA**.

```sql
select
    test_result_id,
    control_id,
    domain,
    finding_detail,
    days_open,
    remediation_sla_days
from {{ ref('mart_control_exceptions') }}
where severity = 'Critical'
and remediation_sla_breached = true
```

Purpose of this test:

* identify high-risk control violations
* highlight issues that require immediate remediation
* support compliance reporting

If this query returns any rows, it indicates that a **critical control exception has exceeded its remediation timeline**, which should not occur in a well-controlled environment.

---

# Integration with the Data Pipeline

Testing is integrated directly into the data transformation workflow.

Execution flow:

Bronze Layer → Raw log ingestion
Silver Layer → Data cleansing and normalization
Gold Layer → Control monitoring datasets
Tests → Data validation and control verification

Running the tests ensures that:

* datasets meet expected quality standards
* control monitoring logic operates correctly
* security violations are properly detected

---

# Security Monitoring Value

Automated testing plays a critical role in maintaining **continuous compliance monitoring**.

Security benefits include:

* early detection of control failures
* improved reliability of compliance reports
* faster identification of operational risks
* stronger audit evidence

This approach helps organizations transition from **periodic audit testing** to **continuous control monitoring**.

---

# Key Takeaways

The tests implemented in this project ensure that the ITGC monitoring system remains both **technically reliable and security-focused**.

Data Engineering Benefits

* ensures data quality and consistency
* prevents pipeline errors from propagating
* validates transformation logic

Security Benefits

* verifies that security controls are functioning correctly
* detects violations automatically
* supports continuous compliance monitoring

By combining **data validation and security control testing**, the platform ensures that the analytics pipeline produces **accurate, reliable, and audit-ready security insights**.
