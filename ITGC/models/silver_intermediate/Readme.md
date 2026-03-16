# Silver Layer: Data Cleansing, Normalization & Control Logic Preparation

## 🔍 Overview
The **Silver Layer** is the transformation engine of the Enterprise ITGC Monitoring platform. While the Bronze layer preserves raw logs, the Silver layer converts them into structured, high-fidelity datasets. 

This stage applies controlled transformations to improve data quality and consistency, ensuring that downstream **Gold Layer** automated controls operate on a "Clean Source of Truth."

---

## 🏗️ Medallion Architecture Role
The platform follows a structured data progression:
1.  **Bronze:** Raw log ingestion and evidence preservation.
2.  **Silver:** **Data cleansing, normalization, and correlation.**
3.  **Gold:** Control validation and automated security monitoring.

### Data Lineage Overview
The following flow illustrates how raw Bronze datasets are transformed into standardized intermediate models:



---

## 🛡️ Security Perspective
### Preparing Data for Control Validation
Raw logs are often insufficient for security audits due to mismatched identifiers and inconsistent timestamps. The Silver layer bridges this gap by:
* **Identity Alignment:** Linking disparate IDs across HR and Identity Providers.
* **Timestamp Normalization:** Converting all system logs to a unified UTC format for accurate forensics.
* **Filtering:** Removing unusable or corrupt records that could trigger false positives.

### Identity Correlation
By aligning HR records, IdP accounts, and infrastructure privileges, we enable the detection of:
* **Orphaned Accounts:** Active accounts with no corresponding active employee.
* **Unauthorized Privileges:** Admin access not mapped to an authorized user profile.
* **Identity Inconsistency:** Mismatched attributes across enterprise systems.

---

## ⚙️ Data Engineering Perspective
### 1. Data Normalization
Standardizes formats to ensure records can be joined reliably.
* **Example:** Normalizing Employee IDs like `EMP_1001`, `1001`, and `emp-1001` into a single canonical format.

### 2. Deduplication
Prevents inflated event counts and misleading metrics by:
* Identifying unique event UUIDs.
* Selecting the "Latest" record based on modified timestamps.

### 3. Handling Nulls & Missing Values
Ensures data integrity by:
* Replacing nulls with standardized placeholders (e.g., `N/A` or `Unknown`).
* Filtering records that lack critical security context.

---

## 📋 Intermediate Models (Silver Tables)

### `int_identity_users`
**Purpose:** Standardizes user identity across HR and IdP datasets.
* **Security Value:** Detects orphaned accounts and inactive employees with active access.

### `int_admin_accounts`
**Purpose:** Prepares privileged account datasets for monitoring.
* **Security Value:** Maps privileged accounts to actual employees to identify "Ghost Admins."

### `int_change_validation`
**Purpose:** Validates production deployments against ITSM change requests.
* **Security Value:** Detects unauthorized system changes and policy violations.

### `int_backup_monitoring`
**Purpose:** Standardizes backup job records.
* **Security Value:** Monitors for failed jobs and backup SLA violations.

---

## 🗝️ Key Takeaways
| Objective | Focus Area |
| :--- | :--- |
| **Security** | Prepare datasets for automated ITGC validation and anomaly detection. |
| **Engineering** | Normalize structures, remove duplicates, and standardize timestamps. |

**The Silver Layer ensures the ITGC monitoring platform operates on clean, consistent, and trustworthy datasets.**
