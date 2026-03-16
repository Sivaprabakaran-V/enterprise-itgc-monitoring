# Bronze Layer: Data Ingestion & Security Logging

## 📌 Overview
The **Bronze Layer** represents the raw data ingestion tier of the Enterprise ITGC Monitoring platform. Its primary responsibility is to capture and preserve operational logs from enterprise systems without modification, ensuring that original security evidence remains intact.

This layer serves as the foundation for downstream analytics and security control validation implemented in the Silver and Gold layers.

---

## 🛡️ Security Perspective: Log Fidelity
Log fidelity is a fundamental requirement in cybersecurity monitoring and forensic investigations. The Bronze layer preserves this by ensuring:

* **Zero Transformation:** No enrichment occurs during ingestion.
* **Timestamp Integrity:** Original timestamps and identifiers are retained.
* **Immutability:** Source records remain unchanged once ingested.
* **Full Traceability:** Raw events can always be traced back to their originating system.

### Critical Use Cases
Maintaining raw logs is essential for:
* Security investigations & Incident response.
* Compliance audits (ISO 27001, SOC 2, NIST).
* Root cause analysis and regulatory evidence preservation.

---

## 📊 Data Source Selection & Strategy
Each dataset in this project represents a critical security control domain within the IT General Controls (ITGC) framework.

| Log Source | Purpose | Security Relevance |
| :--- | :--- | :--- |
| **HR Employee Records** | Authoritative source of truth for staff. | Detects orphaned or unauthorized accounts. |
| **Identity Provider (AD/Azure)** | Monitors authentication and identity systems. | Tracks user provisioning and access lifecycle. |
| **Privileged Accounts** | Monitors high-risk admin access. | Detects unauthorized elevation of privileges. |
| **Change Management** | Tracks ITSM approved change requests. | Ensures production changes are authorized. |
| **System Deployment Logs** | Monitors system-level modifications. | Detects "Shadow IT" or unauthorized deployments. |
| **Backup Status Logs** | Monitors operational backup activity. | Ensures business continuity and data recoverability. |

---

## ⚙️ Data Engineering Design


### Schema-on-Read Design
The Bronze layer follows a **schema-on-read** architecture, meaning raw data is ingested without enforcing strict constraints immediately.
* **Flexibility:** Easily ingest heterogeneous log sources.
* **Evolution:** Supports semi-structured or changing log formats.
* **Reprocessing:** Ability to re-run pipelines if schema requirements change in Silver/Gold.

### Handling Semi-Structured Data
Operational logs often contain nested fields and inconsistent formats. By storing raw records:
1. No valuable information is discarded.
2. Future transformations have access to the full event context.
3. Data lineage is preserved for audit trails.

### Reliability & Anti-Loss Principles
* **Immutable Raw Storage:** Logs are stored as append-only datasets.
* **Idempotent Ingestion:** Pipelines can be safely retried without duplicating or corrupting records.
* **Lineage Traceability:** Every record maintains a reference to its original source system.

---

## 🔄 Role in the Medallion Architecture
The Bronze layer is the first step in a three-tier transformation process:

1.  **Bronze:** Raw ingestion and log preservation.
2.  **Silver:** Data standardization and relationship modeling.
3.  **Gold:** Security control monitoring and ITGC validation.

---

## 🗝️ Key Takeaways
* **Security Objective:** Preserve log fidelity, maintain forensic evidence, and enable traceability.
* **Engineering Objective:** Enable scalable ingestion of diverse sources and support flexible data modeling.
