# Snowflake Infrastructure Setup

## Overview

This document describes the Snowflake infrastructure used in the Enterprise ITGC Monitoring platform.

The goal of this setup is to create a **scalable and secure data ingestion environment** that loads operational security logs from cloud storage into Snowflake for downstream processing using dbt.

The infrastructure supports ingestion of enterprise operational logs used for **IT General Controls (ITGC) monitoring and compliance analytics**.

---

# Architecture Overview

The ingestion pipeline follows this flow:

```text
Operational Log Files
        │
        ▼
AWS S3 Bucket
        │
        ▼
Snowflake External Stage
        │
        ▼
Snowflake Staging Tables
        │
        ▼
dbt Transformations
(Bronze → Silver → Gold)
```

This design separates **data ingestion, transformation, and analytics layers**, enabling scalable security monitoring.

---

# Snowflake Warehouse Configuration

A dedicated Snowflake warehouse is created to execute data ingestion and transformation workloads.

Purpose of the warehouse:

* run ingestion queries
* support dbt transformations
* execute analytical queries

Example configuration:

```sql
CREATE OR REPLACE WAREHOUSE ITGC
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

Key design considerations:

* **Auto suspend** reduces compute cost
* **Auto resume** ensures workloads start automatically
* **Small warehouse size** is sufficient for log ingestion workloads

---

# Database and Schema Structure

The project uses the following logical structure:

```text
DATABASE: ITGC

SCHEMAS

staging      → raw ingested data
bronze       → raw data modeled for ingestion layer
silver       → standardized datasets
gold         → compliance monitoring tables
```

The staging schema stores raw tables loaded directly from cloud storage.

---

# External Storage Integration

Operational log files are stored in **AWS S3** and accessed securely using a Snowflake storage integration.

Purpose of the integration:

* allow Snowflake to read files from S3
* avoid storing AWS credentials in SQL scripts
* enforce secure cross-account access

Example configuration:

```sql
CREATE STORAGE INTEGRATION aws_s3
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://itgc-project/SourceData/');
```

This configuration enables controlled access between Snowflake and the S3 bucket.

---

# External Stage

An external stage is created to reference the S3 location where log files are stored.

```sql
CREATE STAGE ITGC_SNOWSTAGE
URL = 's3://itgc-project/SourceData/'
STORAGE_INTEGRATION = aws_s3
FILE_FORMAT = csv_format;
```

The stage acts as a **logical pointer to the storage location** containing log files.

---

# File Format Configuration

A reusable CSV file format is defined to standardize how Snowflake parses incoming log files.

```sql
CREATE FILE FORMAT csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
```

This ensures that log files are consistently interpreted during ingestion.

---

# Log Tables Created

The following staging tables are created to store raw operational logs.

| Table                      | Description                       |
| -------------------------- | --------------------------------- |
| HR_EVENTS                  | Employee lifecycle events         |
| BACKUP_JOB                 | Backup execution logs             |
| RESTORATION_TEST           | Disaster recovery testing results |
| ACCESS_PROVISIONING        | User access provisioning logs     |
| CHANGE_TICKETS             | Change management records         |
| DEPLOYMENT                 | Application deployment logs       |
| INCIDENT_TICKETS           | Incident management records       |
| BATCH_JOB_RUN              | Batch job execution logs          |
| PRIVILEGED_ACCESS_REGISTER | Privileged account assignments    |

These datasets simulate operational logs typically monitored as part of **IT General Controls (ITGC)**.

---

# Data Ingestion Process

Data is loaded into Snowflake using the `COPY INTO` command.

Example ingestion command:

```sql
COPY INTO ITGC.STAGING.HR_EVENTS
FROM @ITGC.STAGING.ITGC_SNOWSTAGE/hr_events.csv;
```

This command:

1. Reads files from the external stage
2. Parses them using the defined file format
3. Loads the data into Snowflake tables

---

# Security Monitoring Value

The ingested datasets represent operational events that are critical for security and compliance monitoring.

Examples include:

Access management logs
Change management records
Backup operations
Incident response data
Privileged access assignments

These datasets provide the **raw evidence used to validate ITGC controls in the downstream data models**.

---

# Key Takeaways

This infrastructure setup provides a secure and scalable foundation for the ITGC monitoring platform.

Key capabilities include:

* cloud-based log ingestion
* secure Snowflake–S3 integration
* structured staging tables for operational logs
* scalable architecture supporting dbt transformations

The environment enables reliable ingestion of operational logs that are later transformed into **security monitoring and compliance reporting datasets**.
