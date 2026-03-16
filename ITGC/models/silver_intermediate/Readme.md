Silver Intermediate Layer
Overview

The Silver Intermediate Layer contains transformed and standardized datasets derived from the Bronze (staging) layer.

In this layer, raw operational data is cleaned, normalized, and enriched to create consistent intermediate models that are ready for downstream analytics and compliance monitoring.

These models form the core transformation layer in the ITGC monitoring pipeline and provide structured datasets used by the Gold (mart) layer to generate compliance insights and control exception reporting.

Architecture Context
Raw Source Data
      │
      ▼
Bronze Layer (Staging)
Raw ingestion models
      │
      ▼
Silver Layer (Intermediate)
Data cleansing + normalization
      │
      ▼
Gold Layer (Marts)
ITGC control monitoring & analytics
Key Responsibilities of Silver Layer

The Silver layer performs the following transformations:

1. Data Standardization

Normalize column naming conventions

Ensure consistent data types

Standardize timestamps and identifiers

2. Data Cleansing

Remove invalid or duplicate records

Handle null or missing values

Apply basic validation logic

3. Data Enrichment

Combine related datasets

Derive additional attributes

Generate control evaluation fields

4. Business Logic Implementation

Apply ITGC control logic

Standardize operational datasets for governance monitoring

Models in This Layer
Access Provisioning

Processes user access provisioning events and prepares datasets used to monitor access management controls.

Typical fields:

employee_id

system_name

privileged_role

approval_status

approval_date

review_due_date

Purpose:

Identify unauthorized provisioning

Validate approval workflow compliance

Track privileged access lifecycle

Change Management

Transforms raw change ticket data to enable ITGC change management monitoring.

Typical fields:

ticket_id

change_type

created_at

approved_at

deployment_ts

approval_status

Purpose:

Ensure proper change approvals

Track deployment timelines

Detect unauthorized or emergency changes

Exception Tracking

Processes identified control violations and prepares structured records for compliance monitoring.

Typical fields:

control_id

domain

severity

finding_detail

remediation_sla_days

days_open

Purpose:

Track control violations

Measure remediation timelines

Identify SLA breaches

Data Sources

Silver models primarily consume data from the Bronze Staging Layer, including:

Source Model	Description
stg_access_provisioning_log	Raw access provisioning events
stg_change_tickets	Change management ticket data
Other staging models	Raw operational datasets
Materialization Strategy

Most models in this layer use:

materialized = incremental
incremental_strategy = merge

Benefits:

Efficient processing of large datasets

Reduced compute cost

Faster pipeline execution

Supports near-real-time ITGC monitoring

Testing and Data Quality

Data quality tests are applied to ensure reliability of intermediate datasets.

Typical tests include:

Not Null Tests

Unique Key Validation

Referential Integrity Checks

Control Logic Validation

These tests help maintain accurate compliance monitoring outputs.

Downstream Usage

The outputs from the Silver layer are used by the Gold Layer to:

Generate ITGC compliance dashboards

Identify control violations

Track remediation SLAs

Produce audit-ready reports

Best Practices Followed

Modular dbt models

Incremental processing

Reusable transformation logic

Clear separation of staging and business logic

Audit-friendly data structures
