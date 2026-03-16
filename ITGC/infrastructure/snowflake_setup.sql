
-- WAREHOUSE CREATED
CREATE OR REPLACE WAREHOUSE ITGC
WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 2
INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE ITGC;

-- DATABSE ITGC CREATED
CREATE OR REPLACE DATABASE ITGC;

-- STAGING SCHEMA CREATED
CREATE OR REPLACE SCHEMA ITGC.staging;


-- CREATING A FILE FORMAT UNDER STAGING SCHEMA
CREATE OR REPLACE FILE FORMAT ITGC.STAGING.csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;



-- CREATE A STORAGE INTERGATIONS
CREATE OR REPLACE  STORAGE INTEGRATION aws_s3
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER =  S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::245955024561:role/snowflake-itgc-access-role'
STORAGE_ALLOWED_LOCATIONS =  ('s3://itgc-project/SourceData/');

DESC integration aws_s3;

-- CREATE AN EXTERNAL STORAGE
CREATE OR REPLACE STAGE ITGC_snowstage
URl = 's3://itgc-project/SourceData/'
STORAGE_INTEGRATION = aws_s3
FILE_FORMAT = ITGC.STAGING.csv_format;


LIST @ITGC.STAGING.ITGC_SNOWSTAGE;


-- CREATE THE TABLE TO COPY FROM THE STAGE
CREATE OR REPLACE TABLE ITGC.STAGING.hr_events (
    event_id STRING PRIMARY KEY,
    employee_id STRING,
    employee_name STRING,
    department STRING,
    event_type STRING,
    event_date TIMESTAMP_NTZ,
    performed_by STRING,
    notes STRING  
);


-- COPYING THE DATA FROM THE STAGE TO THE TABLES
COPY INTO ITGC.STAGING.HR_EVENTS FROM @ITGC.STAGING.ITGC_SNOWSTAGE/hr_events.csv;

SELECT * FROM ITGC.STAGING.HR_EVENTS
LIMIT 10;



-- CREATING TABLE BACKUP_JOB
CREATE OR REPLACE TABLE ITGC.STAGING.BACKUP_JOB (
    backup_id STRING,
    system_name STRING,
    backup_tool STRING,
    backup_type STRING,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_mins INT,
    status STRING,
    size_gb FLOAT,
    offsite_replicated BOOLEAN,
    offsite_location STRING,
    encrypted BOOLEAN,
    retention_days INT,
    incident_raised BOOLEAN,
    incident_id STRING,
    is_critical_system BOOLEAN,
    verified BOOLEAN,
    operator_id STRING
);

-- LODING INTO BACKUP_JOB TABLE
COPY INTO ITGC.STAGING.BACKUP_JOB FROM @ITGC.STAGING.ITGC_SNOWSTAGE/backup_job_log.csv;

SELECT * FROM ITGC.STAGING.BACKUP_JOB;

--  CREATING TABLE RESTORATION_TEST
CREATE OR REPLACE TABLE ITGC.STAGING.restoration_test (
    test_id STRING,
    system_name STRING,
    test_date TIMESTAMP_NTZ,
    test_type STRING,
    conducted_by STRING,
    approved_by STRING,
    rto_target_hrs INT, 
    rto_actual_hrs FLOAT,
    rto_met BOOLEAN,
    rpo_target_hrs INT,
    rpo_actual_hrs FLOAT,
    rpo_met STRING,
    overall_result STRING,
    issues_found INT,
    remediation_plan BOOLEAN,
    it_mgr_sign_off BOOLEAN,
    next_test_due TIMESTAMP_NTZ,
    notes STRING
);

-- LODING INTO RESTORATION_TEST TABLE
COPY INTO ITGC.STAGING.RESTORATION_TEST FROM @ITGC.STAGING.ITGC_SNOWSTAGE/restoration_test_log.csv;

SELECT * FROM ITGC.STAGING.RESTORATION_TEST;

-- CREATING TABLE ACCESS_PROVISIONING TABLE
CREATE OR REPLACE TABLE ITGC.STAGING.access_provisioning (
    log_id STRING,
    employee_id STRING,
    employee_name STRING, 
    system_name STRING,
    action STRING,
    role_granted STRING,
    event_timestamp TIMESTAMP_NTZ,
    performed_by STRING,
    approved_by STRING,
    request_ticket_id STRING, 
    is_emergency BOOLEAN,
    source_ip STRING
);

LIST @ITGC.STAGING.ITGC_SNOWSTAGE;

-- LOADING TO TABEL ACCESS PROVISIONING
COPY INTO ITGC.STAGING.ACCESS_PROVISIONING FROM @ITGC.STAGING.ITGC_SNOWSTAGE/access_provisioning_log.csv;

SELECT * FROM ITGC.STAGING.ACCESS_PROVISIONING;

-- CREATING TABLE CHANGE_TICKETS
CREATE OR REPLACE TABLE ITGC.STAGING.change_tickets(
    ticket_id STRING,
    title  STRING, 
    change_type STRING, 
    system_name STRING, 
    environment STRING,
    requestor_id STRING,
    created_at TIMESTAMP_NTZ,
    approved_by STRING, 
    approved_at TIMESTAMP_NTZ,
    cab_approved BOOLEAN,
    uat_completed BOOLEAN,
    uat_sign_off_by STRING, 
    status STRING, 
    rollback_plan BOOLEAN,
    priority STRING,
    closed_at TIMESTAMP_NTZ
);

-- LOADING TO TABLE CHANGE TICKETS FROM THE STAGE
COPY INTO ITGC.STAGING.CHANGE_TICKETS FROM @ITGC.STAGING.ITGC_SNOWSTAGE/change_tickets.csv;

SELECT * FROM ITGC.STAGING.CHANGE_TICKETS;

-- CREATING TABLE DEPLOYMENT
CREATE OR REPLACE TABLE ITGC.STAGING.deployment(
    deployment_id STRING, 
    system_name STRING, 
    environment STRING, 
    deployed_by STRING, 
    deployment_ts TIMESTAMP_NTZ,
    change_ticket_id STRING, 
    branch_name STRING, 
    deployment_method STRING, 
    duration_mins INT, 
    status STRING, 
    is_emergency BOOLEAN,
    notes STRING
);

--LOADING DATA INTO TABLE DEPLOYMENT
COPY INTO ITGC.STAGING.DEPLOYMENT FROM @ITGC.STAGING.ITGC_SNOWSTAGE/deployment_log.csv;

SELECT * FROM ITGC.STAGING.DEPLOYMENT
LIMIT 10;



-- CREATING TABLE INCIDENT_TICKETS
CREATE OR REPLACE TABLE ITGC.STAGING.incident_tickets (
    incident_id STRING, 
    related_job_run_id STRING, 
    title STRING,
    priority STRING, 
    category STRING, 
    created_at TIMESTAMP_NTZ,
    assigned_to STRING,
    resolved_at TIMESTAMP_NTZ,
    resolution_hrs INT, 
    sla_hrs INT, 
    sla_breached BOOLEAN,
    rca_documented BOOLEAN, 
    recurrence_count INT, 
    status STRING    
);

LIST @ITGC.STAGING.ITGC_SNOWSTAGE;

-- LOADING DATA INTO THE INCIDENT TICKETS TABLE
COPY INTO ITGC.STAGING.INCIDENT_TICKETS FROM @ITGC.STAGING.ITGC_SNOWSTAGE/incident_tickets.csv;

SELECT * FROM ITGC.STAGING.INCIDENT_TICKETS;

-- CREATING TABLE BATCH_JOB_RUN 
CREATE OR REPLACE TABLE ITGC.STAGING.BATCH_JOB_RUN (
    run_id STRING,
    job_name STRING,
    scheduler STRING,
    scheduled_start TIMESTAMP_NTZ,
    actual_start TIMESTAMP_NTZ,
    actual_end TIMESTAMP_NTZ,
    duration_mins NUMBER,
    status STRING,
    exit_code NUMBER,
    server STRING,
    triggered_by STRING,
    triggered_by_user STRING,
    incident_raised BOOLEAN,
    incident_id STRING,
    out_of_window BOOLEAN,
    schedule_changed BOOLEAN,
    schedule_change_ticket STRING,
    retry_count NUMBER,
    log_path STRING
);

--LOADING DATA INTO BATCH_JOB_RUN TABLE
COPY INTO ITGC.STAGING.BATCH_JOB_RUN FROM @ITGC.STAGING.ITGC_SNOWSTAGE/job_run_log.csv;

-- CREATING TABLE PRIVILEGED_ACCESS_REGISTER
CREATE OR REPLACE TABLE ITGC.STAGING.PRIVILEGED_ACCESS_REGISTER (
    register_id STRING,
    employee_id STRING,
    employee_name STRING,
    system_name STRING,
    privileged_role STRING,
    approved_by STRING,
    approval_date TIMESTAMP_NTZ,
    review_due_date TIMESTAMP_NTZ,
    is_active BOOLEAN,
    notes STRING
);

-- LOADING DATA INTO PRIVILEGED_ACCESS_REGISTER
COPY INTO ITGC.STAGING.PRIVILEGED_ACCESS_REGISTER FROM @ITGC.STAGING.ITGC_SNOWSTAGE/privileged_access_register.csv;

SELECT * FROM ITGC.STAGING.PRIVILEGED_ACCESS_REGISTER;

-- ── Access Management Streams ─────────────────────────────
CREATE OR REPLACE STREAM ITGC.STAGING.stream_access_provisioning
    ON TABLE ITGC.STAGING.ACCESS_PROVISIONING
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new IAM provisioning events for AC-1, AC-2, AC-3 testing';

CREATE OR REPLACE STREAM ITGC.STAGING.stream_hr_events
    ON TABLE ITGC.STAGING.HR_EVENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new hire and termination events for AC-2 testing';

CREATE OR REPLACE STREAM ITGC.STAGING.stream_privileged_register
    ON TABLE ITGC.STAGING.PRIVILEGED_ACCESS_REGISTER
    APPEND_ONLY = FALSE   -- standard stream — approvals can be updated
    COMMENT = 'Captures changes to privileged access approvals for AC-3 testing';

-- ── Change Management Streams ─────────────────────────────
CREATE OR REPLACE STREAM ITGC.STAGING.stream_change_tickets
    ON TABLE ITGC.STAGING.CHANGE_TICKETS
    APPEND_ONLY = FALSE   -- tickets get updated (status changes)
    COMMENT = 'Captures new and updated change tickets for CM-1, CM-2 testing';

CREATE OR REPLACE STREAM ITGC.STAGING.stream_deployments
    ON TABLE ITGC.STAGING.DEPLOYMENT
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new deployment events for CM-1, CM-2, CM-3 testing';

-- ── IT Operations Streams ──────────────────────────────────
CREATE OR REPLACE STREAM ITGC.STAGING.stream_job_runs
    ON TABLE ITGC.STAGING.BATCH_JOB_RUN
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new job execution events for OPS-1 testing';

CREATE OR REPLACE STREAM ITGC.STAGING.stream_incidents
    ON TABLE ITGC.STAGING.INCIDENT_TICKETS
    APPEND_ONLY = FALSE   -- incidents get updated (resolved, closed)
    COMMENT = 'Captures new and updated incidents for OPS-1, OPS-2 testing';

-- ── Backup & Recovery Streams ──────────────────────────────
CREATE OR REPLACE STREAM ITGC.STAGING.stream_backup_jobs
    ON TABLE ITGC.STAGING.BACKUP_JOB
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new backup job runs for BR-1 testing';

CREATE OR REPLACE STREAM ITGC.STAGING.stream_restoration_tests
    ON TABLE ITGC.STAGING.RESTORATION_TEST
    APPEND_ONLY = FALSE   -- test results can be updated
    COMMENT = 'Captures new and updated restoration tests for BR-2 testing';

-- ── Verify all streams created ─────────────────────────────
SHOW STREAMS IN SCHEMA ITGC.STAGING;
