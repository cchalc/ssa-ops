-- Dashboard schema for SSA Activity metrics
-- Run on Lakebase (ssa_ops_dev database)
-- These tables are populated by the sync job from logfood views

-- Create dashboard schema
CREATE SCHEMA IF NOT EXISTS dashboard;

-- Team Summary (single row, refreshed daily)
CREATE TABLE IF NOT EXISTS dashboard.team_summary (
    id SERIAL PRIMARY KEY,
    total_open_asqs INTEGER NOT NULL,
    overdue_asqs INTEGER NOT NULL,
    missing_notes_asqs INTEGER NOT NULL,
    completed_qtd INTEGER NOT NULL,
    avg_turnaround_days DECIMAL(5,1),
    team_members_green INTEGER NOT NULL,
    team_members_yellow INTEGER NOT NULL,
    team_members_red INTEGER NOT NULL,
    team_capacity_status VARCHAR(10) NOT NULL,
    unique_specializations INTEGER,
    unique_support_types INTEGER,
    snapshot_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ASQ Completed Metrics
CREATE TABLE IF NOT EXISTS dashboard.asq_completed_metrics (
    asq_id VARCHAR(18) PRIMARY KEY,
    asq_number VARCHAR(50) NOT NULL,
    asq_title VARCHAR(255),
    status VARCHAR(50),
    account_name VARCHAR(255),
    specialization VARCHAR(255),
    support_type VARCHAR(100),
    owner_name VARCHAR(100) NOT NULL,
    owner_email VARCHAR(255),
    created_date TIMESTAMP,
    assignment_date TIMESTAMP,
    due_date TIMESTAMP,
    completion_date TIMESTAMP,
    days_total INTEGER,
    days_in_progress INTEGER,
    days_to_assign INTEGER,
    completion_quarter VARCHAR(10),
    completion_year INTEGER,
    completion_month INTEGER,
    completion_week INTEGER,
    delivered_on_time SMALLINT,
    quality_closure SMALLINT,
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SLA Metrics
CREATE TABLE IF NOT EXISTS dashboard.asq_sla_metrics (
    asq_id VARCHAR(18) PRIMARY KEY,
    asq_number VARCHAR(50) NOT NULL,
    asq_title VARCHAR(255),
    status VARCHAR(50),
    account_name VARCHAR(255),
    owner_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    assignment_date TIMESTAMP,
    due_date TIMESTAMP,
    days_to_review INTEGER,
    days_to_assignment INTEGER,
    days_to_first_response INTEGER,
    review_sla_met SMALLINT,
    assignment_sla_met SMALLINT,
    response_sla_met SMALLINT,
    sla_stage VARCHAR(50),
    created_week VARCHAR(10),
    created_month VARCHAR(10),
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Effort Accuracy
CREATE TABLE IF NOT EXISTS dashboard.asq_effort_accuracy (
    asq_id VARCHAR(18) PRIMARY KEY,
    asq_number VARCHAR(50) NOT NULL,
    asq_title VARCHAR(255),
    status VARCHAR(50),
    account_name VARCHAR(255),
    specialization VARCHAR(255),
    support_type VARCHAR(100),
    owner_name VARCHAR(100) NOT NULL,
    estimated_days DECIMAL(5,1),
    actual_days DECIMAL(5,1),
    days_in_progress INTEGER,
    effective_actual_days DECIMAL(5,1),
    effort_ratio DECIMAL(5,2),
    accuracy_category VARCHAR(50),
    variance_days DECIMAL(5,1),
    completion_quarter VARCHAR(10),
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Account Re-engagement
CREATE TABLE IF NOT EXISTS dashboard.asq_reengagement (
    account_id VARCHAR(18) PRIMARY KEY,
    account_name VARCHAR(255) NOT NULL,
    total_asqs INTEGER NOT NULL,
    unique_ssas INTEGER,
    first_asq_date TIMESTAMP,
    latest_asq_date TIMESTAMP,
    engagement_span_days INTEGER,
    asqs_ytd INTEGER,
    asqs_qtd INTEGER,
    active_asqs INTEGER,
    completed_asqs INTEGER,
    engagement_tier VARCHAR(50),
    is_repeat_customer SMALLINT,
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SSA Performance (aggregated per SSA)
CREATE TABLE IF NOT EXISTS dashboard.ssa_performance (
    owner_name VARCHAR(100) PRIMARY KEY,
    total_open_asqs INTEGER NOT NULL,
    overdue_count INTEGER,
    missing_notes INTEGER,
    pct_missing_notes DECIMAL(5,1),
    pct_overdue DECIMAL(5,1),
    completed_qtd INTEGER,
    avg_turnaround_days DECIMAL(5,1),
    avg_effort_ratio DECIMAL(5,2),
    capacity_status VARCHAR(10),
    synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_completed_owner ON dashboard.asq_completed_metrics(owner_name);
CREATE INDEX IF NOT EXISTS idx_completed_quarter ON dashboard.asq_completed_metrics(completion_quarter);
CREATE INDEX IF NOT EXISTS idx_sla_owner ON dashboard.asq_sla_metrics(owner_name);
CREATE INDEX IF NOT EXISTS idx_sla_stage ON dashboard.asq_sla_metrics(sla_stage);
CREATE INDEX IF NOT EXISTS idx_effort_owner ON dashboard.asq_effort_accuracy(owner_name);
CREATE INDEX IF NOT EXISTS idx_reengagement_tier ON dashboard.asq_reengagement(engagement_tier);

-- Sync metadata table
CREATE TABLE IF NOT EXISTS dashboard.sync_log (
    id SERIAL PRIMARY KEY,
    sync_type VARCHAR(50) NOT NULL,
    source_view VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    rows_synced INTEGER,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);
