-- Schema migrations for ssa-ops application
-- All statements are idempotent (safe to re-run)

-- =============================================================================
-- EXTENSIONS & AUTHENTICATION
-- =============================================================================

-- Enable Databricks native authentication extension
-- Maps Databricks workspace identities to Postgres roles
CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Create developers role if not exists (for group-based access)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ssa_ops_developers') THEN
        CREATE ROLE ssa_ops_developers;
    END IF;
END
$$;

-- Grant group membership (maps Databricks workspace group to Postgres role)
-- Note: Actual mapping is done via databricks_auth extension
GRANT ssa_ops_developers TO CURRENT_USER;

-- =============================================================================
-- APPLICATION SCHEMA
-- =============================================================================

-- Create app schema for application tables
CREATE SCHEMA IF NOT EXISTS app;

-- Grant schema access to developers
GRANT USAGE ON SCHEMA app TO ssa_ops_developers;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO ssa_ops_developers;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ssa_ops_developers;

-- =============================================================================
-- CORE TABLES
-- =============================================================================

-- Test items table (for Electric SQL sync testing)
CREATE TABLE IF NOT EXISTS app.test_items (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2),
    quantity INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for common queries
CREATE INDEX IF NOT EXISTS idx_test_items_name ON app.test_items(name);
CREATE INDEX IF NOT EXISTS idx_test_items_created_at ON app.test_items(created_at);

-- Insert sample data if table is empty
INSERT INTO app.test_items (name, description, price, quantity)
SELECT 'Sample Item 1', 'First test item for sync testing', 19.99, 100
WHERE NOT EXISTS (SELECT 1 FROM app.test_items LIMIT 1);

INSERT INTO app.test_items (name, description, price, quantity)
SELECT 'Sample Item 2', 'Second test item for sync testing', 29.99, 50
WHERE NOT EXISTS (SELECT 1 FROM app.test_items WHERE name = 'Sample Item 2');

INSERT INTO app.test_items (name, description, price, quantity)
SELECT 'Sample Item 3', 'Third test item for sync testing', 39.99, 25
WHERE NOT EXISTS (SELECT 1 FROM app.test_items WHERE name = 'Sample Item 3');

-- =============================================================================
-- UPDATED_AT TRIGGER
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION app.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to test_items (drop first to make idempotent)
DROP TRIGGER IF EXISTS update_test_items_updated_at ON app.test_items;
CREATE TRIGGER update_test_items_updated_at
    BEFORE UPDATE ON app.test_items
    FOR EACH ROW
    EXECUTE FUNCTION app.update_updated_at_column();

-- =============================================================================
-- ELECTRIC SQL PUBLICATION (for logical replication)
-- =============================================================================

-- Note: Logical replication (CREATE PUBLICATION) is not yet supported in Lakebase Autoscaling
-- Electric SQL sync will require either:
-- 1. Databricks to enable this feature
-- 2. Using a polling-based sync approach
-- 3. Using a different database with replication support

-- Placeholder for when replication becomes available:
-- CREATE PUBLICATION electric_publication FOR TABLE app.test_items;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

-- Show created objects
SELECT 'Schema: ' || schema_name AS created FROM information_schema.schemata WHERE schema_name = 'app'
UNION ALL
SELECT 'Table: app.' || table_name FROM information_schema.tables WHERE table_schema = 'app'
UNION ALL
SELECT 'Index: ' || indexname FROM pg_indexes WHERE schemaname = 'app';
