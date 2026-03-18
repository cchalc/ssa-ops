-- Setup Unity Catalog schema for ssa-ops
-- Deployed via Databricks Asset Bundle
-- Catalog: cjc_aws_workspace_catalog
-- Schema: ssa_ops_dev

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS cjc_aws_workspace_catalog.ssa_ops_dev
COMMENT 'SSA-Ops application schema';

-- Create test table with sample data
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.test_items (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  name STRING NOT NULL,
  description STRING,
  price DECIMAL(10, 2),
  quantity INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
COMMENT 'Test items table for local-first sync demo'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Insert sample data
INSERT INTO cjc_aws_workspace_catalog.ssa_ops_dev.test_items (name, description, price, quantity, created_at, updated_at)
VALUES
  ('Widget A', 'A basic widget for testing', 9.99, 100, current_timestamp(), current_timestamp()),
  ('Widget B', 'An advanced widget', 19.99, 50, current_timestamp(), current_timestamp()),
  ('Gadget X', 'A premium gadget', 49.99, 25, current_timestamp(), current_timestamp()),
  ('Gadget Y', 'Budget-friendly gadget', 14.99, 200, current_timestamp(), current_timestamp()),
  ('Tool Z', 'Multi-purpose tool', 29.99, 75, current_timestamp(), current_timestamp());

-- Show the data
SELECT * FROM cjc_aws_workspace_catalog.ssa_ops_dev.test_items;
