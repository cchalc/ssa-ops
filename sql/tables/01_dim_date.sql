-- ============================================================================
-- dim_date - Date Dimension with Fiscal Calendar
-- ============================================================================
-- Generates a date dimension with both calendar and fiscal attributes
-- Databricks fiscal year ends January 31 (FY26 = Feb 1, 2025 - Jan 31, 2026)
-- Run on: fevm-cjc workspace
-- ============================================================================

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_date (
  -- Primary key
  date_key DATE NOT NULL,

  -- Calendar attributes
  cal_year INT,
  cal_quarter INT,
  cal_quarter_name STRING,
  cal_month INT,
  cal_month_name STRING,
  cal_month_short STRING,
  cal_week INT,
  cal_day_of_month INT,
  cal_day_of_week INT,
  cal_day_name STRING,
  cal_day_short STRING,
  cal_is_weekend BOOLEAN,
  cal_is_weekday BOOLEAN,

  -- Fiscal attributes (Databricks FY ends Jan 31)
  fy_year INT,
  fy_year_name STRING,
  fy_quarter INT,
  fy_quarter_name STRING,
  fy_month INT,
  fy_week INT,
  fy_day_of_year INT,

  -- Year-Quarter combinations
  cal_year_quarter STRING,
  fy_year_quarter STRING,

  -- Year-Month combinations
  cal_year_month STRING,
  fy_year_month STRING,

  -- Relative time flags (updated daily by refresh job)
  is_today BOOLEAN,
  is_current_week BOOLEAN,
  is_current_month BOOLEAN,
  is_current_quarter BOOLEAN,
  is_current_cal_year BOOLEAN,
  is_current_fy BOOLEAN,

  -- Period-to-date flags
  is_wtd BOOLEAN,
  is_mtd BOOLEAN,
  is_qtd BOOLEAN,
  is_ytd BOOLEAN,
  is_fytd BOOLEAN,

  -- Prior period references
  same_day_prior_week DATE,
  same_day_prior_month DATE,
  same_day_prior_quarter DATE,
  same_day_prior_year DATE,

  -- Week boundaries
  week_start_date DATE,
  week_end_date DATE,

  -- Month boundaries
  month_start_date DATE,
  month_end_date DATE,

  -- Quarter boundaries
  quarter_start_date DATE,
  quarter_end_date DATE,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Date dimension with calendar and fiscal (FY ends Jan 31) attributes'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Populate dim_date (5 years of history + 2 years future)
-- ============================================================================

-- Generate date sequence
WITH date_sequence AS (
  SELECT EXPLODE(SEQUENCE(
    DATE('2020-01-01'),
    DATE('2028-12-31'),
    INTERVAL 1 DAY
  )) AS date_key
),

-- Calculate fiscal year (FY ends Jan 31)
-- If month > 1, FY = cal_year + 1
-- If month = 1, FY = cal_year
fiscal_calc AS (
  SELECT
    date_key,
    YEAR(date_key) AS cal_year,
    QUARTER(date_key) AS cal_quarter,
    MONTH(date_key) AS cal_month,
    WEEKOFYEAR(date_key) AS cal_week,
    DAYOFMONTH(date_key) AS cal_day_of_month,
    DAYOFWEEK(date_key) AS cal_day_of_week,  -- 1=Sunday, 7=Saturday

    -- Fiscal year calculation (FY ends Jan 31)
    CASE
      WHEN MONTH(date_key) = 1 THEN YEAR(date_key)
      ELSE YEAR(date_key) + 1
    END AS fy_year,

    -- Fiscal month (Feb=1, Mar=2, ..., Jan=12)
    CASE
      WHEN MONTH(date_key) = 1 THEN 12
      ELSE MONTH(date_key) - 1
    END AS fy_month
  FROM date_sequence
),

-- Calculate fiscal quarter from fiscal month
fiscal_quarter_calc AS (
  SELECT
    *,
    CASE
      WHEN fy_month BETWEEN 1 AND 3 THEN 1   -- Feb, Mar, Apr
      WHEN fy_month BETWEEN 4 AND 6 THEN 2   -- May, Jun, Jul
      WHEN fy_month BETWEEN 7 AND 9 THEN 3   -- Aug, Sep, Oct
      ELSE 4                                   -- Nov, Dec, Jan
    END AS fy_quarter
  FROM fiscal_calc
),

-- Add all derived columns
full_date_calc AS (
  SELECT
    fc.date_key,

    -- Calendar
    fc.cal_year,
    fc.cal_quarter,
    CONCAT('Q', fc.cal_quarter) AS cal_quarter_name,
    fc.cal_month,
    DATE_FORMAT(fc.date_key, 'MMMM') AS cal_month_name,
    DATE_FORMAT(fc.date_key, 'MMM') AS cal_month_short,
    fc.cal_week,
    fc.cal_day_of_month,
    fc.cal_day_of_week,
    DATE_FORMAT(fc.date_key, 'EEEE') AS cal_day_name,
    DATE_FORMAT(fc.date_key, 'EEE') AS cal_day_short,
    fc.cal_day_of_week IN (1, 7) AS cal_is_weekend,
    fc.cal_day_of_week NOT IN (1, 7) AS cal_is_weekday,

    -- Fiscal
    fc.fy_year,
    CONCAT('FY', SUBSTRING(CAST(fc.fy_year AS STRING), 3, 2)) AS fy_year_name,
    fc.fy_quarter,
    CONCAT('FQ', fc.fy_quarter) AS fy_quarter_name,
    fc.fy_month,
    -- Fiscal week (weeks since FY start)
    CEILING(DATEDIFF(fc.date_key,
      CASE WHEN MONTH(fc.date_key) = 1 THEN DATE(CONCAT(fc.fy_year - 1, '-02-01'))
           ELSE DATE(CONCAT(fc.fy_year - 1, '-02-01'))
      END) / 7.0) AS fy_week,
    DATEDIFF(fc.date_key,
      DATE(CONCAT(fc.fy_year - 1, '-02-01'))) + 1 AS fy_day_of_year,

    -- Combinations
    CONCAT(fc.cal_year, '-Q', fc.cal_quarter) AS cal_year_quarter,
    CONCAT('FY', SUBSTRING(CAST(fc.fy_year AS STRING), 3, 2), '-Q', fc.fy_quarter) AS fy_year_quarter,
    DATE_FORMAT(fc.date_key, 'yyyy-MM') AS cal_year_month,
    CONCAT('FY', SUBSTRING(CAST(fc.fy_year AS STRING), 3, 2), '-M', LPAD(fc.fy_month, 2, '0')) AS fy_year_month,

    -- Relative flags (based on current date)
    fc.date_key = CURRENT_DATE() AS is_today,
    WEEKOFYEAR(fc.date_key) = WEEKOFYEAR(CURRENT_DATE()) AND fc.cal_year = YEAR(CURRENT_DATE()) AS is_current_week,
    fc.cal_month = MONTH(CURRENT_DATE()) AND fc.cal_year = YEAR(CURRENT_DATE()) AS is_current_month,
    fc.cal_quarter = QUARTER(CURRENT_DATE()) AND fc.cal_year = YEAR(CURRENT_DATE()) AS is_current_quarter,
    fc.cal_year = YEAR(CURRENT_DATE()) AS is_current_cal_year,
    fc.fy_year = (CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE()) ELSE YEAR(CURRENT_DATE()) + 1 END) AS is_current_fy,

    -- Period-to-date flags
    WEEKOFYEAR(fc.date_key) = WEEKOFYEAR(CURRENT_DATE())
      AND fc.cal_year = YEAR(CURRENT_DATE())
      AND fc.date_key <= CURRENT_DATE() AS is_wtd,
    fc.cal_month = MONTH(CURRENT_DATE())
      AND fc.cal_year = YEAR(CURRENT_DATE())
      AND fc.date_key <= CURRENT_DATE() AS is_mtd,
    fc.cal_quarter = QUARTER(CURRENT_DATE())
      AND fc.cal_year = YEAR(CURRENT_DATE())
      AND fc.date_key <= CURRENT_DATE() AS is_qtd,
    fc.cal_year = YEAR(CURRENT_DATE())
      AND fc.date_key <= CURRENT_DATE() AS is_ytd,
    fc.fy_year = (CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE()) ELSE YEAR(CURRENT_DATE()) + 1 END)
      AND fc.date_key <= CURRENT_DATE() AS is_fytd,

    -- Prior period references
    DATE_SUB(fc.date_key, 7) AS same_day_prior_week,
    ADD_MONTHS(fc.date_key, -1) AS same_day_prior_month,
    ADD_MONTHS(fc.date_key, -3) AS same_day_prior_quarter,
    ADD_MONTHS(fc.date_key, -12) AS same_day_prior_year,

    -- Week boundaries (Monday-Sunday)
    DATE_SUB(fc.date_key, (fc.cal_day_of_week + 5) % 7) AS week_start_date,
    DATE_ADD(DATE_SUB(fc.date_key, (fc.cal_day_of_week + 5) % 7), 6) AS week_end_date,

    -- Month boundaries
    DATE_TRUNC('MONTH', fc.date_key) AS month_start_date,
    LAST_DAY(fc.date_key) AS month_end_date,

    -- Quarter boundaries
    DATE_TRUNC('QUARTER', fc.date_key) AS quarter_start_date,
    LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', fc.date_key), 2)) AS quarter_end_date,

    CURRENT_TIMESTAMP() AS updated_at

  FROM fiscal_quarter_calc fc
)

-- Insert/merge into dim_date
MERGE INTO ${catalog}.${schema}.dim_date AS target
USING full_date_calc AS source
ON target.date_key = source.date_key
WHEN MATCHED THEN UPDATE SET
  -- Update relative flags (they change daily)
  target.is_today = source.is_today,
  target.is_current_week = source.is_current_week,
  target.is_current_month = source.is_current_month,
  target.is_current_quarter = source.is_current_quarter,
  target.is_current_cal_year = source.is_current_cal_year,
  target.is_current_fy = source.is_current_fy,
  target.is_wtd = source.is_wtd,
  target.is_mtd = source.is_mtd,
  target.is_qtd = source.is_qtd,
  target.is_ytd = source.is_ytd,
  target.is_fytd = source.is_fytd,
  target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *;

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT * FROM ${catalog}.${schema}.dim_date WHERE is_current_fy LIMIT 10;
-- SELECT fy_year, fy_quarter, MIN(date_key), MAX(date_key), COUNT(*) FROM ${catalog}.${schema}.dim_date GROUP BY ALL ORDER BY 1, 2;
