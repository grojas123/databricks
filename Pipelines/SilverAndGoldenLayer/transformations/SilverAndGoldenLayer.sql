-- ═══════════════════════════════════════════════════════════════════
-- SILVER LAYER: Cleansed, active golden records only
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REFRESH STREAMING TABLE silver_pipeline_assets
COMMENT 'Active, high-quality pipeline segments from Profisee MDM'
TBLPROPERTIES ('quality' = 'silver')
 AS SELECT
     _profisee_code AS pipeline_code,
     pipeline_system,
     segment_id,
     business_segment,
     diameter_inches,
     length_miles,
     max_allowable_operating_pressure_psig AS maop_psig,
     pipe_material,
     commodity_transported,
     production_basin,
     state,
     installation_year,
     last_inline_inspection_date,
     capacity_mmbtu_per_day,
     capacity_barrels_per_day,
     is_interstate,
     ferc_regulated,
     _profisee_data_quality_score AS dq_score,
     _profisee_source_system AS source_system,
     _profisee_updated_dtm AS last_updated
FROM STREAM(mdm.energy_transfer.dim_pipeline_assets)
WHERE _profisee_record_status = 'Active'
   AND _profisee_data_quality_score >= 70;


CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'Active customer golden records from Profisee MDM'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
 _profisee_code AS customer_code,
     customer_name,
     customer_type,
     duns_number,
     city,
     state,
     credit_rating,
     contract_type,
     primary_commodity,
     annual_volume_contracted_mmbtu,
     payment_terms_days,
     is_key_account,
     _profisee_data_quality_score AS dq_score,
     _profisee_updated_dtm AS last_updated
 FROM STREAM(mdm.energy_transfer.dim_customers)
 WHERE _profisee_record_status = 'Active'
   AND _profisee_data_quality_score >= 70;

CREATE OR REFRESH STREAMING TABLE silver_facilities
COMMENT 'Operational facilities from Profisee MDM'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
     _profisee_code AS facility_code,
     facility_name,
     facility_type,
     business_segment,
     state,
     county,
     latitude,
     longitude,
     nameplate_capacity,
     capacity_uom,
     year_commissioned,
     connected_pipeline_system,
     _profisee_data_quality_score AS dq_score,
     _profisee_updated_dtm AS last_updated
 FROM STREAM(mdm.energy_transfer.dim_facilities)
 WHERE _profisee_record_status = 'Active'
   AND is_operational = true;


CREATE OR REFRESH STREAMING TABLE silver_contracts
COMMENT 'Active contracts from Profisee MDM'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
     _profisee_code AS contract_code,
     contract_number,
     contract_type,
     customer_code,
     pipeline_system,
     commodity,
     effective_date,
     expiration_date,
     term_years,
     max_daily_quantity_mmbtu,
     max_daily_quantity_bpd,
     rate_per_unit_usd,
     has_escalation_clause,
     status AS contract_status,
     _profisee_data_quality_score AS dq_score
FROM STREAM(mdm.energy_transfer.dim_contracts)
WHERE _profisee_record_status = 'Active'
   AND status IN ('Active', 'Pending Execution');

CREATE OR REFRESH STREAMING TABLE silver_throughput
COMMENT 'Validated daily throughput measurements'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
     measurement_date,
     pipeline_asset_code,
     scheduled_volume_mmbtu,
     actual_volume_mmbtu,
     utilization_pct,
     flow_direction,
     measurement_quality_flag,
     _profisee_updated_dtm AS last_updated
 FROM STREAM(mdm.energy_transfer.fact_throughput_daily)
 WHERE measurement_quality_flag IN ('Validated', 'Estimated');




-- GOLD LAYER: Business-ready aggregations
CREATE OR REFRESH MATERIALIZED VIEW gold_pipeline_summary AS
SELECT
     p.business_segment,
     p.commodity_transported,
     p.state,
     COUNT(*) AS segment_count,
     ROUND(SUM(p.length_miles), 1) AS total_miles,
     ROUND(AVG(p.dq_score), 2) AS avg_dq_score,
     SUM(CASE WHEN p.ferc_regulated THEN 1 ELSE 0 END) AS ferc_regulated_count
FROM silver_pipeline_assets p
GROUP BY p.business_segment, p.commodity_transported, p.state;

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_portfolio AS
SELECT
     c.customer_type,
     c.state,
     COUNT(*) AS customer_count,
     SUM(c.annual_volume_contracted_mmbtu) AS total_contracted_volume,
     ROUND(AVG(c.dq_score), 2) AS avg_dq_score,
     SUM(CASE WHEN c.is_key_account THEN 1 ELSE 0 END) AS key_accounts
FROM silver_customers c
GROUP BY c.customer_type, c.state;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_utilization AS
SELECT
     t.measurement_date,
     p.pipeline_system,
     p.business_segment,
     p.commodity_transported,
     COUNT(*) AS segments_measured,
     ROUND(AVG(t.utilization_pct), 2) AS avg_utilization_pct,
     ROUND(SUM(t.actual_volume_mmbtu), 0) AS total_actual_mmbtu,
     ROUND(SUM(t.scheduled_volume_mmbtu), 0) AS total_scheduled_mmbtu
 FROM silver_throughput t
 JOIN silver_pipeline_assets p
   ON t.pipeline_asset_code = p.pipeline_code
 GROUP BY t.measurement_date, p.pipeline_system, p.business_segment, p.commodity_transported;


CREATE OR REFRESH MATERIALIZED VIEW gold_contract_exposure AS
SELECT
     cn.contract_type,
     cn.pipeline_system,
     cn.commodity,
     COUNT(*) AS contract_count,
     ROUND(AVG(cn.rate_per_unit_usd), 4) AS avg_rate_usd,
     SUM(cn.max_daily_quantity_mmbtu) AS total_firm_mdq_mmbtu,
     SUM(CASE WHEN cn.has_escalation_clause THEN 1 ELSE 0 END) AS with_escalation
FROM silver_contracts cn
WHERE cn.contract_status = 'Active'
GROUP BY cn.contract_type, cn.pipeline_system, cn.commodity;

-- MAGIC ## Cell 7 — Data Quality Expectations (Optional)
--# MAGIC
--# MAGIC Add DQ constraints to the silver layer:

--# COMMAND ----------

--# MAGIC %sql
--# MAGIC -- Add to the silver_pipeline_assets definition:
--# MAGIC -- CONSTRAINT valid_diameter   EXPECT (diameter_inches > 0)
--# MAGIC -- CONSTRAINT valid_length     EXPECT (length_miles > 0)
--# MAGIC -- CONSTRAINT valid_state      EXPECT (state IS NOT NULL)
--# MAGIC -- CONSTRAINT high_dq_score    EXPECT (dq_score >= 70) ON VIOLATION DROP ROW
