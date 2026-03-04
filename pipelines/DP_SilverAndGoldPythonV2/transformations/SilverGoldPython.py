# Databricks notebook source
# Lakeflow Declarative Pipelines — Python translation

import dlt
from pyspark.sql.functions import (
    col, count, sum, avg, round, when, lit
)

# ═══════════════════════════════════════════════════════════════════
# SILVER LAYER: Cleansed, active golden records only
# ═══════════════════════════════════════════════════════════════════


@dlt.table(
    name="silver_pipeline_assets",
    comment="Active, high-quality pipeline segments from Profisee MDM",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_diameter", "diameter_inches > 0")
@dlt.expect("valid_length", "length_miles > 0")
@dlt.expect("valid_state", "state IS NOT NULL")
@dlt.expect_or_drop("high_dq_score", "_profisee_data_quality_score >= 70")
def silver_pipeline_assets():
    return (
        dlt.read_stream("mdm.energy_transfer.dim_pipeline_assets")
        .where("_profisee_record_status = 'Active'")
        .where("_profisee_data_quality_score >= 70")
        .select(
            col("_profisee_code").alias("pipeline_code"),
            "pipeline_system",
            "segment_id",
            "business_segment",
            "diameter_inches",
            "length_miles",
            col("max_allowable_operating_pressure_psig").alias("maop_psig"),
            "pipe_material",
            "commodity_transported",
            "production_basin",
            "state",
            "installation_year",
            "last_inline_inspection_date",
            "capacity_mmbtu_per_day",
            "capacity_barrels_per_day",
            "is_interstate",
            "ferc_regulated",
            col("_profisee_data_quality_score").alias("dq_score"),
            col("_profisee_source_system").alias("source_system"),
            col("_profisee_updated_dtm").alias("last_updated"),
        )
    )


@dlt.table(
    name="silver_customers",
    comment="Active customer golden records from Profisee MDM",
    table_properties={"quality": "silver"},
)
def silver_customers():
    return (
        dlt.read_stream("mdm.energy_transfer.dim_customers")
        .where("_profisee_record_status = 'Active'")
        .where("_profisee_data_quality_score >= 70")
        .select(
            col("_profisee_code").alias("customer_code"),
            "customer_name",
            "customer_type",
            "duns_number",
            "city",
            "state",
            "credit_rating",
            "contract_type",
            "primary_commodity",
            "annual_volume_contracted_mmbtu",
            "payment_terms_days",
            "is_key_account",
            col("_profisee_data_quality_score").alias("dq_score"),
            col("_profisee_updated_dtm").alias("last_updated"),
        )
    )


@dlt.table(
    name="silver_facilities",
    comment="Operational facilities from Profisee MDM",
    table_properties={"quality": "silver"},
)
def silver_facilities():
    return (
        dlt.read_stream("mdm.energy_transfer.dim_facilities")
        .where("_profisee_record_status = 'Active'")
        .where("is_operational = true")
        .select(
            col("_profisee_code").alias("facility_code"),
            "facility_name",
            "facility_type",
            "business_segment",
            "state",
            "county",
            "latitude",
            "longitude",
            "nameplate_capacity",
            "capacity_uom",
            "year_commissioned",
            "connected_pipeline_system",
            col("_profisee_data_quality_score").alias("dq_score"),
            col("_profisee_updated_dtm").alias("last_updated"),
        )
    )


@dlt.table(
    name="silver_contracts",
    comment="Active contracts from Profisee MDM",
    table_properties={"quality": "silver"},
)
def silver_contracts():
    return (
        dlt.read_stream("mdm.energy_transfer.dim_contracts")
        .where("_profisee_record_status = 'Active'")
        .where("status IN ('Active', 'Pending Execution')")
        .select(
            col("_profisee_code").alias("contract_code"),
            "contract_number",
            "contract_type",
            "customer_code",
            "pipeline_system",
            "commodity",
            "effective_date",
            "expiration_date",
            "term_years",
            "max_daily_quantity_mmbtu",
            "max_daily_quantity_bpd",
            "rate_per_unit_usd",
            "has_escalation_clause",
            col("status").alias("contract_status"),
            col("_profisee_data_quality_score").alias("dq_score"),
        )
    )


@dlt.table(
    name="silver_throughput",
    comment="Validated daily throughput measurements",
    table_properties={"quality": "silver"},
)
def silver_throughput():
    return (
        dlt.read_stream("mdm.energy_transfer.fact_throughput_daily")
        .where("measurement_quality_flag IN ('Validated', 'Estimated')")
        .select(
            "measurement_date",
            "pipeline_asset_code",
            "scheduled_volume_mmbtu",
            "actual_volume_mmbtu",
            "utilization_pct",
            "flow_direction",
            "measurement_quality_flag",
            col("_profisee_updated_dtm").alias("last_updated"),
        )
    )


# ═══════════════════════════════════════════════════════════════════
# GOLD LAYER: Business-ready aggregations
# ═══════════════════════════════════════════════════════════════════


@dlt.table(name="gold_pipeline_summary")
def gold_pipeline_summary():
    return (
        dlt.read("silver_pipeline_assets")
        .groupBy("business_segment", "commodity_transported", "state")
        .agg(
            count("*").alias("segment_count"),
            round(sum("length_miles"), 1).alias("total_miles"),
            round(avg("dq_score"), 2).alias("avg_dq_score"),
            sum(when(col("ferc_regulated"), 1).otherwise(0)).alias(
                "ferc_regulated_count"
            ),
        )
    )


@dlt.table(name="gold_customer_portfolio")
def gold_customer_portfolio():
    return (
        dlt.read("silver_customers")
        .groupBy("customer_type", "state")
        .agg(
            count("*").alias("customer_count"),
            sum("annual_volume_contracted_mmbtu").alias("total_contracted_volume"),
            round(avg("dq_score"), 2).alias("avg_dq_score"),
            sum(when(col("is_key_account"), 1).otherwise(0)).alias("key_accounts"),
        )
    )


@dlt.table(name="gold_daily_utilization")
def gold_daily_utilization():
    t = dlt.read("silver_throughput")
    p = dlt.read("silver_pipeline_assets")

    return (
        t.join(p, t.pipeline_asset_code == p.pipeline_code)
        .groupBy(
            "measurement_date",
            "pipeline_system",
            p.business_segment,
            "commodity_transported",
        )
        .agg(
            count("*").alias("segments_measured"),
            round(avg("utilization_pct"), 2).alias("avg_utilization_pct"),
            round(sum("actual_volume_mmbtu"), 0).alias("total_actual_mmbtu"),
            round(sum("scheduled_volume_mmbtu"), 0).alias("total_scheduled_mmbtu"),
        )
    )


@dlt.table(name="gold_contract_exposure")
def gold_contract_exposure():
    return (
        dlt.read("silver_contracts")
        .where("contract_status = 'Active'")
        .groupBy("contract_type", "pipeline_system", "commodity")
        .agg(
            count("*").alias("contract_count"),
            round(avg("rate_per_unit_usd"), 4).alias("avg_rate_usd"),
            sum("max_daily_quantity_mmbtu").alias("total_firm_mdq_mmbtu"),
            sum(when(col("has_escalation_clause"), 1).otherwise(0)).alias(
                "with_escalation"
            ),
        )
    )