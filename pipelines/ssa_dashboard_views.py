# Databricks notebook source
# MAGIC %md
# MAGIC # SSA Activity Dashboard - Declarative Pipeline
# MAGIC
# MAGIC This pipeline creates and refreshes materialized views for the SSA Activity Dashboard.
# MAGIC Views are created in the target schema specified by the pipeline configuration.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `stitch.salesforce.approvalrequest__c` - ASQ records
# MAGIC - `stitch.salesforce.user` - User records
# MAGIC - `stitch.salesforce.approved_usecase__c` - UCO records
# MAGIC - `main.gtm_gold.core_usecase_curated` - Enriched UCO data
# MAGIC - `main.gtm_gold.account_product_adoption` - Product adoption flags

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Configuration
MANAGER_ID = "0053f000000pKoTAAU"  # Christopher Chalcraft's SF User ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base Tables (Bronze/Silver)

# COMMAND ----------

@dlt.view(
    name="v_team_asqs_raw",
    comment="Raw ASQ data for CJC team members"
)
def team_asqs_raw():
    """Base view of all ASQs owned by team members."""
    return (
        spark.table("stitch.salesforce.approvalrequest__c")
        .alias("a")
        .join(
            spark.table("stitch.salesforce.user").alias("u"),
            F.col("a.OwnerId") == F.col("u.Id")
        )
        .filter(F.col("u.ManagerId") == MANAGER_ID)
        .select(
            F.col("a.Id").alias("asq_id"),
            F.col("a.Name").alias("asq_number"),
            F.col("a.Request_Name__c").alias("asq_title"),
            F.col("a.Status__c").alias("status"),
            F.col("a.Account_Name__c").alias("account_name"),
            F.col("a.AccountId__c").alias("account_id"),
            F.col("a.Specialization__c").alias("specialization"),
            F.col("a.Support_Type__c").alias("support_type"),
            F.col("a.CreatedDate").alias("created_date"),
            F.col("a.AssignmentDate__c").alias("assignment_date"),
            F.col("a.End_Date__c").alias("due_date"),
            F.col("a.LastModifiedDate").alias("last_modified_date"),
            F.col("a.Request_Status_Notes__c").alias("notes"),
            F.col("a.Actual_Effort_Days__c").alias("actual_effort_days"),
            F.col("a.Estimated_Effort_Days__c").alias("estimated_effort_days"),
            F.col("a.Approved_Use_Case__c").alias("linked_uco_id"),
            F.col("u.Name").alias("owner_name"),
            F.col("u.Email").alias("owner_email")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - Completed Metrics

# COMMAND ----------

@dlt.table(
    name="asq_completed_metrics",
    comment="Closed ASQ analysis with turnaround time and completion metrics"
)
def asq_completed_metrics():
    """Materialized view of completed ASQ metrics."""
    return (
        dlt.read("v_team_asqs_raw")
        .filter(F.col("status").isin(["Completed", "Closed"]))
        .withColumn("days_total",
            F.datediff(F.col("last_modified_date"), F.col("created_date")))
        .withColumn("days_in_progress",
            F.datediff(F.col("last_modified_date"), F.col("assignment_date")))
        .withColumn("days_to_assign",
            F.datediff(F.col("assignment_date"), F.col("created_date")))
        .withColumn("completion_quarter",
            F.concat(F.year("last_modified_date"), F.lit("-Q"), F.quarter("last_modified_date")))
        .withColumn("completion_year", F.year("last_modified_date"))
        .withColumn("completion_month", F.month("last_modified_date"))
        .withColumn("completion_week", F.weekofyear("last_modified_date"))
        .withColumn("delivered_on_time",
            F.when((F.col("due_date").isNotNull()) &
                   (F.col("last_modified_date") <= F.col("due_date")), 1).otherwise(0))
        .withColumn("quality_closure",
            F.when((F.col("notes").isNotNull()) &
                   (F.length("notes") > 50) &
                   (F.col("actual_effort_days").isNotNull()), 1).otherwise(0))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - SLA Metrics

# COMMAND ----------

@dlt.table(
    name="asq_sla_metrics",
    comment="SLA tracking per ASQ - measures time to key milestones"
)
def asq_sla_metrics():
    """Materialized view of SLA metrics per ASQ."""
    return (
        dlt.read("v_team_asqs_raw")
        .withColumn("days_to_review",
            F.when(F.col("assignment_date").isNotNull(),
                   F.datediff(F.col("assignment_date"), F.col("created_date"))))
        .withColumn("days_to_assignment",
            F.when(F.col("status").isin(["In Progress", "On Hold", "Completed", "Closed"]),
                   F.datediff(F.col("assignment_date"), F.col("created_date"))))
        .withColumn("days_to_first_response",
            F.when((F.col("notes").isNotNull()) &
                   (F.length("notes") > 10) &
                   (F.col("assignment_date").isNotNull()),
                   F.least(F.datediff(F.current_date(), F.col("assignment_date")), F.lit(7))))
        .withColumn("review_sla_met",
            F.when((F.col("days_to_review").isNotNull()) & (F.col("days_to_review") <= 2), 1).otherwise(0))
        .withColumn("assignment_sla_met",
            F.when((F.col("days_to_assignment").isNotNull()) & (F.col("days_to_assignment") <= 5), 1).otherwise(0))
        .withColumn("response_sla_met",
            F.when((F.col("days_to_first_response").isNotNull()) & (F.col("days_to_first_response") <= 5), 1).otherwise(0))
        .withColumn("sla_stage",
            F.when(F.col("days_to_review").isNull(), "Pending Review")
             .when(F.col("days_to_assignment").isNull(), "Pending Assignment")
             .when(F.col("days_to_first_response").isNull(), "Pending Response")
             .otherwise("Active"))
        .withColumn("created_week",
            F.concat(F.year("created_date"), F.lit("-W"), F.lpad(F.weekofyear("created_date"), 2, "0")))
        .withColumn("created_month",
            F.concat(F.year("created_date"), F.lit("-"), F.lpad(F.month("created_date"), 2, "0")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - Effort Accuracy

# COMMAND ----------

@dlt.table(
    name="asq_effort_accuracy",
    comment="Estimate vs actual effort comparison for calibration"
)
def asq_effort_accuracy():
    """Materialized view of effort accuracy metrics."""
    # Calculate estimated days based on specialization if not provided
    base = (
        dlt.read("v_team_asqs_raw")
        .filter(F.col("status").isin(["Completed", "Closed"]))
        .withColumn("derived_estimate",
            F.when(F.col("specialization").like("%ML%") | F.col("specialization").like("%AI%"), 10)
             .when(F.col("specialization").like("%Delta%"), 5)
             .when(F.col("specialization").like("%SQL%"), 3)
             .when(F.col("support_type") == "Deep Dive", 8)
             .when(F.col("support_type") == "Technical Review", 3)
             .otherwise(5))
        .withColumn("estimated_days",
            F.coalesce(F.col("estimated_effort_days"), F.col("derived_estimate")))
        .withColumn("days_in_progress",
            F.datediff(F.col("last_modified_date"), F.col("assignment_date")))
        .withColumn("effective_actual_days",
            F.coalesce(F.col("actual_effort_days"), F.col("days_in_progress")))
    )

    return (
        base
        .filter(F.col("estimated_days") > 0)
        .withColumn("effort_ratio",
            F.round(F.col("effective_actual_days") / F.col("estimated_days"), 2))
        .withColumn("accuracy_category",
            F.when(F.col("estimated_days").isNull() | (F.col("estimated_days") == 0), "No Estimate")
             .when(F.col("effective_actual_days") <= F.col("estimated_days") * 0.8, "Under Estimate")
             .when(F.col("effective_actual_days") <= F.col("estimated_days") * 1.2, "Accurate")
             .when(F.col("effective_actual_days") <= F.col("estimated_days") * 1.5, "Slight Over")
             .otherwise("Significant Over"))
        .withColumn("variance_days",
            F.col("effective_actual_days") - F.col("estimated_days"))
        .withColumn("completion_quarter",
            F.concat(F.year("last_modified_date"), F.lit("-Q"), F.quarter("last_modified_date")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - Re-engagement

# COMMAND ----------

@dlt.table(
    name="asq_reengagement",
    comment="Repeat account tracking - identifies accounts with multiple ASQs"
)
def asq_reengagement():
    """Materialized view of account re-engagement metrics."""
    base = dlt.read("v_team_asqs_raw")

    return (
        base
        .groupBy("account_name", "account_id")
        .agg(
            F.count("*").alias("total_asqs"),
            F.countDistinct("owner_name").alias("unique_ssas"),
            F.min("created_date").alias("first_asq_date"),
            F.max("created_date").alias("latest_asq_date"),
            F.sum(F.when(F.year("created_date") == F.year(F.current_date()), 1).otherwise(0)).alias("asqs_ytd"),
            F.sum(F.when((F.year("created_date") == F.year(F.current_date())) &
                         (F.quarter("created_date") == F.quarter(F.current_date())), 1).otherwise(0)).alias("asqs_qtd"),
            F.collect_set("specialization").alias("specializations_used"),
            F.collect_set("support_type").alias("support_types_used"),
            F.sum(F.when(F.col("status").isin(["In Progress", "Under Review", "On Hold"]), 1).otherwise(0)).alias("active_asqs"),
            F.sum(F.when(F.col("status").isin(["Completed", "Closed"]), 1).otherwise(0)).alias("completed_asqs")
        )
        .withColumn("engagement_span_days",
            F.datediff(F.col("latest_asq_date"), F.col("first_asq_date")))
        .withColumn("engagement_tier",
            F.when(F.col("total_asqs") >= 5, "High Engagement")
             .when(F.col("total_asqs") >= 2, "Repeat Customer")
             .otherwise("Single Engagement"))
        .withColumn("is_repeat_customer",
            F.when(F.col("total_asqs") >= 2, 1).otherwise(0))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - UCO Linkage

# COMMAND ----------

@dlt.table(
    name="asq_uco_linkage",
    comment="UCO (Use Case Opportunity) and consumption linkage"
)
def asq_uco_linkage():
    """Materialized view of ASQ to UCO linkage."""
    asq_base = dlt.read("v_team_asqs_raw")

    uco_details = (
        spark.table("stitch.salesforce.approved_usecase__c")
        .alias("uc")
        .join(
            spark.table("main.gtm_gold.core_usecase_curated").alias("curated"),
            F.col("uc.Id") == F.col("curated.use_case_id"),
            "left"
        )
        .select(
            F.col("uc.Id").alias("uco_id"),
            F.col("uc.Name").alias("uco_number"),
            F.col("uc.Use_Case_Title__c").alias("uco_title"),
            F.col("uc.Status__c").alias("uco_status"),
            F.col("uc.Stage__c").alias("uco_stage"),
            F.col("uc.Estimated_DBUs__c").alias("estimated_dbus"),
            F.col("curated.use_case_type"),
            F.col("curated.primary_product"),
            F.col("curated.estimated_arr_usd")
        )
    )

    return (
        asq_base
        .join(uco_details, asq_base.linked_uco_id == uco_details.uco_id, "left")
        .withColumn("has_uco_link",
            F.when(F.col("uco_id").isNotNull(), 1).otherwise(0))
        .withColumn("linkage_status",
            F.when((F.col("has_uco_link") == 1) & (F.col("uco_status") == "Active"), "Strong Link")
             .when(F.col("has_uco_link") == 1, "Linked")
             .otherwise("No UCO Link"))
    )

# COMMAND ----------

@dlt.table(
    name="asq_uco_summary",
    comment="UCO linkage summary by SSA"
)
def asq_uco_summary():
    """Aggregated UCO linkage summary per owner."""
    return (
        dlt.read("asq_uco_linkage")
        .groupBy("owner_name")
        .agg(
            F.count("*").alias("total_asqs"),
            F.sum("has_uco_link").alias("asqs_with_uco"),
            F.round(100.0 * F.sum("has_uco_link") / F.count("*"), 1).alias("uco_linkage_rate_pct"),
            F.sum(F.coalesce("estimated_dbus", F.lit(0))).alias("total_linked_dbus"),
            F.sum(F.coalesce("estimated_arr_usd", F.lit(0))).alias("total_linked_arr")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - Product Adoption

# COMMAND ----------

@dlt.table(
    name="asq_product_adoption",
    comment="Product adoption metrics linked to ASQ accounts"
)
def asq_product_adoption():
    """Materialized view of product adoption by ASQ accounts."""
    asq_accounts = (
        dlt.read("v_team_asqs_raw")
        .groupBy("account_id", "account_name")
        .agg(
            F.max("created_date").alias("latest_asq_date"),
            F.count("*").alias("total_asqs")
        )
    )

    product_adoption = spark.table("main.gtm_gold.account_product_adoption")

    return (
        asq_accounts
        .join(product_adoption, asq_accounts.account_id == product_adoption.account_id, "left")
        .withColumn("ai_ml_score",
            F.coalesce("has_model_serving", F.lit(0)) +
            F.coalesce("has_feature_store", F.lit(0)) +
            F.coalesce("has_mlflow", F.lit(0)) +
            F.coalesce("has_vector_search", F.lit(0)))
        .withColumn("modern_platform_score",
            F.coalesce("has_dlt", F.lit(0)) +
            F.coalesce("has_serverless_sql", F.lit(0)) +
            F.coalesce("has_unity_catalog", F.lit(0)))
        .withColumn("adoption_tier",
            F.when((F.col("ai_ml_score") >= 3) & (F.col("modern_platform_score") >= 2), "Advanced")
             .when((F.col("ai_ml_score") >= 2) | (F.col("modern_platform_score") >= 2), "Growing")
             .when((F.col("ai_ml_score") >= 1) | (F.col("modern_platform_score") >= 1), "Early")
             .otherwise("Basic"))
        .withColumn("ssa_influence_status",
            F.when((F.col("total_asqs") > 0) & ((F.col("ai_ml_score") >= 2) | (F.col("modern_platform_score") >= 2)), "SSA Influenced")
             .when(F.col("total_asqs") > 0, "SSA Engaged")
             .otherwise("No SSA"))
    )

# COMMAND ----------

@dlt.table(
    name="product_adoption_summary",
    comment="Team-level product adoption summary"
)
def product_adoption_summary():
    """Aggregated product adoption summary."""
    return (
        dlt.read("asq_product_adoption")
        .agg(
            F.countDistinct("account_id").alias("unique_accounts_supported"),
            F.sum(F.when(F.col("has_model_serving") == 1, 1).otherwise(0)).alias("accounts_with_model_serving"),
            F.sum(F.when(F.col("has_feature_store") == 1, 1).otherwise(0)).alias("accounts_with_feature_store"),
            F.sum(F.when(F.col("has_mlflow") == 1, 1).otherwise(0)).alias("accounts_with_mlflow"),
            F.sum(F.when(F.col("has_vector_search") == 1, 1).otherwise(0)).alias("accounts_with_vector_search"),
            F.sum(F.when(F.col("has_dlt") == 1, 1).otherwise(0)).alias("accounts_with_dlt"),
            F.sum(F.when(F.col("has_serverless_sql") == 1, 1).otherwise(0)).alias("accounts_with_serverless_sql"),
            F.sum(F.when(F.col("has_unity_catalog") == 1, 1).otherwise(0)).alias("accounts_with_unity_catalog"),
            F.sum(F.when(F.col("adoption_tier") == "Advanced", 1).otherwise(0)).alias("tier_advanced"),
            F.sum(F.when(F.col("adoption_tier") == "Growing", 1).otherwise(0)).alias("tier_growing"),
            F.sum(F.when(F.col("adoption_tier") == "Early", 1).otherwise(0)).alias("tier_early"),
            F.sum(F.when(F.col("adoption_tier") == "Basic", 1).otherwise(0)).alias("tier_basic"),
            F.sum(F.coalesce("total_dbu_consumption", F.lit(0))).alias("total_dbu_consumption"),
            F.current_timestamp().alias("snapshot_time")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views - Team Summary

# COMMAND ----------

@dlt.table(
    name="team_summary",
    comment="Team-level executive summary metrics"
)
def team_summary():
    """Executive dashboard summary metrics."""
    open_asqs = (
        dlt.read("v_team_asqs_raw")
        .filter(F.col("status").isin(["In Progress", "Under Review", "On Hold", "Submitted"]))
    )

    completed_qtd = (
        dlt.read("v_team_asqs_raw")
        .filter(
            (F.col("status").isin(["Completed", "Closed"])) &
            (F.quarter("last_modified_date") == F.quarter(F.current_date())) &
            (F.year("last_modified_date") == F.year(F.current_date()))
        )
    )

    # Calculate capacity per person
    capacity = (
        open_asqs
        .withColumn("estimated_days",
            F.when(F.col("specialization").like("%ML%") | F.col("specialization").like("%AI%"), 10)
             .when(F.col("specialization").like("%Delta%"), 5)
             .when(F.col("specialization").like("%SQL%"), 3)
             .when(F.col("support_type") == "Deep Dive", 8)
             .when(F.col("support_type") == "Technical Review", 3)
             .otherwise(5))
        .groupBy("owner_name")
        .agg(F.sum("estimated_days").alias("total_days"))
        .withColumn("capacity_status",
            F.when(F.col("total_days") <= 5, "GREEN")
             .when(F.col("total_days") <= 10, "YELLOW")
             .otherwise("RED"))
    )

    # Aggregate metrics
    return (
        spark.createDataFrame([(1,)], ["dummy"])
        .withColumn("total_open_asqs", F.lit(open_asqs.count()))
        .withColumn("overdue_asqs",
            F.lit(open_asqs.filter(F.col("due_date") < F.current_date()).count()))
        .withColumn("missing_notes_asqs",
            F.lit(open_asqs.filter(
                (F.col("notes").isNull()) | (F.length("notes") < 10)
            ).count()))
        .withColumn("completed_qtd", F.lit(completed_qtd.count()))
        .withColumn("avg_turnaround_days",
            F.lit(completed_qtd.withColumn("days",
                F.datediff("last_modified_date", "assignment_date"))
                .agg(F.round(F.avg("days"), 1)).first()[0]))
        .withColumn("team_members_green",
            F.lit(capacity.filter(F.col("capacity_status") == "GREEN").count()))
        .withColumn("team_members_yellow",
            F.lit(capacity.filter(F.col("capacity_status") == "YELLOW").count()))
        .withColumn("team_members_red",
            F.lit(capacity.filter(F.col("capacity_status") == "RED").count()))
        .withColumn("team_capacity_status",
            F.when(F.col("team_members_red") > 2, "RED")
             .when((F.col("team_members_red") + F.col("team_members_yellow")) > 3, "YELLOW")
             .otherwise("GREEN"))
        .withColumn("snapshot_time", F.current_timestamp())
        .drop("dummy")
    )
