from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule

def silver_checks():
    return [
        DQRowRule(
            name="product_id_required",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="product_id",
        ),
        DQDatasetRule(
            name="current_product_id_unique",
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=["product_id"],
        ),
        DQRowRule(
            name="nova_group_valid",
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={
                "expression": (
                    "nova_group IS NULL OR CAST(nova_group AS INT) BETWEEN 0 AND 4"
                )
            },
        ),
        DQRowRule(
            name="is_current_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="is_current",
        ),
    ]

def gold_products_checks():
    return [
        DQRowRule(
            name="product_id_required",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="product_id",
        ),
        DQDatasetRule(
            name="product_id_unique",
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=["product_id"],
        ),
    ]

def gold_brands_checks():
    return [
        DQRowRule(
            name="brand_id_required",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="brand_id",
        ),
        DQDatasetRule(
            name="brand_id_unique",
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=["brand_id"],
        ),
    ]

def gold_fact_checks():
    return [
        DQRowRule(
            name="entry_id_required",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="entry_id",
        ),
        DQDatasetRule(
            name="entry_id_unique",
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=["entry_id"],
        ),
    ]

def write_quarantine(
    invalid_df: DataFrame,
    source_table: str,
    quarantine_table: str,
    run_id: str,
):
    if invalid_df.limit(1).count() == 0:
        print(f"{source_table}: no invalid rows")
        return

    quarantine_df = (
        invalid_df
        .withColumn("_dq_source_table", F.lit(source_table))
        .withColumn("_dq_run_id", F.lit(run_id))
        .withColumn(
            "_dq_quarantined_at",
            F.current_timestamp(),
        )
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table)
    )

    print(
        f"{source_table}: wrote invalid rows to "
        f"{quarantine_table}"
    )

def run_dqx(
    engine: DQEngine,
    df: DataFrame,
    checks: list,
    source_table: str,
    quarantine_table: str,
    run_id: str,
):
    valid_df, invalid_df = engine.apply_checks_and_split(df, checks)

    write_quarantine(
        invalid_df=invalid_df,
        source_table=source_table,
        quarantine_table=quarantine_table,
        run_id=run_id,
    )

    return valid_df, invalid_df