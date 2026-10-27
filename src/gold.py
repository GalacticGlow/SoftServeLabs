from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_gold_tables(
    silver_df: DataFrame,
    fact_product_entries_cols: list[str],
    dim_brands_cols: list[str],
    dim_products_cols: list[str]
):
    df_main = silver_df.filter(F.col("is_current") == True)

    print("=== df_main ===")
    print("rows:", df_main.count())
    print("distinct product IDs:", df_main.select("product_id").distinct().count())

    df_main = (
        df_main
        .withColumn("entry_id", F.xxhash64("product_id"))
        .withColumn("brand_id", F.xxhash64("brands_list"))
    )

    fact_product_entries = df_main.select(*fact_product_entries_cols).distinct()
    dim_brands = df_main.select(*dim_brands_cols).distinct()
    dim_products = df_main.select(*dim_products_cols).distinct()

    print("=== AFTER SELECT/DISTINCT ===")
    print(
        "fact product IDs:",
        fact_product_entries.select("product_id").distinct().count()
    )
    print(
        "dim product IDs:",
        dim_products.select("product_id").distinct().count()
    )

    window = Window.partitionBy("brand_id", "brands_list").orderBy(
        F.when(
            F.col("brand_owner").isNull() | (F.col("brand_owner") == "N/A"),
            1,
        ).otherwise(0)
    )

    dim_brands = (
        dim_brands
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    return {
        "fact_product_entries": fact_product_entries,
        "dim_brands": dim_brands,
        "dim_products": dim_products,
    }