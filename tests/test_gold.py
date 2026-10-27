from pathlib import Path
import sys

SRC_PATH = str(Path.cwd().parent / "src")
sys.path.insert(0, SRC_PATH)

from gold import build_gold_tables

def test_build_gold_tables_derives_ids_filters_current_rows_and_preserves_relationships(spark):
    source_df = spark.createDataFrame(
        [
            ("p1", True, "Acme", "N/A", "Apple juice"),
            ("p2", True, "Acme", "Acme Corp", "Apple sauce"),
            ("p3", False, "Old Brand", "Old Owner", "Old product"),
        ],
        [
            "product_id",
            "is_current",
            "brands_list",
            "brand_owner",
            "product_name",
        ],
    )

    tables = build_gold_tables(
        silver_df=source_df,
        fact_product_entries_cols=["entry_id", "product_id", "brand_id"],
        dim_brands_cols=["brand_id", "brands_list", "brand_owner"],
        dim_products_cols=["product_id", "product_name"],
    )

    fact = tables["fact_product_entries"]
    brands = tables["dim_brands"]
    products = tables["dim_products"]

    assert fact.count() == 2
    assert products.count() == 2
    assert brands.count() == 1

    brand = brands.first()
    assert brand["brand_owner"] == "Acme Corp"
    assert brand["brand_id"] is not None

    assert fact.filter("entry_id IS NULL OR brand_id IS NULL").count() == 0

    joined = (
        fact.join(products, on="product_id", how="left")
        .join(brands, on="brand_id", how="left")
    )

    assert joined.count() == 2
    assert joined.filter(
        "product_name IS NULL OR brands_list IS NULL"
    ).count() == 0