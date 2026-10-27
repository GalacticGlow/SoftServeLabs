from pyspark.sql.types import StringType, StructField, StructType
from pathlib import Path
import sys
import yaml

SRC_PATH = str(Path.cwd().parent / "src")
sys.path.insert(0, SRC_PATH)

from silver import build_scd2_column_list, prepare_silver_dataframe

CONFIG_PATH = "/Workspace/Users/art.gulidov@softserve.academy/Lab6/config/config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

def test_build_scd2_column_list_applies_renames(spark):
    columns = ["code", "owner", "product_quantity", "product_name"]
    rename_map = {
        "code": "product_id",
        "owner": "brand_owner",
        "product_quantity": "product_weight",
    }

    result = build_scd2_column_list(columns, rename_map)

    assert result == [
        "product_id",
        "brand_owner",
        "product_weight",
        "product_name",
    ]

def test_prepare_silver_dataframe_deduplicates_and_cleans_values(spark):
    COLUMNS_TO_KEEP = config["SILVER"]["COLUMNS_TO_KEEP"]
    RENAME_MAP = config["SILVER"]["RENAME_MAP"]
    PNN_CATEGORIES = config["SILVER"]["PNN_CATEGORIES"]
    MICROELEMENT_COLS = config["SILVER"]["MICROELEMENT_COLS"]

    base = {column: "N/A" for column in COLUMNS_TO_KEEP}
    base.update({
        "code": "product-1",
        "url": "  https://example.com/product  ",
        "creator": "test-user",
        "created_datetime": "2024-12-01 10:00:00",
        "last_modified_datetime": "2025-01-02 03:04:05",
        "last_updated_datetime": "2025-01-02 03:04:05",
        "last_image_datetime": "2025-01-01 10:00:00",
        "product_name": "en:APPLE JUICE",
        "packaging": "en:PLASTIC BOTTLE",
        "packaging_text": "en:RECYCLE",
        "brands": "ACME",
        "manufacturing_places": "us",
        "purchase_places": "us",
        "countries": "us",
        "ingredients_text": "en:WATER, APPLE",
        "allergens": "en:NONE",
        "serving_quantity": "12.345",
        "nova_group": "3",
        "pnns_groups_2": "unknown-category",
        "environmental_score_score": "120",
        "environmental_score_grade": "b",
        "product_quantity": "250",
        "owner": "org-acme-inc",
        "unique_scans_n": "-2",
        "completeness": "0.8",
        "image_url": "bad-url",
        "image_ingredients_url": "https://example.com/ingredients.jpg",
        "image_nutrition_url": None,
        "energy-kj_100g": "418.4",
        "energy-kcal_100g": "0",
        "fat_100g": "5",
        "saturated-fat_100g": "8",
        "cholesterol_100g": "0.1",
        "carbohydrates_100g": "12.5",
        "sugars_100g": "10",
        "lactose_100g": "0",
        "starch_100g": "1.5",
        "fiber_100g": "2",
        "proteins_100g": "-1.234",
        "salt_100g": "0.5",
        "sodium_100g": "0.2",
        "alcohol_100g": "0",
        "vitamin-a_100g": "0.01",
        "vitamin-d_100g": "0.02",
        "vitamin-e_100g": "0.03",
        "vitamin-k_100g": "0.04",
        "vitamin-c_100g": "0.05",
        "vitamin-b1_100g": "0.06",
        "vitamin-b2_100g": "0.07",
        "vitamin-b6_100g": "0.08",
        "vitamin-b9_100g": "0.09",
        "vitamin-b12_100g": "0.1",
        "potassium_100g": "100",
        "calcium_100g": "50",
        "phosphorus_100g": "30",
        "iron_100g": "1.2",
        "magnesium_100g": "15",
        "zinc_100g": "0.5",
        "copper_100g": "0.1",
        "iodine_100g": "0.02",
        "caffeine_100g": "0",
        "carbon-footprint_100g": "2.5",
    })

    older = dict(base)
    older["last_modified_datetime"] = "2025-01-01 03:04:05"
    older["product_name"] = "en:OLD PRODUCT"

    schema = StructType([
        StructField(column, StringType(), True)
        for column in COLUMNS_TO_KEEP
    ])
    bronze_df = spark.createDataFrame([older, base], schema=schema)

    result = prepare_silver_dataframe(
        bronze_snapshot_df=bronze_df,
        columns_to_keep=COLUMNS_TO_KEEP,
        rename_map=RENAME_MAP,
        pnn_categories=["Category A", "Category B"],
        microelement_columns=MICROELEMENT_COLS,
        country_map={"us": "United States"},
    )

    rows = result.collect()

    assert len(rows) == 1
    row = rows[0].asDict()

    assert row["product_name"] == "Apple juice"
    assert row["countries"] == "United States"
    assert row["product_page_url"] == "https://example.com/product"
    assert row["product_image_url"] == "N/A"
    assert row["product_creation_datetime"] is not None
    assert row["last_user_modification_datetime"] is not None

    assert row["product_id"] == "product-1"
    assert row["entry_creator"] == "test-user"
    assert row["packaging_type"] == "Plastic bottle"
    assert row["packaging_details"] == "Recycle"
    assert row["brands_list"] == "Acme"
    assert row["manufacturing_locations"] == "United States"
    assert row["purchasing_locations"] == "United States"
    assert row["ingridients_description"] == "Water, apple"
    assert row["product_weight"] == 250.0
    assert row["brand_owner"] == "Acme Inc"
    assert row["unique_scan_count"] == 0
    assert row["product_data_completeness"] == "80.0%"
    assert row["last_product_image_datetime"] is not None
    assert row["image_nutrition_data_url"] == "N/A"

    assert row["serving_quantity"] == 12.35
    assert row["nova_group"] == 3
    assert row["environmental_score_score"] == 0
    assert row["environmental_score_grade"] == "B"
    assert row["energy_kcal_100g"] == 100.0
    assert row["saturated_fat_100g"] == 0
    assert row["proteins_100g"] == 0.0
    assert row["carbon_footprint_100g"] == 2.5

    for column in RENAME_MAP:
        assert column not in row

    assert "owner" not in row
    assert "ingridients_description" in row