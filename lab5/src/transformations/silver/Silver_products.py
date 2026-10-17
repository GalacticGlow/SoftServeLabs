import pyspark.sql.functions as F

from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import json

COLUMNS_TO_KEEP = json.loads(
    spark.conf.get("lab5.batch.COLUMNS_TO_KEEP")
)

RENAME_MAP = json.loads(
    spark.conf.get("lab5.batch.RENAME_MAP")
)

pnn_categories = json.loads(
    spark.conf.get("lab5.batch.pnn_categories")
)

microelement_cols = json.loads(
    spark.conf.get("lab5.batch.microelement_cols")
)

country_map = json.loads(
    spark.conf.get("lab5.batch.country_map")
)

business_key = spark.conf.get("lab5.batch.business_key")

BRONZE_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.bronze_schema")}.{spark.conf.get("lab5.batch_bronze_table_name")}"
SILVER_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.silver_schema")}.{spark.conf.get("lab5.batch_silver_to_scd_table_name")}"

def silver_cleaning(df):
    df = df.select(*COLUMNS_TO_KEEP)

    #cleaning code
    window = Window.partitionBy('code').orderBy(F.desc('last_modified_datetime'))

    df = df.withColumn('flag', F.row_number() \
                    .over(window)) \
                    .filter("flag = 1") \
                    .drop('flag')
    #cleaning url
    df = df.withColumn(
        'url',
        F.when(
            ~F.col('url').contains('http') | F.col('url').isNull(),
            "N/A"
        ).otherwise(F.trim(F.col('url')))
    )
    #cleaning created_datetime, last_modified_datetime, last_updated_datetime and last_image_datetime
    cols = [
        "created_datetime",
        "last_modified_datetime",
        "last_updated_datetime",
        "last_image_datetime"
    ]

    for c in cols:
        df = df.withColumn(
            c,
            F.to_timestamp(c)
        )
    #cleaning product_name, packaging, packaging_text, brands, manufacturing_places, purchase_places, ingredients_text, allergens, countries
    cols = [
        'product_name',
        'packaging',
        'packaging_text',
        'brands',
        'manufacturing_places',
        'purchase_places',
        'ingredients_text',
        'allergens',
        'countries'
    ]

    for c in cols:
        df = df.withColumn(
            c,
            F.regexp_replace(c, r'\b[a-zA-Z]{2}:', "")
        )
        df = df.withColumn(
            c,
            F.regexp_replace(c, r'^(0?[1-9]|1[0-2])\/\d{4}$', "")
        )

    cols_to_capitalize_first_letter = [
        'product_name',
        'packaging',
        'packaging_text',
        'brands',
        'ingredients_text',
        'allergens',
    ]

    for c in cols_to_capitalize_first_letter:
        df = df.withColumn(
            c,
            F.trim(
                F.concat(
                    F.upper(F.substring(c, 1, 1)),
                    F.lower(F.substring(c, 2, 1000))
                )
            )
        )

    cols_to_initcap = [
        'manufacturing_places',
        'purchase_places',
        'countries'
    ]

    for c in cols_to_initcap:
        df = df.withColumn(
            c,
            F.initcap(F.col(c))
        )

    df = df.withColumn(
        'brands',
        F.when(
            F.col('brands').rlike(r'[!#"%&\^\*\(\)\-\+;:\'\`\$\~\.\[\]/<>@]') |
            F.col('brands').rlike(r'[0-9]$'),
            'N/A'
        ).otherwise(F.col('brands'))
    )

    country_mapper = F.create_map(
        [F.lit(x) for x in sum(country_map.items(), ())]
    )

    country_cols = ['countries', 'purchase_places', 'manufacturing_places']
    for col in country_cols:
        df = df.withColumn(
            col,
            F.when(
                F.lower(F.col(col)).isin(list(country_map.keys())),
                country_mapper[F.col(col)]
            ).otherwise(F.col(col))
        )
    #cleaning serving_quantity
    df = df.withColumn(
        'serving_quantity',
        F.when(
            (F.col('serving_quantity').try_cast('float') < 0) | (F.col('serving_quantity').isNull()),
            0
        ).otherwise(F.col('serving_quantity').try_cast('float'))
    )
    df = df.withColumn(
        'serving_quantity',
        F.round(F.col('serving_quantity'), 2)
    )
    #cleaning nova_group
    df = df.withColumn(
        'nova_group',
        F.when(
            ~F.col('nova_group').isin('1', '2', '3', '4') | (F.col('nova_group').isNull()),
            0
        ).otherwise(F.col("nova_group").try_cast('int'))
    )
    #cleaning pnns_groups_2
    df = df.withColumn(
        'pnns_groups_2',
        F.when(
            ~F.col('pnns_groups_2').isin(pnn_categories) | F.col('pnns_groups_2').isNull(),
            'N/A'
        ).otherwise(F.initcap(F.col('pnns_groups_2')))
    )
    #cleaning environmental_score_score and _grade
    df = df.withColumn(
        'environmental_score_score',
        F.when(
            (F.col('environmental_score_score').try_cast('int') < 0) |
            (F.col('environmental_score_score').try_cast('int') > 100) |
            (F.col('environmental_score_score').isNull()),
            0
        ).otherwise(F.col('environmental_score_score').try_cast('int'))
    )
    df = df.withColumn(
        'environmental_score_grade',
        F.when(
            ~(F.col('environmental_score_grade').isin('a', 'b', 'c', 'd', 'e', 'f')) |
            (F.col("environmental_score_grade").isNull()),
            'N/A'
        ).otherwise(F.upper(F.col('environmental_score_grade')))
    )
    #cleaning product_quantity
    df = df.withColumn(
    'product_quantity',
    F.when(
        (F.col('product_quantity').try_cast('float') < 0) | 
        F.col('product_quantity').isNull(),
        0
    ).otherwise(F.round(F.col('product_quantity').try_cast('float'), 2))
    )
    #cleaning owner
    df = df.withColumn(
        'owner',
        F.regexp_replace('owner', r'org-', '')
    )
    df = df.withColumn(
        'owner',
        F.regexp_replace('owner', r'-', ' ')
    )
    df = df.withColumn(
        'owner',
        F.initcap(F.col('owner'))
    )
    #cleaning unique_scans_n
    df = df.withColumn(
        'unique_scans_n',
        F.when(
        (F.col('unique_scans_n').try_cast('int') < 0) | 
        F.col('unique_scans_n').isNull(),
        0
    ).otherwise(F.col('unique_scans_n').try_cast('int'))
    )
    #cleaning completeness
    df = df.withColumn(
        'completeness',
        F.col("completeness").try_cast('float') * 100
    )
    df = df.withColumn(
        'completeness',
        F.round(F.col('completeness'), 2)
    )
    df = df.withColumn(
        'completeness',
        F.concat(F.col('completeness').cast('string'), F.lit('%'))
    )
    #cleaning image_url columns
    df = df.withColumn(
        'image_url',
        F.when(
            ~F.col('image_url').contains('http') | F.col('image_url').isNull(),
            "N/A"
        ).otherwise(F.trim(F.col('image_url')))
    )
    df = df.withColumn(
        'image_ingredients_url',
        F.when(
            ~F.col('image_ingredients_url').contains('http') | 
            F.col('image_ingredients_url').isNull(),
            "N/A"
        ).otherwise(F.trim(F.col('image_ingredients_url')))
    )
    df = df.withColumn(
        'image_nutrition_url',
        F.when(
            ~F.col('image_nutrition_url').contains('http') | 
            F.col('image_nutrition_url').isNull(),
            "N/A"
        ).otherwise(F.trim(F.col('image_nutrition_url')))
    )
    #cleaning all the microelements columns

    for c in microelement_cols:
        df = df.withColumn(
            c,
            F.when(
                (F.col(c).try_cast('float') < 0) |
                F.col(c).isNull(),
                0
            ).otherwise(F.col(c).try_cast('float'))
        )
        df = df.withColumn(
            c,
            F.round(F.col(c), 2)
        )
    #data integrity for energy-kcal_100g
    df = df.withColumn(
        'energy-kcal_100g',
        F.col("energy-kj_100g") / 4.184
    )
    df = df.withColumn(
        'energy-kcal_100g',
        F.round(F.col('energy-kcal_100g'), 2)
    )
    #data integrity for the fat data columns
    df = df.withColumn(
        'saturated-fat_100g',
        F.when(
            F.col('saturated-fat_100g') > F.col("fat_100g"),
            0
        ).otherwise(F.col("saturated-fat_100g"))
    )
    cols = [
        "code", "creator", "product_name", "packaging", "packaging_text",
        "brands", "manufacturing_places", "purchase_places", "countries",
        "ingredients_text", "allergens", "owner",
        "completeness"
    ]

    df = df.replace('unknown', 'N/A', subset=cols)
    df = df.fillna('N/A', subset=cols)
    for old, new in RENAME_MAP.items():
        df = df.withColumnRenamed(old, new)

    return df

@dp.expect_all_or_drop({
    "valid_product_id": 
        "product_id IS NOT NULL AND TRIM(product_id) != ''",
    "valid_snapshot_version": 
        "_snapshot_version IS NOT NULL AND _snapshot_version > 0",
    "valid_product_name": 
        "product_name IS NOT NULL AND TRIM(product_name) != ''",
    "valid_brands_list": 
        "brands_list IS NOT NULL AND TRIM(brands_list) != ''"
})
@dp.materialized_view(
    name=SILVER_TABLE,
    table_properties={
        "quality": "silver",
        "layer": "silver"
    }
)
def products_cleaned_snapshots():
    df = spark.read.table(BRONZE_TABLE)

    df = silver_cleaning(df)

    df = df.filter(
        F.col("_snapshot_version").isNotNull()
        & F.col(business_key).isNotNull()
    )

    return df