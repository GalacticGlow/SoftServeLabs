CREATE OR REFRESH STREAMING TABLE
  dbr_dev_ua5816bd.gulidov_artem_silver.products;

CREATE FLOW products_scd2_flow AS
AUTO CDC INTO dbr_dev_ua5816bd.gulidov_artem_silver.products

FROM SNAPSHOT (
  SELECT * EXCEPT (_snapshot_version)
  FROM dbr_dev_ua5816bd.gulidov_artem_silver.products_to_scd
  WHERE _snapshot_version = (
    SELECT version
    FROM current_snapshot_version()
  )
)

WITH VERSION (
  SELECT _snapshot_version AS version
  FROM dbr_dev_ua5816bd.gulidov_artem_silver.products_to_scd
  WHERE (
    NOT EXISTS (
      SELECT 1 FROM last_snapshot_version()
    )
    OR _snapshot_version > (
      SELECT version FROM last_snapshot_version()
    )
  )
  GROUP BY _snapshot_version
  ORDER BY version
  LIMIT 1
)

KEYS (product_id)
STORED AS SCD TYPE 2