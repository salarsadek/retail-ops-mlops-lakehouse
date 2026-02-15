{{ config(materialized='view') }}

select *
from read_parquet(
  '{{ (env_var("DBT_GOLD_PARQUET", "data/processed/m5/gold/fact_sales_features_sample.parquet")) }}'
)
