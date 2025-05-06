{{ config(materialized = 'view') }}

with raw as (
  select * from {{ source('kaggle_db', 'studentvle') }}
),

nomulti as (
  select distinct
    code_module,
    code_presentation,
    id_student,
    id_site,
    date,
    sum_click
  from raw
),

cleaned as (
  select
    upper(trim(code_module)) as code_module,
    upper(trim(code_presentation)) as code_presentation,
    cast(id_student as integer) as id_student,
    cast(id_site as integer) as id_site,
    cast(date as integer) as date,
    cast(sum_click as integer) as sum_click
  from nomulti
),

no_missing as (
  select * from cleaned
  where id_student is not null
    and id_site is not null
    and date is not null
    and sum_click is not null
),

validated as (
  select * from no_missing
  where date >= 0
    and sum_click >= 0
),

filtered as (
  select * from validated
  where sum_click > 0
)

select * from filtered
