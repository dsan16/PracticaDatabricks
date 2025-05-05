{{ config(materialized = 'view') }}

with raw as (
  select *
  from {{ source('kaggle_db', 'courses') }}
),

nomulti as (
  select
    distinct
    code_module,
    code_presentation,
    module_presentation_length
  from raw
),

cleaned as (
  select
    upper(trim(code_module))         as code_module,
    upper(trim(code_presentation))   as code_presentation,
    try_cast(module_presentation_length as integer) as presentation_length
  from nomulti
),

no_missing as (
  select *
  from cleaned
  where code_module          is not null
    and code_presentation    is not null
    and presentation_length  is not null
),

validated as (
  select *
  from no_missing
  where presentation_length > 0
    and presentation_length < 10000 
),

filtered as (
  select *
  from validated
)

select * from filtered