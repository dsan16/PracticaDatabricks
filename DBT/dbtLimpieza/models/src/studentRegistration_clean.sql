{{ config(materialized = 'view') }}

with raw as (
  select * from {{ source('kaggle_db', 'studentregistration') }}
),

nomulti as (
  select distinct
    code_module,
    code_presentation,
    id_student,
    date_registration,
    date_unregistration
  from raw
),

cleaned as (
  select
    upper(trim(code_module)) as code_module,
    upper(trim(code_presentation)) as code_presentation,
    cast(id_student as integer) as id_student,
    cast(date_registration as integer) as date_registration,
    case
      when date_unregistration >= 0
      then cast(date_unregistration as integer)
      else null
    end as date_unregistration
  from nomulti
),

no_missing as (
  select * from cleaned
  where id_student is not null
    and date_registration is not null
),

validated as (
  select * from no_missing
  where date_registration >= -365
),

filtered as (
  select * from validated
)

select
  code_module,
  code_presentation,
  sha2(to_varchar(id_student), 256) as anon_student_id,
  date_registration,
  date_unregistration
from filtered
