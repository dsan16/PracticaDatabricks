{{ config(materialized = 'view') }}

with raw as (
  select * from {{ source('kaggle_db', 'student_info') }}
),

nomulti as (
  select distinct
    code_module,
    code_presentation,
    id_student,
    gender,
    region,
    highest_education,
    imd_band,
    age_band,
    num_of_prev_attempts,
    studied_credits,
    disability,
    final_result
  from raw
),

cleaned as (
  select
    upper(trim(code_module))           as code_module,
    upper(trim(code_presentation))     as code_presentation,
    try_cast(id_student as integer)    as id_student,
    initcap(lower(gender))             as gender,
    initcap(trim(region))              as region,
    initcap(trim(highest_education))   as highest_education,
    upper(trim(imd_band))              as imd_band,
    trim(age_band)                     as age_band,
    try_cast(num_of_prev_attempts as integer) as num_of_prev_attempts,
    try_cast(studied_credits as integer)      as studied_credits,
    case when lower(disability) in ('y','yes','1','true') then true else false end as disability,
    initcap(lower(final_result))               as final_result
  from nomulti
),

no_missing as (
  select * from cleaned
  where id_student is not null
    and gender  is not null
    and region is not null
    and highest_education is not null
    and imd_band is not null
    and age_band  is not null
    and num_of_prev_attempts is not null
    and studied_credits is not null
    and disability is not null
    and final_result is not null
),

validated as (
  select * from no_missing
  where num_of_prev_attempts >= 0
    and studied_credits      >= 0
),

filtered as (
  select * from validated
  where final_result != 'Withdrawn'
)

select * from filtered
