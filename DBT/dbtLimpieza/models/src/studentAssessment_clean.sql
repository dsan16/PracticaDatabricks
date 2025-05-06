{{ config(materialized = 'view') }}

with raw as (
  select * from {{ source('kaggle_db', 'studentassessment') }}
),

nomulti as (
  select distinct * from raw
),

cleaned as (
  select
    try_cast(id_assessment as integer) as id_assessment,
    try_cast(id_student as integer) as id_student,
    try_cast(date_submitted as integer) as date_submitted,
    (try_cast(is_banked as integer) = 1) as is_banked,
    try_cast(score as float) as score
  from nomulti
),

no_missing as (
  select * from cleaned
  where id_assessment is not null
    and id_student is not null
    and date_submitted is not null
    and score is not null
),

validated as (
  select * from no_missing
  where score between 0 and 100
),

filtered as (
  select * from validated
  where score > 30
)

select * from filtered