{{ config(materialized='view') }}

with raw as (
  select * from {{ source('kaggle_db', 'assessments') }}
),

nomulti as (
  select distinct
    code_module,
    code_presentation,
    id_assessment,
    assessment_type,
    date,
    weight
  from raw
),

parsed as (
  select
    code_module,
    code_presentation,
    id_assessment,
    assessment_type,
    try_cast(date as integer)  as raw_date,
    try_cast(weight as double) as weight
  from nomulti
),

filled as (
  select
    code_module,
    code_presentation,
    id_assessment,
    assessment_type,
    case
      when assessment_type = 'Exam'
        and raw_date is null
      then lag(raw_date)
             over (
               partition by code_module, code_presentation
               order by id_assessment
             ) + 50
      else raw_date
    end as date_filled,
    weight
  from parsed
),

no_missing as (
  select *
  from filled
  where code_module       is not null
    and code_presentation is not null
    and id_assessment     is not null
    and date_filled       is not null
    and weight            is not null
),

validated as (
  select *
  from no_missing
  where weight between 0 and 100
),

filtered as (
  select *
  from validated
  where weight > 0
)

select * from filtered