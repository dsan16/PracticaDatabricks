{{ config(materialized = 'view') }}

with raw as (
  select * from {{ source('kaggle_db', 'vle') }}
),

nomulti as (
  select distinct
    id_site,
    code_module,
    code_presentation,
    activity_type,
    week_from,
    week_to
  from raw
),

cleaned as (
  select
    cast(id_site               as integer)      as id_site,
    upper(trim(code_module))                   as code_module,
    upper(trim(code_presentation))             as code_presentation,
    lower(trim(activity_type))                 as activity_type,
    case when week_from is null then null
         else cast(week_from as integer)
    end                                        as week_from,
    case when week_to   is null then null
         else cast(week_to   as integer)
    end                                        as week_to
  from nomulti
),

no_missing as (
  select * from cleaned
  where id_site           is not null
    and code_module       is not null
    and code_presentation is not null
    and activity_type     is not null
),

validated as (
  select * from no_missing
  where week_from >= 0
    and (week_to is null or week_to >= week_from)
),

filtered as (
  select * from validated
  where week_from is not null
)

select * from filtered