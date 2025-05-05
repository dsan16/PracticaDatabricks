{{ config(materialized = 'view') }}

with
students as (
  select
    code_module,
    code_presentation,
    id_student,
    highest_education
  from {{ ref('studentInfo_clean') }}
),

students_distinct as (
  select distinct
    code_module,
    code_presentation,
    id_student,
    highest_education
  from students
),

by_education as (
  select
    code_module,
    code_presentation,
    highest_education,
    count(*) as students_by_education
  from students_distinct
  group by 1,2,3
),

with_totals as (
  select
    code_module,
    code_presentation,
    highest_education,
    students_by_education,
    sum(students_by_education)
      over (partition by code_module, code_presentation) as total_students
  from by_education
)

select
  c.code_module,
  c.code_presentation,
  c.presentation_length,
  w.total_students,
  w.highest_education,
  w.students_by_education
from with_totals w
join {{ ref('courses_clean') }} c
  on w.code_module = c.code_module
 and w.code_presentation = c.code_presentation
order by
  c.code_module,
  c.code_presentation,
  w.highest_education