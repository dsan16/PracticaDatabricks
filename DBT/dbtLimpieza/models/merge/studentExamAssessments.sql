{{ config(materialized = 'view') }}

with 
  a as (
    select * from {{ ref('assessments_clean') }}
  ),
  s as (
    select * from {{ ref('studentAssessment_clean') }}
  )

select
  a.code_module,
  a.code_presentation,
  a.id_assessment,
  a.date_filled,
  s.id_student,
  s.date_submitted,
  s.score
from a
join s using (id_assessment)
where a.assessment_type = 'Exam'
