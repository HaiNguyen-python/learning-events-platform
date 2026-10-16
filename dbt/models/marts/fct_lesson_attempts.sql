-- One row per started lesson attempt, with its outcome and the plan the learner had *when
-- the attempt started* (a point-in-time join to the SCD2 dimension).
with completed as (
    select
        attempt_id,
        min(completed_at) as completed_at,
        max(duration_seconds) as duration_seconds,
        bool_or(is_success) as is_success,
        max(schema_version) as completed_schema_version
    from {{ ref('stg_lesson_completed') }}
    group by attempt_id
),

quiz as (
    select
        attempt_id,
        count(*) as quiz_answers,
        sum(case when is_correct then 1 else 0 end) as quiz_correct,
        avg(score_scaled) as avg_quiz_score
    from {{ ref('stg_quiz_answered') }}
    group by attempt_id
)

select
    s.attempt_id,
    s.learner_id,
    d.learner_sk,
    d.plan as plan_at_start,
    s.class_id,
    s.course_id,
    s.lesson_id,
    s.platform,
    s.started_at,
    c.completed_at,
    c.completed_at is not null as is_completed,
    c.duration_seconds,
    c.is_success,
    c.completed_schema_version,
    coalesce(q.quiz_answers, 0) as quiz_answers,
    coalesce(q.quiz_correct, 0) as quiz_correct,
    q.avg_quiz_score
from {{ ref('stg_lesson_started') }} as s
left join completed as c on s.attempt_id = c.attempt_id
left join quiz as q on s.attempt_id = q.attempt_id
left join {{ ref('dim_learner') }} as d
    on s.learner_id = d.learner_id
    and s.started_at >= d.valid_from
    and (s.started_at < d.valid_to or d.valid_to is null)
