-- One row per learner per active day (UTC date).
with attempts as (
    select
        learner_id,
        cast(started_at as date) as activity_date,
        count(*) as lessons_started,
        sum(case when is_completed then 1 else 0 end) as lessons_completed,
        sum(quiz_answers) as quiz_answers
    from {{ ref('fct_lesson_attempts') }}
    group by 1, 2
),

sessions as (
    select
        learner_id,
        min(class_id) as class_id,
        cast(session_start as date) as activity_date,
        count(*) as sessions,
        sum(events) as events,
        sum(duration_seconds) / 60.0 as active_minutes
    from {{ ref('int_sessions') }}
    group by 1, 3
)

select
    md5(s.learner_id || '-' || cast(s.activity_date as varchar)) as learner_day_id,
    s.learner_id,
    s.class_id,
    s.activity_date,
    s.sessions,
    s.events,
    s.active_minutes,
    coalesce(a.lessons_started, 0) as lessons_started,
    coalesce(a.lessons_completed, 0) as lessons_completed,
    coalesce(a.quiz_answers, 0) as quiz_answers
from sessions as s
left join attempts as a
    on s.learner_id = a.learner_id and s.activity_date = a.activity_date
