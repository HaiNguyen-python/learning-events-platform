-- Teacher view: one row per class per ISO week (Monday start).
-- "At risk" = enrolled and no learning activity in the 7 days up to the week's last day.
-- With a 7-day week this equals "enrolled but not active this week"; the HTML report lists
-- who those learners are.
with weeks as (
    select distinct cast({{ dbt.date_trunc('week', 'activity_date') }} as date) as week_start
    from {{ ref('fct_daily_learner_activity') }}
),

learners as (
    select learner_id, min(class_id) as class_id, min(valid_from) as signed_up_at
    from {{ ref('dim_learner') }}
    group by learner_id
),

enrolled as (
    select
        l.class_id,
        l.learner_id,
        w.week_start,
        cast({{ dbt.dateadd('day', 6, 'w.week_start') }} as date) as week_last_day
    from learners as l
    cross join weeks as w
    where cast(l.signed_up_at as date) <= cast({{ dbt.dateadd('day', 6, 'w.week_start') }} as date)
),

last_seen as (
    select
        e.class_id,
        e.learner_id,
        e.week_start,
        e.week_last_day,
        max(a.activity_date) as last_active_date
    from enrolled as e
    left join {{ ref('fct_daily_learner_activity') }} as a
        on e.learner_id = a.learner_id and a.activity_date <= e.week_last_day
    group by 1, 2, 3, 4
),

risk as (
    select
        class_id,
        week_start,
        count(*) as enrolled_learners,
        sum(
            case
                when last_active_date is null then 1
                when {{ dbt.datediff('last_active_date', 'week_last_day', 'day') }} >= 7 then 1
                else 0
            end
        ) as learners_at_risk
    from last_seen
    group by 1, 2
),

activity as (
    select
        class_id,
        cast({{ dbt.date_trunc('week', 'activity_date') }} as date) as week_start,
        count(distinct learner_id) as active_learners
    from {{ ref('fct_daily_learner_activity') }}
    group by 1, 2
),

attempts as (
    select
        class_id,
        cast({{ dbt.date_trunc('week', 'started_at') }} as date) as week_start,
        count(*) as lessons_started,
        sum(case when is_completed then 1 else 0 end) as lessons_completed
    from {{ ref('fct_lesson_attempts') }}
    group by 1, 2
),

quiz as (
    select
        class_id,
        cast({{ dbt.date_trunc('week', 'answered_at') }} as date) as week_start,
        avg(score_scaled) as avg_quiz_score
    from {{ ref('stg_quiz_answered') }}
    group by 1, 2
)

select
    md5(r.class_id || '-' || cast(r.week_start as varchar)) as class_week_id,
    r.class_id,
    r.week_start,
    r.enrolled_learners,
    coalesce(a.active_learners, 0) as active_learners,
    coalesce(t.lessons_started, 0) as lessons_started,
    coalesce(t.lessons_completed, 0) as lessons_completed,
    case
        when coalesce(t.lessons_started, 0) = 0 then null
        else 1.0 * t.lessons_completed / t.lessons_started
    end as completion_rate,
    q.avg_quiz_score,
    r.learners_at_risk
from risk as r
left join activity as a on r.class_id = a.class_id and r.week_start = a.week_start
left join attempts as t on r.class_id = t.class_id and r.week_start = t.week_start
left join quiz as q on r.class_id = q.class_id and r.week_start = q.week_start
