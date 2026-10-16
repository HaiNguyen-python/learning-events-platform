-- Sessionization: a learner's events belong to one session until there is a gap of more than
-- `session_gap_seconds` (30 min). A gap of exactly 30:00 stays in the same session.
-- Built on event time, not arrival time, so late offline events land in the right session.
with learning_events as (
    select learner_id, class_id, event_id, started_at as event_ts from {{ ref('stg_lesson_started') }}
    union all
    select learner_id, class_id, event_id, completed_at from {{ ref('stg_lesson_completed') }}
    union all
    select learner_id, class_id, event_id, answered_at from {{ ref('stg_quiz_answered') }}
    union all
    select learner_id, class_id, event_id, watched_at from {{ ref('stg_video_watched') }}
),

with_gaps as (
    select
        *,
        lag(event_ts) over (partition by learner_id order by event_ts, event_id) as previous_ts
    from learning_events
),

flagged as (
    select
        *,
        case
            when previous_ts is null then 1
            when {{ dbt.datediff('previous_ts', 'event_ts', 'second') }}
                > {{ var('session_gap_seconds') }} then 1
            else 0
        end as is_new_session
    from with_gaps
),

numbered as (
    select
        *,
        sum(is_new_session) over (
            partition by learner_id order by event_ts, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from flagged
)

select
    md5(learner_id || '-' || cast(session_number as varchar)) as session_id,
    learner_id,
    min(class_id) as class_id,
    session_number,
    min(event_ts) as session_start,
    max(event_ts) as session_end,
    {{ dbt.datediff('min(event_ts)', 'max(event_ts)', 'second') }} as duration_seconds,
    count(*) as events
from numbered
group by learner_id, session_number
