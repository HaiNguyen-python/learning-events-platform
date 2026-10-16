-- One row per lesson attempt start. `attempt_id` is xAPI context.registration: every event
-- of the same attempt carries it, which makes attempts easy to join later.
select
    event_id,
    learner_id,
    class_id,
    event_ts as started_at,
    payload -> 'context' ->> 'registration' as attempt_id,
    payload -> 'object' ->> 'lesson_id' as lesson_id,
    payload -> 'object' ->> 'course_id' as course_id,
    payload -> 'context' ->> 'platform' as platform
from {{ source('raw', 'events') }}
where event_type = 'lesson_started'
