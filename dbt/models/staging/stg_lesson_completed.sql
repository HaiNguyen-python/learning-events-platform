-- v1 and v2 events in one model. v2 added two optional fields, so v1 rows get nulls:
-- this is what "backward compatible" buys downstream (no model change was needed).
select
    event_id,
    learner_id,
    class_id,
    event_ts as completed_at,
    schema_version,
    payload -> 'context' ->> 'registration' as attempt_id,
    payload -> 'object' ->> 'lesson_id' as lesson_id,
    cast(payload -> 'result' ->> 'duration_seconds' as integer) as duration_seconds,
    cast(payload -> 'result' ->> 'success' as boolean) as is_success,
    cast(payload -> 'result' ->> 'score_scaled' as double precision) as score_scaled,
    payload -> 'context' ->> 'app_version' as app_version
from {{ source('raw', 'events') }}
where event_type = 'lesson_completed'
