select
    event_id,
    learner_id,
    class_id,
    event_ts as watched_at,
    payload -> 'context' ->> 'registration' as attempt_id,
    payload -> 'object' ->> 'video_id' as video_id,
    payload -> 'object' ->> 'lesson_id' as lesson_id,
    cast(payload -> 'result' ->> 'seconds_watched' as integer) as seconds_watched,
    cast(payload -> 'result' ->> 'video_seconds' as integer) as video_seconds
from {{ source('raw', 'events') }}
where event_type = 'video_watched'
