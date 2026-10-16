select
    event_id,
    learner_id,
    class_id,
    event_ts as answered_at,
    payload -> 'context' ->> 'registration' as attempt_id,
    payload -> 'object' ->> 'lesson_id' as lesson_id,
    payload -> 'object' ->> 'question_id' as question_id,
    cast(payload -> 'result' ->> 'success' as boolean) as is_correct,
    cast(payload -> 'result' ->> 'score_scaled' as double precision) as score_scaled,
    cast(payload -> 'result' ->> 'response_ms' as integer) as response_ms
from {{ source('raw', 'events') }}
where event_type = 'quiz_answered'
