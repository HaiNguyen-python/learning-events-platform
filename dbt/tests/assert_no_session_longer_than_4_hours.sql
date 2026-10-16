-- A learning session longer than 4 hours means the sessionization is wrong (or a device
-- clock is broken). Returns the offending sessions; dbt fails the test if any row comes back.
select session_id, learner_id, session_start, session_end, duration_seconds
from {{ ref('int_sessions') }}
where duration_seconds > 4 * 3600
