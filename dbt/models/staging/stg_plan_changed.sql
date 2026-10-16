-- Signups and plan changes from the billing backend; the input for the SCD2 dim_learner.
select
    event_id,
    learner_id,
    class_id,
    email_hash,
    event_ts as changed_at,
    payload -> 'object' ->> 'plan' as plan,
    payload -> 'result' ->> 'previous_plan' as previous_plan
from {{ source('raw', 'events') }}
where event_type = 'plan_changed'
