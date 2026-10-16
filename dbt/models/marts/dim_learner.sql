-- SCD type 2: one row per learner per plan period. `valid_to` is the start of the next
-- period (exclusive) and null for the current row. Repeated "changes" to the same plan
-- (e.g. a re-sent webhook) do not open a new version.
with changes as (
    select
        *,
        lag(plan) over (partition by learner_id order by changed_at, event_id) as plan_before
    from {{ ref('stg_plan_changed') }}
),

versions as (
    select * from changes
    where plan_before is null or plan_before <> plan
)

select
    md5(learner_id || '-' || cast(changed_at as varchar)) as learner_sk,
    learner_id,
    class_id,
    email_hash,
    plan,
    row_number() over (partition by learner_id order by changed_at) as version,
    changed_at as valid_from,
    lead(changed_at) over (partition by learner_id order by changed_at) as valid_to,
    lead(changed_at) over (partition by learner_id order by changed_at) is null as is_current
from versions
