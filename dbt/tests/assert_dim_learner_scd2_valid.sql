-- SCD2 invariants: exactly one current row per learner, and every closed row ends exactly
-- where the next version starts (no gaps, no overlaps).
with ordered as (
    select
        learner_id,
        is_current,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by learner_id order by valid_from) as next_from
    from {{ ref('dim_learner') }}
),

current_rows as (
    select learner_id, sum(case when is_current then 1 else 0 end) as n_current
    from ordered
    group by learner_id
)

select learner_id, 'not exactly one current row' as problem
from current_rows
where n_current <> 1
union all
select learner_id, 'gap or overlap between versions' as problem
from ordered
where valid_to is not null and (next_from is null or valid_to <> next_from)
