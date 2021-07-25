{{
  config(
    schema='product',
    materialized='incremental',
    incremental_strategy = 'merge',
    unique_key='primary_key'
  )
}}

select
  concat(id, '-', current_date()) as primary_key,
  current_date() as date,
  id as transformation_id,
  created_at,
  created_by_id, 
  group_id1,
  name,
  paused,
  trigger,
  last_started_at,
  status
from `singular-vector-135519`.pg_public.transformations
where not _fivetran_deleted



