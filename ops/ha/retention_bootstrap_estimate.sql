-- SELECT-only pre-v9 estimate. Matches 0009 bootstrap with its generic defaults.
-- Canonical JSON bytes are logical reservations, NOT heap/TOAST/index/WAL size.
-- This does not set policy or assume that a 320 MiB production budget is safe.
WITH attempt_bytes AS (
  SELECT task_id,
    coalesce(sum(coalesce(octet_length(error::text),0)),0)::bigint AS historical_errors,
    coalesce(max(coalesce(octet_length(error::text),0)),0)::bigint AS largest_error
  FROM public.attempts GROUP BY task_id
), task_bytes AS (
  SELECT t.tenant_id,t.status,t.max_attempts,
    CASE WHEN t.status='completed' AND t.body='{}'::jsonb AND t.response IS NULL
      THEN 0 ELSE coalesce(octet_length(t.body::text),0)::bigint END AS body_bytes,
    coalesce(octet_length(t.response::text),0)::bigint AS response_bytes,
    coalesce(octet_length(t.error::text),0)::bigint AS task_error_bytes,
    coalesce(a.historical_errors,0)::bigint AS historical_errors,
    greatest(32768,coalesce(octet_length(t.error::text),0),coalesce(a.largest_error,0))::bigint AS error_cap
  FROM public.tasks t LEFT JOIN attempt_bytes a ON a.task_id=t.id
), charges AS (
  SELECT *,
    CASE WHEN status IN ('pending','running') THEN greatest(1048576,response_bytes)
      ELSE response_bytes END AS response_charge,
    CASE WHEN status IN ('pending','running') THEN
      (max_attempts::bigint+1)*error_cap+historical_errors
      ELSE task_error_bytes+historical_errors END AS error_charge
  FROM task_bytes
), totals AS (
  SELECT n.id AS tenant_id,count(c.tenant_id) AS retained_tasks,
    count(*) FILTER(WHERE c.status IN ('pending','running')) AS active_tasks,
    coalesce(sum(body_bytes),0)::bigint AS body_charge_bytes,
    coalesce(sum(response_charge),0)::bigint AS response_charge_bytes,
    coalesce(sum(error_charge),0)::bigint AS error_charge_bytes,
    coalesce(sum(body_bytes+response_charge+error_charge),0)::bigint AS bootstrap_payload_charge_bytes
  FROM public.tenants n LEFT JOIN charges c ON c.tenant_id=n.id GROUP BY n.id
)
SELECT *,335544320::bigint-bootstrap_payload_charge_bytes AS headroom_at_320_mib,
  50000::bigint-retained_tasks AS headroom_at_50000_tasks,
  117964800::bigint AS minimum_100_new_default_tasks_excluding_bodies,
  335544320::bigint-bootstrap_payload_charge_bytes-117964800::bigint
    AS remaining_after_100_minimum_reservations_before_new_bodies
FROM totals ORDER BY tenant_id;
