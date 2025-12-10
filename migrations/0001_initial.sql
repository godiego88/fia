-- migrations/0001_initial.sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS run_log (
  run_id text PRIMARY KEY,
  stage text,
  meta jsonb,
  started_at timestamptz,
  ended_at timestamptz,
  success boolean,
  details jsonb
);

CREATE TABLE IF NOT EXISTS signals (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id text,
  ticker text,
  payload jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id text,
  ticker text,
  payload jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fia_configs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  active boolean DEFAULT false,
  config jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monthly_usage (
  month text PRIMARY KEY,
  cpu_minutes_estimated double precision DEFAULT 0,
  cpu_minutes_actual double precision DEFAULT 0,
  cost_estimated double precision DEFAULT 0,
  cost_actual double precision DEFAULT 0,
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fly_reservations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id text,
  estimated_cpu_minutes double precision,
  status text,
  created_at timestamptz DEFAULT now(),
  expires_at timestamptz
);

CREATE TABLE IF NOT EXISTS anomaly_rollups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  day text,
  summary jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_run_log_started_at ON run_log (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_signals_ticker ON signals (ticker);
CREATE INDEX IF NOT EXISTS ix_results_ticker ON results (ticker);
CREATE INDEX IF NOT EXISTS ix_reservations_status ON fly_reservations (status);
CREATE INDEX IF NOT EXISTS ix_anomaly_day ON anomaly_rollups (day);
