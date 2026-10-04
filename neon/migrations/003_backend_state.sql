-- Remaining Hail Money backend state moved off Firestore.
-- Server-only tables. Firebase remains authentication only.

CREATE TABLE IF NOT EXISTS public.abc_supply_connections (
  uid text PRIMARY KEY,
  org_id text NOT NULL,
  data jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS abc_supply_connections_org_idx
  ON public.abc_supply_connections(org_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.abc_user_onboarding (
  uid text PRIMARY KEY,
  org_id text NOT NULL,
  data jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS abc_user_onboarding_org_idx
  ON public.abc_user_onboarding(org_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.abc_oauth_states (
  state text PRIMARY KEY,
  org_id text NOT NULL,
  uid text NOT NULL,
  data jsonb NOT NULL DEFAULT '{}'::jsonb,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS abc_oauth_states_expires_idx
  ON public.abc_oauth_states(expires_at);

CREATE TABLE IF NOT EXISTS public.ai_estimator_usage (
  org_id text NOT NULL,
  uid_hash text NOT NULL,
  day date NOT NULL,
  request_count integer NOT NULL DEFAULT 0,
  current_hour text NOT NULL DEFAULT '',
  current_hour_count integer NOT NULL DEFAULT 0,
  reserved_cost_usd numeric NOT NULL DEFAULT 0,
  input_tokens bigint NOT NULL DEFAULT 0,
  output_tokens bigint NOT NULL DEFAULT 0,
  actual_cost_usd numeric NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(org_id, uid_hash, day)
);
CREATE INDEX IF NOT EXISTS ai_estimator_usage_org_day_idx
  ON public.ai_estimator_usage(org_id, day DESC);
