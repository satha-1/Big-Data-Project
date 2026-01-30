-- Core database (created by POSTGRES_DB env): frauddb
-- Create a separate Airflow metadata database inside the same Postgres instance.
-- This file is executed by the official postgres image via `psql`, so psql meta-commands are allowed.

SELECT format('CREATE DATABASE %I OWNER %I', 'airflow', current_user)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec

-- Fraud alerts: secure sink
CREATE TABLE IF NOT EXISTS fraud_alerts (
  id SERIAL PRIMARY KEY,
  txn_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  merchant_category TEXT NOT NULL,
  amount NUMERIC(18,2) NOT NULL,
  country TEXT NOT NULL,
  city TEXT NOT NULL,
  fraud_type TEXT NOT NULL CHECK (fraud_type IN ('HIGH_SPEND', 'IMPOSSIBLE_TRAVEL')),
  detected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id ON fraud_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_event_time ON fraud_alerts(event_time);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_category ON fraud_alerts(merchant_category);

-- Ingress metrics: total raw ingress by 6-hour bucket (event time)
CREATE TABLE IF NOT EXISTS ingress_metrics (
  id SERIAL PRIMARY KEY,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  total_amount NUMERIC(18,2) NOT NULL,
  txn_count BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingress_metrics_window_end ON ingress_metrics(window_end);

