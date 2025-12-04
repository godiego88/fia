# FIA Configuration Layer

This folder contains all configuration files for FIA.

## Files

### `defaults.json`
Baseline configuration used by FIA runtimes. These values are also stored in Supabase under the `config` table.

### `schema.json`
JSON Schema definition used for validating any configuration updates.  
Keeps config strict, typed, and predictable.

### `secrets_template.json`
List of **all required secrets** for FIA.  
No real secrets belong here—only placeholders.  
Real secrets are stored in GitHub Secrets.

## Config Keys Explained

- **cost_guardrails**
  - `monthly_hard_stop`: Maximum monthly Fly.io budget in USD.
  - `reservation_ttl_hours`: Validity window for Fly.io job reservations.

- **run_settings**
  - `dry_run`: Whether FIA is allowed to call paid APIs.
  - `max_concurrent_fly_jobs`: Safety cap to prevent runaway compute jobs.

- **trigger_thresholds**
  - Defines thresholds for Stage 1 signal filtering.

- **paths**
  - Local filenames for artifacts produced by Stage 1 and Stage 2.

This configuration layer ensures FIA stays predictable, safe, and easy to maintain.
