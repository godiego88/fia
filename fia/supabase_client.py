import os
from typing import Any, Dict, Optional
from supabase import create_client, Client


class SupabaseClient:
    """
    Central Supabase wrapper for FIA.
    Handles:
    - Config fetch (used by config_loader)
    - Run logging (stage1, stage2)
    - Reservation lifecycle
    - Usage tracking for cost guardrails
    """

    def __init__(self):
        url = os.getenv("SUPABASE_DB_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise RuntimeError(
                "Missing Supabase environment vars: SUPABASE_DB_URL / SUPABASE_SERVICE_ROLE_KEY"
            )

        self.client: Client = create_client(url, key)

    # -------------------------------------------------------------
    # CONFIG TABLE
    # -------------------------------------------------------------
    def fetch_config_rows(self) -> Dict[str, Any]:
        """Returns { key: value } for all rows."""
        res = self.client.table("config").select("*").execute()
        rows = res.data or []

        out = {}
        for row in rows:
            out[row["key"]] = row["value"]

        return out

    # -------------------------------------------------------------
    # RUN LOGGING
    # -------------------------------------------------------------
    def log_stage1_run(self, metadata: Dict[str, Any]) -> None:
        self.client.table("stage1_runs").insert(metadata).execute()

    def log_stage2_run(self, metadata: Dict[str, Any]) -> None:
        self.client.table("stage2_runs").insert(metadata).execute()

    # -------------------------------------------------------------
    # STAGE ARTIFACT STORAGE
    # -------------------------------------------------------------
    def store_stage1_artifact(self, run_id: str, artifact: Dict[str, Any]) -> None:
        """Stores trigger_context.json contents."""
        self.client.table("stage1_artifacts").insert({
            "run_id": run_id,
            "artifact": artifact
        }).execute()

    def store_stage2_artifact(self, run_id: str, artifact: Dict[str, Any]) -> None:
        """Stores deep_results.json contents."""
        self.client.table("stage2_artifacts").insert({
            "run_id": run_id,
            "artifact": artifact
        }).execute()

    # -------------------------------------------------------------
    # RESERVATION SYSTEM
    # -------------------------------------------------------------
    def create_reservation(self, run_id: str, ttl_hours: int) -> None:
        """Creates a time-limited Stage 2 reservation."""
        self.client.table("reservations").insert({
            "run_id": run_id,
            "ttl_hours": ttl_hours
        }).execute()

    def get_active_reservation(self) -> Optional[Dict[str, Any]]:
        """Return the most recent active reservation (if any)."""
        res = (
            self.client
            .rpc("get_active_reservation")   # using stored function
            .execute()
        )
        if res.data:
            return res.data[0]
        return None

    def expire_reservation(self, reservation_id: int) -> None:
        self.client.table("reservations").update({
            "expired": True
        }).eq("id", reservation_id).execute()

    # -------------------------------------------------------------
    # COST & USAGE TRACKING
    # -------------------------------------------------------------
    def insert_usage(self, stage: str, usd_cost: float, details: Dict[str, Any]) -> None:
        """Records usage cost per run (used by guardrails)."""
        self.client.table("usage").insert({
            "stage": stage,
            "usd_cost": usd_cost,
            "details": details
        }).execute()

    def get_monthly_cost(self) -> float:
        """Returns sum(usd_cost) for the current month."""
        res = self.client.rpc("get_monthly_cost").execute()
        return float(res.data or 0.0)

    # -------------------------------------------------------------
    # UTILITY
    # -------------------------------------------------------------
    def heartbeat(self) -> bool:
        """Simple test query to confirm DB connectivity."""
        try:
            self.client.table("config").select("key").limit(1).execute()
            return True
        except:
            return False


# Singleton accessor
_supabase_instance: Optional[SupabaseClient] = None


def get_supabase() -> SupabaseClient:
    global _supabase_instance
    if _supabase_instance is None:
        _supabase_instance = SupabaseClient()
    return _supabase_instance
