# fia/supabase_client.py
"""
Supabase Client (Final — Supabase v2 Compatible)

Handles creation of a Supabase client using environment variables.
This version works with supabase-py v2.16+.
"""

from __future__ import annotations
import os
from supabase import create_client
from dotenv import load_dotenv

# Load .env if present
load_dotenv()


def get_supabase():
    """
    Return a fully initialized Supabase client (v2).
    Requires SUPABASE_URL and SUPABASE_KEY in environment variables.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in environment.")

    client = create_client(url, key)
    return client
