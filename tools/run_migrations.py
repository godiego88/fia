# tools/run_migrations.py
from __future__ import annotations
import os
import glob
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("SUPABASE_DB_URL")  # full postgres connection URL
if not DB_URL:
    raise RuntimeError("Please set SUPABASE_DB_URL in your environment for run_migrations.py")

def ensure_migrations_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fia_migrations (
            id text PRIMARY KEY,
            applied_at timestamptz DEFAULT now()
        )
        """)
        conn.commit()

def applied_migrations(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM fia_migrations")
        rows = cur.fetchall()
        return {r[0] for r in rows}

def apply_migration(conn, path):
    name = os.path.basename(path)
    with open(path, "r", encoding="utf-8") as f:
        sql_text = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_text)
        cur.execute("INSERT INTO fia_migrations (id) VALUES (%s)", (name,))
    conn.commit()
    print("applied:", name)

def main():
    files = sorted(glob.glob("migrations/*.sql"))
    if not files:
        print("no migrations found")
        return

    conn = psycopg2.connect(DB_URL)
    try:
        ensure_migrations_table(conn)
        applied = applied_migrations(conn)
        for p in files:
            name = os.path.basename(p)
            if name in applied:
                print("skipping already applied:", name)
                continue
            print("applying:", name)
            apply_migration(conn, p)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
