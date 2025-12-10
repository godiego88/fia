from __future__ import annotations
import os, glob
from dotenv import load_dotenv
load_dotenv()

import psycopg2

DB_URL = os.getenv("SUPABASE_DB_URL")
if not DB_URL:
    raise SystemExit("Missing SUPABASE_DB_URL")

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fia_migrations (
        id text PRIMARY KEY,
        applied_at timestamptz DEFAULT now()
    )
    """)
    conn.commit()

    cur.execute("SELECT id FROM fia_migrations")
    applied = {row[0] for row in cur.fetchall()}

    for path in sorted(glob.glob("migrations/*.sql")):
        name = os.path.basename(path)
        if name in applied:
            print("Skipping", name)
            continue

        print("Applying", name)
        sql_text = open(path).read()
        cur.execute(sql_text)
        cur.execute("INSERT INTO fia_migrations (id) VALUES (%s)", (name,))
        conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
