import re
from pathlib import Path

runner_files = [
    Path("runners/stage1_runner.py"),
    Path("runners/stage2_runner.py"),
    Path("runners/reconcile_runner.py")
]

missing = []

for f in runner_files:
    text = f.read_text()
    if "load_dotenv()" not in text:
        missing.append(str(f))

if missing:
    print("FAIL: These runners are missing load_dotenv():")
    for m in missing:
        print(" -", m)
else:
    print("OK: All runners contain load_dotenv() as required.")
