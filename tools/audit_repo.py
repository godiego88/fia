import re, sys, pathlib

base = pathlib.Path("runners")
fail = False

for f in base.glob("*_runner.py"):
    text = f.read_text()
    if "load_dotenv()" not in text.split("\n")[0:10]:
        print(f"ERROR: {f} missing load_dotenv() at top")
        fail = True

if fail:
    sys.exit(1)

print("All runners OK.")
