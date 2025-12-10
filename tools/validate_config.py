import json
import jsonschema
from jsonschema import validate
from pathlib import Path

schema_path = Path("config/schema.json")
defaults_path = Path("config/defaults.json")

schema = json.loads(schema_path.read_text())
defaults = json.loads(defaults_path.read_text())

validate(defaults, schema)

print("config validation OK")
