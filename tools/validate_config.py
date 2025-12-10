import json
import jsonschema

schema = json.load(open("config/schema.json"))
config = json.load(open("config/defaults.json"))

jsonschema.validate(config, schema)
print("config validation OK")
