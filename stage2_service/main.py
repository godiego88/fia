from __future__ import annotations
from fastapi import FastAPI, HTTPException
import uvicorn
import uuid, json
from datetime import datetime, timezone
from fia.config_loader import get_config
from runners.stage2_runner import main as run_stage2_local

app = FastAPI()

@app.post("/run_stage2")
def run_stage2_endpoint(payload: dict):
    if "signals" not in payload:
        raise HTTPException(status_code=400, detail="Missing 'signals' array")
    
    # Write the incoming trigger context
    cfg = get_config()
    path = cfg.paths.trigger_context_path
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # Run stage2 locally (reusing your exact runner)
    run_stage2_local()

    # Return the generated deep_results.json
    with open(cfg.paths.deep_results_path, "r", encoding="utf-8") as f:
        results = json.load(f)

    return results


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
