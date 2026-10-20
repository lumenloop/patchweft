"""Each consumer verifies, then optionally changes, its private artifact copy."""
import json
from pathlib import Path
import sys

consumer = sys.argv[1]
message = json.loads(Path("prepared/message.json").read_text())["message"]
if message != "Welcome, Patchweft!":
    raise SystemExit(f"{consumer}: unexpected composition: {message}")
if consumer == "a":
    Path("prepared/message.json").write_text('{"message":"private edit"}\n')
Path("out").mkdir()
Path(f"out/{consumer}.json").write_text(json.dumps({"consumer": consumer, "passed": True}) + "\n")
