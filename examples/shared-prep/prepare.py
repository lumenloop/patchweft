"""Produce one regular-file artifact from the composed source."""
import json
from pathlib import Path

greeting = json.loads(Path("greeting.json").read_text())["greeting"]
punctuation = json.loads(Path("punctuation.json").read_text())["punctuation"]
Path("prepared").mkdir()
Path("prepared/message.json").write_text(
    json.dumps({"message": f"{greeting}, Patchweft{punctuation}"}) + "\n"
)
