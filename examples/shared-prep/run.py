#!/usr/bin/env python3
"""Run the complete shared-preparation example using only temporary synthetic data."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def execute(argv):
    result = subprocess.run(argv, text=True, capture_output=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} failed:\n{result.stderr}\n{result.stdout}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/patchweft"))
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    example = Path(__file__).resolve().parent
    python = str(Path(sys.executable).resolve())
    with tempfile.TemporaryDirectory(prefix="patchweft-example-") as temporary:
        root = Path(temporary)
        repo = root / "source"
        repo.mkdir()
        state = root / "state"

        def git(*arguments):
            return execute([
                "/usr/bin/git", "-C", str(repo),
                "-c", "user.name=Example", "-c", "user.email=example@local.invalid",
                "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                *arguments,
            ]).strip()

        def call(*arguments):
            return json.loads(execute([str(binary), "--state", str(state), *arguments]))

        git("init", "--quiet")
        for filename in ["prepare.py", "check.py"]:
            shutil.copyfile(example / filename, repo / filename)
        (repo / "greeting.json").write_text('{"greeting":"Hello"}\n')
        (repo / "punctuation.json").write_text('{"punctuation":"."}\n')
        git("add", ".")
        git("commit", "--quiet", "-m", "Initial example")
        call("init", "--slots", "2", "--memory-mb", "2048")
        base = call("import", "--name", "example", "--repo", str(repo), "--owner", "coordinator")
        changes = []
        for owner, filename, value in [
            ("agent-a", "greeting.json", {"greeting": "Welcome"}),
            ("agent-b", "punctuation.json", {"punctuation": "!"}),
        ]:
            workspace = call("workspace", "--revision", base["id"], "--owner", owner)
            (Path(workspace["path"]) / filename).write_text(json.dumps(value) + "\n")
            changes.append(call("checkpoint", workspace["id"], "--message", f"Edit {filename}")["id"])
        composed = call(
            "compose", "--base", base["id"], "--change", changes[0],
            "--change", changes[1], "--owner", "integrator",
        )
        profile_path = root / "profile.json"
        profile_path.write_text(json.dumps({
            "version": 1, "name": "shared-preparation", "certifying": False,
            "tools": {"python": python},
            "jobs": [
                {"name": "prepare", "command": ["python", "prepare.py"],
                 "artifacts": ["prepared"], "report": {"kind": "command"}},
                *[
                    {"name": f"check-{consumer}", "dependencies": ["prepare"],
                     "command": ["python", "check.py", consumer],
                     "artifacts": ["out"], "report": {"kind": "command"}}
                    for consumer in ["a", "b"]
                ],
            ],
        }))
        profile = call("register", str(profile_path))
        candidate = call(
            "submit", "--revision", composed["id"], "--profile", profile["id"],
            "--owner", "integrator",
        )
        shared = call(
            "request", "--revision", composed["id"], "--profile", profile["id"],
            "--job", "check-b", "--owner", "second-requester",
        )
        assert shared["id"] == candidate["required"]["check-b"], "outstanding work was duplicated"
        call("worker", "--drain")
        status = call("status", candidate["id"])
        assert status["state"] == "passed", json.dumps(status)
        events = call("events")
        started = [event for event in events if event["kind"] == "attempt.started"]
        assert len(started) == 3, f"expected one preparation and two consumers, got {len(started)}"
        for consumer in ["a", "b"]:
            destination = root / f"delivery-{consumer}"
            call("deliver", candidate["required"][f"check-{consumer}"], "--destination", str(destination))
            received = json.loads((destination / "out" / f"{consumer}.json").read_text())
            assert received == {"consumer": consumer, "passed": True}
        print(json.dumps({
            "result": "passed",
            "agent_changes_composed": 2,
            "preparation_executions": 1,
            "consumer_executions": 2,
            "duplicate_request_shared": True,
            "private_artifact_copies_verified": True,
        }, indent=2))


if __name__ == "__main__":
    main()
