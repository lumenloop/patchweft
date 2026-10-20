# Shared preparation

From the repository root:

```bash
cargo build --release --locked
python3 examples/shared-prep/run.py --binary ./target/release/patchweft
```

The driver creates an isolated temporary Git project and state store. Two agents
change different source files, checkpoint their work, and compose both revisions.
It registers a profile with one preparation job and two downstream checks, then
adds a second request for one check before starting the worker.

`prepare.py` generates a message from both source files. `check.py` verifies the
combined result in each consumer. Consumer A changes its artifact copy; consumer B
must still receive the original. The driver checks that only three attempts
started and verifies both delivered result files.

Expected output includes `"result": "passed"`, one preparation execution,
two consumer executions, and successful sharing and private-copy checks.
The example uses Python's standard library, local Git and the Patchweft binary;
it makes no network requests. Temporary data is removed when the driver exits.

The profile is exploratory: the example does not assert that a human review
occurred or advance a delivery frontier. The scripts are intentionally small;
adapt the preparation step to your project's downloaded dependencies, generated
sources or build outputs. See [profiles](../../docs/profiles.md) for dependency
downloads and the limits on completed-result reuse.
