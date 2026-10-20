# Contributing

Use Linux, the Rust toolchain pinned in `rust-toolchain.toml`, a C/C++ build
toolchain, Git at `/usr/bin/git`, Python 3 at `/usr/bin/python3`, `/usr/bin/sh`,
and `setsid` from util-linux. Put Node.js 22 on `PATH`: the workflow tests run
a real JavaScript provider/consumer contract. Node is a development-test
dependency, not a requirement for every Patchweft project.

```bash
rustup component add rustfmt clippy
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked -- --test-threads=2
cargo build --release --locked
python3 examples/shared-prep/run.py --binary ./target/release/patchweft
```

Tests use temporary, synthetic projects. Workflow tests exercise native processes,
signals and Linux process supervision, so a sandbox that forbids those operations
cannot prove their behavior. Run on a trusted development host.

For behavior changes, add a focused regression that would fail without the fix.
Keep failure and cancellation checks meaningful; do not weaken existing assertions
to accommodate a failing change. Report which commands actually ran and any
prerequisite that prevented verification.

Use an issue or pull request to describe the problem, intended behavior and
validation. Keep source, credentials, deployment details and private workload
evidence out of reports. Generic reproductions and scripts others can run are
the most useful evidence.

Version 0.1 APIs, state format and profiles may change. Call out compatibility
effects in changes to them. Contributions are provided under the repository's
MIT license.
