# Execution profiles and shared preparation

A profile is an immutable, machine-specific definition of work. It names tool
files, exact argv, environment, dependencies, resource reservations, reports,
artifacts and optional cleanup. Registering it hashes the definition, declared
tools, runner binary and platform. Changed tools or a new runner require a new
registration. Use the returned profile ID in requests.

Commands run in private attempt directories with a cleared environment and
private `HOME` and `TMPDIR`. `PATH` contains declared tool directories and system
directories. Add required non-secret environment values explicitly. Shell
expansion only happens when a command explicitly invokes a declared shell.
Declare adapter scripts and sidecar configuration as tool entries when they are
outside the source revision.

## Prepare once, consume independently

Dependency downloads, generated code, compiled libraries and test fixtures can
be preparation jobs. A downstream job lists their names in `dependencies`.
Patchweft schedules dependencies first and copies their declared artifacts into
each consumer's working directory after checking their hashes.

```json
{
  "version": 1,
  "name": "shared-preparation",
  "certifying": false,
  "tools": {"python": "/usr/bin/python3"},
  "jobs": [
    {
      "name": "prepare",
      "command": ["python", "prepare.py"],
      "artifacts": ["prepared"],
      "report": {"kind": "command"}
    },
    {
      "name": "check-a",
      "dependencies": ["prepare"],
      "command": ["python", "check.py", "a"],
      "report": {"kind": "command"}
    },
    {
      "name": "check-b",
      "dependencies": ["prepare"],
      "command": ["python", "check.py", "b"],
      "report": {"kind": "command"}
    }
  ]
}
```

The [runnable example](../examples/shared-prep/README.md) supplies these scripts,
resolves the local Python path and verifies one preparation plus two consumers.
One consumer changes its copy; the other still sees the original artifact.

Artifacts cannot overwrite source files. Conflicting outputs from dependencies,
symlinks and escaping paths are rejected. Arbitrary installed package directories
often contain symlinks or machine-specific paths. Prefer regular archive files or
downloaded package files, then unpack or install into each consumer's private
workspace. Do not share a mutable installation directory between running jobs.

For Python dependencies, a preparation script can run
`python -m pip download --only-binary=:all: --require-hashes -r requirements.lock -d wheelhouse`.
Consumers can install from that artifact using
`python -m pip install --no-index --find-links wheelhouse --only-binary=:all: --require-hashes -r requirements.lock`
inside a private virtual environment. Supply a complete hash-pinned lockfile,
compatible platform packages and a pinned Python/pip environment. Those package
tools are project prerequisites; Patchweft does not install them automatically.
The wheel-only form avoids an implicit source-build dependency download. See
[pip download](https://pip.pypa.io/en/stable/cli/pip_download/) and
[pip's hash-checking guidance](https://pip.pypa.io/en/stable/topics/secure-installs/)
for the package-manager requirements.

Network fetching should default to fresh execution or in-flight sharing.
Completed reuse needs an audit of package identity, platform, installer, scripts
and external inputs. A URL or lockfile alone does not close every input.
An author workspace is initially a source checkout; preparation is not an automatic
editor setup hook. Local agents can explicitly `deliver ACTION_ID --destination
NEW_DIRECTORY` and use the verified artifact.

## Choose a reuse policy

| Policy | Behavior |
|---|---|
| `inflight` (default) | Identical outstanding work shares one execution. A new direct request after completion starts a fresh observation. |
| `never` | Each request creates an independent action. |
| `exact` | Completed results may be reused for the same complete source commit/history, profile and dependency identities. Requires an audit. |
| `files` | Materializes only explicitly listed regular source files; supports completed reuse across revisions with identical inputs. Requires an audit and no job dependencies. |

Use `inputs` only with `files`, and include every source input, such as both the
lockfile and preparation script. A files-only job has no implicit full checkout.
Reuse remains scoped to the project and profile.

The `audit` field records why completed reuse is sound. The engine checks that
an audit exists, not whether its reasoning is true. Native execution still sees
host libraries and can use the network. Hashing a compiler launcher does not hash
the compiler and all its libraries. Reusing the same result is not a fresh test.

## Validate useful evidence

Every job needs a `report`:

- `command`: a successful process exit is the evidence contract.
- `node-tap`: parse Node test output with a positive `minimum` executed count.
- `vitest`: parse a fresh JSON report at `path`, with a positive `minimum`.
- `junit`: parse a fresh XML report at `path`, with a positive `minimum`.

Test reports reject skips unless `allow_skips` is explicitly true. File reports
are removed before execution and must be freshly produced inside the attempt
workspace. Counts do not establish an exact test roster or sufficient coverage;
those remain part of the project's policy.

Each job can specify `slots`, `memory_mb` and `timeout_seconds`. These default
to 1 slot, 1024 MiB reserved and 1800 seconds. Reservations are admission units,
not hard CPU/RSS limits. Declare a bounded `cleanup` argv for external resources
such as project-owned containers. Uncertain cleanup retains capacity for an operator.

Jobs receive `PATCHWEFT_STATE`, `PATCHWEFT_ATTEMPT`, `PATCHWEFT_REVISION`,
`PATCHWEFT_SLOTS` and `PATCHWEFT_REPORT`. Cleanup receives the state and attempt
identifiers. These are trusted execution hooks, not a permission boundary.
