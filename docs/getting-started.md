# Start with a Git project

Build Patchweft as described in the [README](../README.md). Put the binary on
`PATH`, or replace `patchweft` below with its absolute path. Use a private,
persistent state directory on a local filesystem. All operators and workers for
that store must use the same binary and host.

## Import and edit

```bash
patchweft --state /path/to/state init --slots 4 --memory-mb 8192
patchweft --state /path/to/state import --name my-project \
  --repo /path/to/my-project --owner coordinator
```

The repository must already have a commit. Import captures committed source;
it does not publish uncommitted edits. Save the returned revision `id` as your
base and the `project` ID for API permissions.

```bash
patchweft --state /path/to/state workspace --revision BASE_ID --owner agent-a
```

Edit the returned workspace `path` using your normal tools and project instructions.
Then publish the working edits:

```bash
patchweft --state /path/to/state checkpoint WORKSPACE_ID --message "Add feature"
```

Save its revision `id`. The checkpoint leaves the editing workspace's Git HEAD
and staging alone. Further checkpoints retain revision ancestry.

Another agent can create a workspace from that revision, or both agents can work
from the same base and compose their published changes:

```bash
patchweft --state /path/to/state compose --base BASE_ID \
  --change REVISION_A --change REVISION_B --owner integrator
```

Text conflicts create repair records. A successful text composition returns a new
revision; it still needs validation.

## Register and run a profile

Profiles contain exact argument arrays and absolute tool paths. This small example
assumes your project contains a `verify.py` script which exits nonzero on failure:

```json
{
  "version": 1,
  "name": "project-checks",
  "certifying": false,
  "tools": {"python": "/usr/bin/python3"},
  "jobs": [{
    "name": "verify",
    "command": ["python", "verify.py"],
    "slots": 1,
    "memory_mb": 1024,
    "timeout_seconds": 120,
    "report": {"kind": "command"}
  }]
}
```

Save it outside your source tree as `profile.json`, adjust the tool and command
for your project, then register it:

```bash
patchweft --state /path/to/state register profile.json
patchweft --state /path/to/state submit --revision REVISION_ID \
  --profile PROFILE_ID --owner agent-a
patchweft --state /path/to/state worker --drain --no-loopback-check
patchweft --state /path/to/state status CANDIDATE_ID
```

`--no-loopback-check` is for a machine whose jobs run no containers. A host whose
jobs use rootless Docker runs `worker` without it, so job containers can never reach
an unlisted loopback service ([operations](operations.md)).

Use IDs returned by the preceding commands. `worker --drain` exits when runnable
work finishes. A long-running `worker` command also waits for future requests.
Run it in a separate terminal when agents submit continuously.

Use `request --revision ID --profile ID --job NAME --owner NAME` for a single
job and its dependencies. `submit` registers all obligations in the profile.
Add `--requires CANDIDATE_ID` only for a real prerequisite, including a candidate
from another project. Without it, candidates are independent.

Resubmitting the same revision/profile/prerequisite set returns the existing
candidate; it does not force a new observation. A new direct `request` follows
the job's reuse policy. For failed or interrupted work:

```bash
patchweft --state /path/to/state repairs
patchweft --state /path/to/state logs ATTEMPT_ID
patchweft --state /path/to/state retry CANDIDATE_ID --job verify
```

Fix source by publishing a new revision and validating it. A retry alone does
not change the source of the old candidate.

If a job needs a larger reservation, such as more `memory_mb` after running out
of memory, register a profile with that change and submit the same revision
with `--retain-from CANDIDATE_ID`. Passed jobs whose identity is unchanged are
kept as earlier observations; failed jobs and their consumers run again under
the new reservation. See [retention](profiles.md#retain-passed-obligations-after-a-profile-change).

## Review and advance

An exploratory profile cannot advance the validated frontier. Once you have
chosen sufficient validation obligations, set `certifying` to `true`, register
that profile, and submit the intended revision under the returned new profile ID.
Exploratory evidence never serves a certifying candidate, so this reruns every
job, even with `--retain-from`.

After validation passes and an authorized reviewer has inspected that exact revision:

```bash
patchweft --state /path/to/state review REVISION_ID --reviewer reviewer-name
patchweft --state /path/to/state promote CANDIDATE_ID
```

The first promotion omits `--expected`. Later promotions must include
`--expected PREVIOUS_CANDIDATE_ID` and contain the current frontier's source
history. Review is a recorded assertion; Patchweft does not perform the review.

Export to a new local bare Git repository:

```bash
git init --bare /path/to/export.git
patchweft --state /path/to/state export REVISION_ID \
  --destination /path/to/export.git --reference refs/heads/validated
```

For an existing export ref, include `--expected PREVIOUS_COMMIT_SHA`.
The export command copies an explicit revision; it does not itself require
frontier certification. An operator should export the certified revision when
that is the intended delivery policy. Network publication is a separate operation.

See [connections](connections.md) for agents on other machines, and
[profiles](profiles.md) for preparation jobs, report validation and safe reuse.
