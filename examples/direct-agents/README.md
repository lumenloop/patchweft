# Two agents, one changing dependency

This cooperative exercise exposes real Patchweft operations to two participants:
native workspaces/checkpoints and the authenticated remote CLI for discovery,
composition and validation. The provider owns `quote.py`; the consumer owns
`invoice.py` and `ledger.py`. Eight fixed behavioral checks cover their public
results and five independent helpers. The gateway chooses no revisions and
supplies no repair implementation.

Use a temporary directory and a built Patchweft binary:

```sh
python3 examples/direct-agents/session.py setup --root /tmp/patchweft-direct-example --binary target/release/patchweft
python3 examples/direct-agents/session.py host --root /tmp/patchweft-direct-example
```

The host runs for at most 15 minutes. In another terminal, send one JSON packet
on stdin to a participant gateway call:

```sh
echo '{"op":"query","data":{"op":"projects"}}' | python3 examples/direct-agents/session.py act --root /tmp/patchweft-direct-example --role consumer
```

`info` returns the base and profile identifiers. `query` and `command` forward
their `data` to the real API; mutations also need a stable `key`. `workspace`
takes a revision. `read`/`write` take a workspace and file (`write` also takes
text). `checkpoint` takes a workspace and message. The source code contains
the complete small packet vocabulary. Calls and results append to `calls.jsonl`.
Credential files are passed to the official CLI, never printed by this helper.

A useful participant sequence is:

1. Consumer discovers the project, makes both consumers call the flat quote
   interface, checkpoints and requests validation of that exact revision.
2. Consumer appends an unfinished author note, checkpoints it, and tells provider.
3. Provider changes the quote to nested `money` with `minor_units` and `currency`,
   checkpoints, and announces that revision.
4. Consumer discovers that revision, composes it with the draft, requests checks,
   reads the failed log, creates a new repair workspace, then checkpoints and
   validates the repaired composition while preserving the note and original view.

Queries return their result directly; commands return an API envelope. Log bytes
are base64 encoded. Failed actions have no deliverable artifacts: their artifact
query currently returns a sanitized internal error. Passing actions have retained
artifacts with source/attempt identity.

The parent can end the host with `seal --root ...`. Sealing records elapsed time
without resetting the clock. These helpers do not review, promote or export code.
No Git mutation command is exposed to participants.

This is instrumentation for trusted synthetic participants, not a sandbox:
native source commands retain local Unix-account authority, and job code runs
under that account. Gateway calls serialize. Scheduler memory values are
reservations, not enforced memory limits. For unattended use, wrap both `host`
and each separate `act` process in a descendant-owning timeout supervisor; the
Linux helper in `examples/control-isolation/owned_process.py` is one example.
The host process alone cannot supervise calls started by another terminal.
