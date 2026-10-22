# Operating Patchweft

Version 0.1 supports one Linux host and a local filesystem. It uses SQLite WAL,
Git object storage and supervised native processes. `/proc`, Linux child-subreaper
support, `renameat2`, `/dev/urandom` and `/usr/bin/git` are required.
Do not put the state directory on a network filesystem.

## Ownership and resources

Run under a dedicated Unix account for trusted code. Local CLI users with access
to the state directory have administrative access. API project/profile/role
permissions constrain remote application operations; they do not isolate executed
code from the worker account or other files that account can read.

Initialize one shared slot and memory budget. Multiple local worker processes
coordinate against that store. Admission is fair between owners; `priority OWNER N`
admits a higher-priority owner's work first (for example a landing-ready merge train)
and `0` clears it. Priority orders admission only: it never preempts running work or
changes action identity. Jobs hold reservations until their supervised
processes and declared cleanup finish. A coordinator exit does not imply that
its jobs have stopped.

Private working directories prevent accidental workspace sharing, but are not
a hostile-code sandbox. Native slot and memory declarations do not impose
process limits. Remote workers, fleet scheduling, replication and per-tenant
latency guarantees are not implemented.

## Failure and recovery

`cancel ACTION_ID --owner NAME` detaches that owner's interests. The last
requester causes a running action to stop. A new requester cannot join the
stopping attempt. Accepted work outlives API or MCP disconnection.

`recover` identifies dead supervisors. Uncertain process or external-resource
cleanup keeps reservations held. Only after inspecting and draining those resources
should an authorized operator use `recover --confirm-drained ATTEMPT_ID`.
That command records the operator's assertion; it cannot establish the assertion.

Before confirming drain, inspect the adapter's durable resource records and
verify that every owned resource is absent, including resources created before
startup failed and dependencies left after a parent was removed. Retry the
adapter's cleanup when needed. Preserve the records until verification finishes;
an exited command or a missing parent alone is insufficient. See the
[external resource hook contract](profiles.md#external-resource-hooks).

After a supervised command times out, its attempt records `reason: "timeout"`
and exit code 124 in the command evidence. Its combined stdout/stderr remains
in `attempts/ATTEMPT_ID/output.log`, with `log_sha256` in the result. A cleanup
failure can change the attempt's final exit code and holds its reservation.
Retain the failed attempt's log and evidence when retrying; a successful retry
is a separate observation. Adapter subprocess wrappers should also preserve
partial output on timeout or startup failure, rather than replacing it with
only an exception message.

`retry CANDIDATE_ID` retains successful obligations and retries failed,
interrupted or canceled obligations. Publish a new revision when source changes.
`resolve REPAIR_ID --candidate PASSING_CANDIDATE_ID` records repair attribution;
it does not establish that the new candidate causally fixed the original defect.

## Review and policy

Frontier promotion requires a certifying profile, passing obligations, an
exact-revision review, completed and drained evidence, ancestry from the previous
frontier, and a matching expected frontier. Promotion also recomputes each
obligation's job identity, dependency wiring, inputs and action key from the
certifying profile, and checks a retained pass against that profile's timeout.
Exploratory work never serves certifying candidates.

The certificate's `profile` is the certifying policy that accepted these job
identities, not a claim that every job ran under it. The certificate also names
the predecessor candidate when passes were retained. Each receipt records the
executing profile, job identity, action key, route (`own`, `joined`, `retained`,
or `legacy` for actions recorded before job identity), observation start and
end, and attempt counts. Read it with `frontier PROJECT_ID` or the `frontier`
query.

Review records are assertions, not authenticated signatures or proof of reviewer
independence. A certifying profile is an operator's policy decision. Registering
a newer profile does not automatically retire older certifying profiles or enforce
one current project policy. Operators must constrain eligible profiles through
their permissions and promotion workflow.

## Backups and upgrades

Stop accepting work, finish or explicitly drain active attempts and external
resources, then stop workers and the API. Copy the entire state directory while
quiescent: database and journal files, Git objects, artifacts, workspaces and
receipts belong together. Retain a copy of the exact runner binary and profile
toolchain. Restore into a separate directory and inspect it before resuming.

There is no automatic database migration or rolling-upgrade protocol. Back up
before changing binaries. Profile and job identity bind runner bytes, so
re-register profiles after changing the runner. Do not mix versions against a
live store. Host power-loss recovery and backup/restore under arbitrary failures
have not been established by the included tests.

To upgrade:

1. Stop accepting work. Cancel queued actions or let them finish; after the
   binary changes they fail in preparation with "runner binary changed", each
   opening a repair.
2. Drain active attempts and external resources.
3. Back up the whole state directory.
4. Upgrade the server and every remote client binary together. Clients and the
   server must agree on the command format.
5. Re-register profiles and grant the new profile IDs.
6. Resume.

Stored rows are not rewritten and no key migration runs. Actions recorded before
job identity keep working by ID for status, delivery and dependencies, and
promotion checks them against their original key. They are never shared or
retained, so the first run of each profile after an upgrade executes every job.

Logs, revisions, events, attempts and artifacts are retained. Automatic retention
and garbage collection are not implemented. Monitor disk space. Deleting files
inside the state store can invalidate evidence or replay; there is no supported
selective pruning command.

## Connections and limits

Use the [connection guide](connections.md) for private credential files, TLS,
roles, receipts and event cursors. API and worker processes must share the same
host, state directory and binary. Non-loopback HTTP listeners require TLS.

Source transfer currently carries full reachable history on download, up to
1 GiB per bundle. Incremental uploads are possible when the server already has
the prerequisites; automatic negotiation is absent. Large histories, Git packing,
disk pressure and network bandwidth can dominate request time.

The API has bounded calls, blocking operations, streams, transfers and pending
actions. Artifact, log and history growth do not yet have automatic quotas.
Metrics are process-local and reset on restart. See the connection guide for
the exact bounds and timeout behavior.
