# Using Patchweft from any coding agent

Use the [authenticated HTTP/JSON API, remote CLI or MCP adapter](connections.md) to connect
from an agent tool, script or editor. Accepted work is durable across disconnection;
preserve mutation idempotency keys and event cursors. Local administrative CLI commands
also remain available to processes sharing the service Unix account. No agent vendor
is embedded in the engine. These instructions apply to a project already imported and
assigned a profile by its operator. The command examples below use the local CLI; the
connection guide maps remote commands and source/artifact transfer.

1. **Find the project.** Run `list projects`, then `list revisions` and `status`.
   Choose an explicit revision as the task's base. The validated frontier has the
   strongest recorded evidence; a newer available revision may still need repair.
2. **Own a workspace.** Run `workspace --revision ID --owner YOUR_NAME`.
   Edit the returned path with normal tools. Keep your existing project instructions.
3. **Publish useful increments.** Run `checkpoint WORKSPACE_ID --message TEXT`.
   This makes an immutable revision available to other agents without a PR or validation
   wait. It leaves your editing directory and Git staging alone.
4. **Request evidence early.** Run `request --revision ID --profile PROFILE_ID --job JOB
   --owner YOUR_NAME`. Use the project's declared jobs. Identical outstanding work is
   shared; do not start a private duplicate merely because another agent requested it.
5. **Consume revisions explicitly.** Create a workspace from another agent's revision,
   or compose it with your own using an explicit base and change IDs. A passing check
   of each change separately does not establish that their combination passes.
6. **Validate the intended composition.** `submit` registers the profile's obligations
   for that revision. Add `--requires CANDIDATE_ID` only for real prerequisites; unrelated
   work should remain free to proceed. Candidate resubmission is idempotent.
7. **Handle failures locally.** Inspect `status`, `logs` and `repairs`; publish a repair
   revision and validate it. `retry` reruns failed obligations while retaining successful
   ones. When the operator registers a changed profile, such as a larger reservation,
   `submit --retain-from CANDIDATE_ID` (the MCP and API `retain_from` field) keeps passes
   of that same revision whose job identity is unchanged. A retained pass is the earlier
   observation, not a fresh test. Cancel only your own interest when you no longer need
   an action.
8. **Deliver by identity.** Retrieve artifacts with `deliver` into a new directory.
   Their revision/attempt identity matters; an older result must not overwrite your
   newer source. Use `events --after N` to follow progress without rereading all history.

Review, frontier promotion, external export and operator recovery belong to the roles
the user assigns. An agent does not gain those permissions merely because a CLI command
exists. In particular, `recover --confirm-drained` and repair resolution are explicit
authorized attestations, not substitutes for checking resources or fixing a defect.

Project profiles decide what compilation, tests, generators, environment or packaging
mean for that project. Native commands require no Docker; projects that need a database, browser, simulator or other
external resource must declare a finalizer that can prove its cleanup or retain capacity.
