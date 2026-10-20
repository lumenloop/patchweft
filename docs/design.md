# Design

Patchweft separates the editing workspace, published source, requested computation
and validated frontier. That separation lets an agent publish useful work early
without declaring it ready to deliver.

A revision identifies an immutable Git commit and its project. A workspace
belongs to an author and has a base and successive checkpoints. Composition
names the base and changes explicitly, so the source being tested is inspectable.
Source composition remains within a repository; validation prerequisites may
cross project boundaries.

An action binds a revision, profile, job and dependency identities. Interests
record who still needs it. Attempts record actual executions, including failures.
A candidate groups the profile's obligations for a revision. The validated
frontier advances only through the configured validation and review checks.

Content identity makes sharing possible, but does not prove that all inputs are
known. Default sharing stops at outstanding work. Completed-result reuse requires
an explicit input audit. Artifact delivery verifies hashes and creates a new
destination, keeping old output from silently replacing current editing files.

The service uses local transactions for durable mutation receipts and database
effects. Git objects and external process side effects are outside that transaction.
Resumable events help agents reconnect; stable mutation keys prevent a lost
response from creating a second logical operation.

## Related work

These references informed the design questions. Patchweft does not embed these
projects or claim their scale or capabilities.

- [Jujutsu](https://github.com/jj-vcs/jj) explores a Git-compatible model with
  first-class working-copy changes and an operation log.
- [Sapling](https://github.com/facebook/sapling) explores scalable source control
  and workflows for related changes.
- [Bazel's hermeticity guide](https://bazel.build/basics/hermeticity) explains why
  source identity, tool identity and isolation matter for reproducible reuse.

Patchweft's current implementation deliberately exposes its trust and operational
boundaries. See [operations](operations.md) and [profiles](profiles.md) before
using a validated frontier as an external delivery gate.
