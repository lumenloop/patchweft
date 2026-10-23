# Control contention probe

Run on a disposable Linux development host with Python 3 and Git:

```sh
python3 examples/control-isolation/run.py --binary ./target/release/patchweft --root /tmp/patchweft-control-01
```

The destination must not exist. The probe keeps its synthetic source, state,
request trace and database evidence there. It never opens an existing state
store or reads credentials. Two public fixture credentials are provisioned
directly into the new store; they are unsuitable for deployment.

Two projects each hold one native job. A barrier releases 128 callers sharing
one noisy principal and one action. A separate principal samples project status
with a 50 ms pause after each response and requests cancellation 100 ms after the burst starts. Mutations
retry bounded backpressure with their original keys for up to 60 seconds.
Every wire attempt and logical operation is appended to a trace as it occurs;
phase observations are saved atomically. Status samples do not
retry. The quiet job's gate stays closed until cancellation drains it.

The probe uses two CPU affinities, two job slots, two coordinators and a
120-second outer process deadline. Both coordinators serve the one local
executor, so the second waits as a standby until the first exits. Memory
values are scheduling reservations.
The ownership wrapper adopts and cleans up its own descendants, including
detached executors. A successful run requires all 128 logical requests to share
one noisy action, two total physical attempts, cancellation and complete drain,
and no surviving owned processes. These are correctness checks, not a latency
SLO. Inspect `observations.json` for refusals and timing; a successful exit does
not assert zero quiet failures.

For a comparison, keep this script and affinity unchanged, use distinct output
directories, pin both binary hashes, alternate run order, and report all runs.
Host load and client-side contention affect results. This experiment does not
test slow request bodies, credential rotation, many noisy principals, or
cancellation preemption of an existing database transaction.
