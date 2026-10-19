# Spec: the transcript store writes far fewer bytes per append, and appends share a commit

**Date:** 2026-10-19
**Author:** Maricon@charlie
**Type:** Spec (storage write path; no change to what is stored or read, step 1)
**Status:** draft — for review by Camper@narko; nothing is implemented. Decision pending on step 1 (group commit) and on whether step 2 is ever needed.
**Issue:** #5141 (AgentInput and turn writes stall 8–18 s behind the transcript store's single connection on a Windows 10 machine with a slow disk)
**Part of:** track E follow-up (srv). Related: `docs/reports/REPORT_AGENT_OPEN_STALL_RCA_2026_10_05.md` (the same lock behind a different stall), `SPEC_FILESTORE_APPEND_WITHOUT_COPIES_2026_10_18.md` (#5090, the in-memory half of this).
**Platform:** all. Measured on Linux; the stall itself was seen on Windows only.

## 1. The problem

`FileStore` has one write connection behind a `std::sync::Mutex` (`TimedConn`, `FileStore::lock_conn`). Every write in the process waits for it: on the global transcript store that is every agent's appends and turn-state writes. A wait cannot be interrupted by a `tokio::time::timeout`.

#5141 (Loap-3, memory lab on a Windows 10 machine, C: a SATA SSD 79% full) found that when the disk stalls, whoever holds the lock holds it for the length of the stall: holds of 5.2, 10.4, 9.4 and 11.4 s in 4 of 12 runs, up to 8 callers queued, `AgentInput` taking 8–14 s. The same runs show the disk saturated (queue length 31, then almost no writes completing for 8 s after bursts of 45–106 MB/s), and the store's WAL reaching about 183 MB for a 9 MB database.

Most of that write traffic is ours to remove. Each transcript append rewrites a whole part (§2).

## 2. What an append writes today

`FileStore::append_in_tx` (`filestore/counter.rs`) does `REPLACE INTO db_file_data` with the *whole part* (up to `PART_DATA_SIZE` = 64 KB): the bytes already in the part plus the new ones. #5090 removed the in-memory copies of this, not the write: SQLite still writes every page of the new blob to the WAL, and the checkpoint copies them again into the database file.

Measured with a test that appends to a file-backed store (WAL, `synchronous=NORMAL`, the background checkpointer running) and reads the process's own `wchar` from `/proc/self/io` (every byte the process writes: WAL frames, checkpoint copies, size and index updates). Linux, one run per row:

| record size | bytes written per append today | × the payload |
|---|---:|---:|
| 300 B | 74.9 KB | 256× |
| 2 KB | 89.0 KB | 46× |
| 8 KB | 105.4 KB | 13.5× |

The consequence in the code's own words (`checkpointer.rs`): a 4 MB WAL fills about every 60 lines. Every replace also pays the `db_file_data_rev_insert_v2` trigger, which reads the old blob and compares it with the start of the new one (`substr(NEW.data, 1, length(d.data)) IS NOT d.data`).

A side-table row instead of a part rewrite is not the 100× it might look like, because SQLite writes whole 4 KB pages. Same method, a prototype `db_file_tail` table, one INSERT plus the UPDATE of the file's size in one transaction:

| record | part rewrite today | one row per transaction | rows, 8 appends per transaction | rows, 32 appends per transaction |
|---|---:|---:|---:|---:|
| 300 B | 74.9 KB | 12.0 KB (6.2× less) | 3.7 KB (20× less) | 1.3 KB (58× less) |
| 2 KB | 89.0 KB | 26.5 KB (3.4× less) | | |
| 8 KB | 105.4 KB | 34.6 KB (3.0× less) | | |

So rows alone are worth 3–6×; the large factors come from several appends sharing one commit. A shared commit also helps today's layout: several appends that land in the same part, in the same transaction, write that part's pages once.

Append latency today (same test, Linux, local SSD): one quiet caller, 300 B: p50 1.35 ms, p99 2.75 ms. Eight callers at once (eight files, 2 ms between appends): p50 9.6 ms, p99 27 ms, max 91 ms for 300 B; p50 60 ms, p99 118 ms for 2 KB records. The queue behind the lock is visible even on a healthy disk.

## 3. Constraints that shape the fix

1. **The store is shared by every srv instance on the machine, and older builds must read what a new build writes.** `migrations.rs` adds the counter columns *without* a `FILESTORE_SCHEMA_VERSION` bump because a bump makes older builds refuse to open the store. Anything stored somewhere older builds don't look breaks them: a file whose `size` claims bytes their parts don't hold reads as zeros there. Step 1 below keeps the layout; step 2 has to solve this.
2. **The line counter and `rev`.** The counter epoch (`gen`, `lines`, `lines_size`) and the triggers' `rev` are maintained inside the append transaction and by triggers; a changed write path must keep their invariants (an append extends the last part, so it must not bump `rev`).
3. **Callers wait for their own write** (5a-3: a transcript event is emitted after its append returns, with the position the append landed at). A shared commit has to return each caller its own `AppendPos`.
4. **The API is synchronous and borrows**: `append_lines_stamped(&self, zone, name, data: &[u8], tsidx)`. A queue needs the arguments owned.

## 4. Step 1: appends share a commit (no schema change)

Appends (and the stamped variant that also writes `output.tsidx`, and only those) go through a small combiner instead of each taking the lock for its own transaction.

- A caller puts its append (owned `zone`, `name`, `data`, mode, tsidx flag, and a one-shot result slot) on a queue. If nobody is committing, it becomes the **leader**: it takes the connection, drains the queue, runs every queued append in order inside **one** `BEGIN IMMEDIATE` transaction, commits once, hands each caller its own result, and releases. Callers that queued meanwhile are the next leader's batch.
- **No timer and no waiting for company.** A lone append with an empty queue is its own leader and does exactly today's work, so a quiet append gets no added latency. Batches form only while a commit is already running. (A "window" that delays the first write is rejected because it adds latency for the common single append.)
- **Order:** appends to the same file are applied in queue order; each applies on top of the previous one's result inside the transaction, so positions and the counter come out as if they had run one after another.
- **Failure isolation:** each append runs in a `SAVEPOINT`; one that fails (file missing, `NotFound`, a counter that needs its epoch dropped) rolls back its own savepoint and returns its error to its own caller. The others commit.
- **What it does not change:** the stored layout, the triggers, the counter, the `rev` rules, the reads. Older builds see exactly what they see today.
- Everything else that needs the connection (`write_file`, `make_file`, deletes, the checkpoint call) still takes the lock directly. The leader holds it only for its batch.

**Expected effect, and what it cannot do.** Under load the batch size is the queue depth (#5141 shows up to 8). The bytes written for appends to the same part fall by about the batch size, and so does the number of times the lock is taken and released. It does **not** end a stall already under way: the leader that is inside a stalled `write()` still holds the lock for the whole stall, and the callers queued behind it wait for it. What it takes away is the load that probably causes the stall (§5) and the serial re-acquisition after it. Whether that is enough on the Windows machine is a measurement, not a claim here (§7).

## 5. Hypothesis to confirm before step 2: who writes the burst

The disk was saturated by 45–106 MB/s bursts. The store's own write volume (§2) and a WAL file that reaches 20× the database (it keeps its high-water size until the 30-minute TRUNCATE loop; a PASSIVE checkpoint never shrinks it, and leaves behind pages a reader still needs) are the candidates. Posa's RCA (in progress) should say whether the long holds line up with a checkpoint, and whether the time is in `write()` or in a flush. If a checkpoint's flush of the database file stalls the volume, the fix includes the checkpoint cadence and the WAL size, not only the append path. This spec does not decide that.

## 6. Step 2 (only if step 1 is not enough): rows plus a fold

Append the record as a row in a side table (`db_file_tail`: `zoneid`, `name`, `seq`, `data`) and fold the rows into parts when the file is idle or at a checkpoint. Measured above: 3–6× fewer bytes by itself, more with step 1.

What it has to solve first, because of §3.1: during the time rows exist, an older build reading the file sees a `size` that its parts don't cover. Options to design, not decided: (a) fold before any older build can read (a per-store marker naming the oldest build allowed to open it, set by the first new build; costs older builds the ability to open the store); (b) keep `size` as the size of the *parts* and put the tail's length in a new column, so an older build reads a consistent shorter file and misses only the unfolded tail; (c) write rows only while no other instance has the store open (detectable?). Each changes what an older build shows, so this part needs Camper's decision and a compatibility test with a pinned older binary before any code.

## 7. How it is measured

For each change, A/B on the same pinned commit (Loap-3's lab, `--srv` swap, at least 4 interleaved runs per side), reporting next to each other:

- **bytes written per append** (the probe test above, Linux `wchar`; on Windows the lab's disk write bytes for srv),
- **p50 / p99 append latency**, one quiet caller and 8 concurrent callers, so any added latency for the single append is visible,
- **`result.storeLock`** (#5142): the longest hold and wait of the store connection and their call sites, and the counts of holds and waits of 500 ms or more,
- the store's **WAL size at the end** of the run, round time, srv CPU and memory (the rule: no change may trade performance for memory).

On the machine that stalls (Area54, with Posa): the same runs, counting how many of N runs have a hold over 5 s.

## 8. Tests (step 1)

- The append path against the naive reference in `tests_append_reference.rs`, driven through the combiner by several threads at once; every file reads back exactly what its reference holds, and each caller's `AppendPos` matches what a serial run would give.
- A failing append (missing file) in the middle of a batch: its caller gets `NotFound`, the others commit and read back.
- Counter and `rev` invariants after a batch: line counts, `lines_size`, no `rev` bump from an append.
- A lone append is not delayed: p99 of 1 caller within noise of today's.
- Crash safety: the transaction either has all of a batch or none (kill during commit in a test with a file-backed store).
- Another process writing the same database (`busy_timeout`) while a batch commits.

## 9. Open questions

1. Are the stalls reached on other hardware? (Loap-3: a second Windows machine does not stall.)
2. Does the wait for the connection happen on a tokio worker? If so, `spawn_blocking` for the store calls is a separate safety net worth shipping first (Camper's step: small, independent).
3. Step 2's compatibility rule (§6), and whether it is needed at all after step 1 and the checkpoint findings.
4. The batch sizes AgentMux sees under real load: the first implementation task is a counter of queue depth at each commit, read from the lab.

## 10. Remaining work

Review of this draft; Posa's three checks; then step 1 as its own PR with the §7 numbers.
