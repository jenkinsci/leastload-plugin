# Why a node can take up to ~30s to take on work after Queue assigns an ExecutorChunk

## Summary

**Root cause:** The executor thread cannot make progress until **Queue.maintain()** releases the **Queue lock**. `maintain()` holds that lock for the **entire loop over all buildable items**. So an executor assigned in the first iteration sits blocked until every later buildable is processed and `maintain()` exits. With many buildables and costly work per item, that can reach tens of seconds.

---

## Flow from “assigned” to “running”

1. **Queue.maintain()** runs (single thread, see `Queue.Maintainer` / `scheduleWithFixedDelay`).
2. It takes the **Queue lock** once at the start and holds it for the whole run:
   - `Queue.java` ~1496: `lock.lock()` at top of `maintain()`
   - ~1710–1711: `lock.unlock()` only in the outer `finally` when `maintain()` is done.
3. For each **BuildableItem** `p` in a **copy** of `buildables`:
   - `getCauseOfBlockageForItem(p)`
   - Build **candidates** from `parked` (one `JobOffer` per idle executor), with `getCauseOfBlockage(p)` per offer
   - **MappingWorksheet** built from item + candidates + load predictors
   - **loadBalancer.map(p.task, ws)** (e.g. leastload assigns an ExecutorChunk)
   - **m.execute(wuc)** → `MappingWorksheet.Mapping.execute(WorkUnitContext)`:
     - For each work chunk, `ExecutorChunk.execute(...)` calls `ExecutorSlot.set(WorkUnit)` on the chosen slot.
   - **JobOffer.set(WorkUnit)** (~246–251):
     - Sets `this.workUnit = p`
     - Calls **executor.start(workUnit)**.
   - **Executor.start(workUnit)** (~815–822):
     - Sets `this.workUnit = task`
     - Calls **super.start()** (i.e. **Thread.start()**), so the **Executor** thread actually starts (or was already running and is woken; see below).
4. The **Executor** thread’s **run()** (~339+):
   - Reads `workUnit`, then quickly enters **Queue.withLock(new Callable<>(){ ... })** (~368).
   - **Queue.withLock** acquires the **same Queue lock** that `maintain()` is still holding.
5. So: **maintain()** is still in the loop (processing more buildables). It **never** releases the lock between iterations. The executor thread therefore **blocks in Queue.withLock()** until **maintain()** exits and releases the lock in the outer `finally`.
6. Only after the lock is released can the executor:
   - Run the `Callable` (set executor on work unit, `queue.onStartExecuting`, create `Executable`, etc.)
   - Leave `withLock`
   - Call **workUnit.context.synchronizeStart()** (and then run the executable).

So the **time from “Queue assigned an ExecutorChunk” to “node actually taking on work”** is at least: **time to finish the rest of the current maintain() run** (process all remaining buildables and exit).

---

## Why this can reach ~30 seconds

- **One lock for the whole maintain():** The Queue lock is held for the **entire** `maintain()` pass, not per buildable. So work assigned in iteration 1 is blocked until the **last** iteration and the end of `maintain()`.
- **Cost per buildable:** For each buildable, maintain() does:
  - `getCauseOfBlockageForItem(p)` (and possibly `updateSnapshot()`)
  - For each parked executor (often many on a big instance): `getCauseOfBlockage(p)` / `getNode()` / `node.canTake(item)` / `QueueTaskDispatcher.canTake(node, item)` etc.
  - Building `MappingWorksheet` (candidates, load predictors if any)
  - **loadBalancer.map(task, ws)** (plugin work: applicable chunks, filtering, sort, assign)
  - After a successful map: `m.execute(wuc)`, `makePending(p)`, `updateSnapshot()`
- With **many buildables** (e.g. tens or more) and **many executors** (e.g. 30+), each iteration can take on the order of **1–3+ seconds**. So 10–20 buildables × 1–2 s → **~10–30 seconds** of lock hold time. The first assigned executor waits for all of that.

So even if leastload (or any balancer) **assigns work immediately to the right label**, the **node does not start that work** until the executor thread can take the Queue lock, which is only after **maintain()** finishes.

---

## Relevant code references (Jenkins core)

| What | Where |
|------|--------|
| maintain() takes lock for full run | `Queue.java` ~1496 `lock.lock()`, ~1710–1711 `lock.unlock()` in finally |
| Loop over buildables | `Queue.java` ~1624–1709 `for (BuildableItem p : new ArrayList<>(buildables))` |
| Assignment: Mapping.execute → JobOffer.set → executor.start(workUnit) | `Queue.java` ~1687; `MappingWorksheet.java` ~316–321; `Queue.java` ~246–250; `Executor.java` ~815–822 |
| Executor thread blocks on Queue lock | `Executor.java` ~368 `Queue.withLock(new Callable<>(){ ... })` |
| Queue.withLock uses same lock | `Queue.java` ~1279–1329, 1409+ (`_withLock` → `lock.lock()`) |

---

## Possible mitigations (core or config)

1. **Shorten maintain() duration** so the lock is held less long:
   - Fewer buildables (e.g. limit concurrency so fewer items sit in buildables).
   - Cheaper per-item work (e.g. cache or reduce work in `getCauseOfBlockage` / worksheet building / load balancer).
2. **Release the lock between buildables** (structural change in core): e.g. release lock after each “assign and makePending” and re-acquire before the next buildable. That would allow executors assigned in earlier iterations to grab the lock and start while maintain() continues. This is a design/thread-safety change and needs careful review.
3. **Reduce maintainInterval** (e.g. `-Dhudson.model.Queue.maintainInterval=2000`) does **not** by itself reduce this delay: it only makes the **next** maintain() run sooner; the delay for an **already-assigned** executor is still “time until the **current** maintain() releases the lock.”

---

## Note on “assigns work immediately to the proper label”

The leastload plugin only decides **which** ExecutorChunk (and thus which node) gets the work. The actual handoff is:

- **Queue** (inside maintain(), under the same lock): `m.execute(wuc)` → `JobOffer.set(workUnit)` → `executor.start(workUnit)`.
- The **Executor** thread then needs the Queue lock in `run()` → `Queue.withLock(...)` and cannot get it until maintain() exits.

So the “up to ~30 seconds” delay is **not** from the load balancer or from the node being slow to accept the label; it is from **Queue lock contention**: the executor thread is blocked waiting for the same lock that maintain() holds for the entire pass over all buildables.
