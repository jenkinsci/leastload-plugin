# Potential Performance Issues in hudson.model.queue.MappingWorksheet

This document summarizes performance-sensitive behavior in `MappingWorksheet` and related code that can contribute to slow queue scheduling (e.g. when used with the leastload-plugin).

---

## 1. Constructor: load prediction block

**Location:** `MappingWorksheet` constructor, lines 334–368.

**What it does:** For each **Computer** (group of executor offers), when the task has `getEstimatedDuration() > 0`:

- Builds a **Timeline** (a `TreeMap<Long, int[]>`).
- For each **LoadPredictor** in `LoadPredictor.all()`, calls **`lp.predict(plan, computer, now, now + duration)`** and consumes up to **100** `FutureLoad` items.
- For each `FutureLoad`, calls **`timeline.insert(start, end, numExecutors)`**, which does:
  - **`splitAt(start)`** and **`splitAt(end)`** (each can add a new entry to the TreeMap).
  - Iteration over `data.tailMap(start).headMap(end)` to update segment counts and compute peak.

**Cost:**
`O(computers × predictors × min(100, |predictions|) × (TreeMap ops + range size))`. With many agents, multiple predictors, and long estimated durations, this can be non-trivial. The worksheet is built **once per buildable item per maintenance cycle** in `Queue.maintain()`.

**Mitigation:** Reduce number of LoadPredictors, or keep prediction logic light. For tasks with no estimated duration, this block is skipped.

---

## 2. `WorkChunk.applicableExecutorChunks()` and `ExecutorChunk.canAccept()`

**Location:**
`MappingWorksheet.WorkChunk.applicableExecutorChunks()` (228–236) calls **`canAccept(this)`** for **every** `ExecutorChunk`.
`ExecutorChunk.canAccept(WorkChunk c)` (136–151) does:

- Capacity check.
- **Label check:** `c.assignedLabel != null && !c.assignedLabel.contains(node)`.
- **Permission check** (unless skipped for flyweights): **`item.authenticate2()`** and **`nodeAcl.hasPermission2(..., Computer.BUILD)`**.

**Label.contains(node):**
In `Label.contains(Node node)` the implementation deliberately **does not** use the cached `getNodes()` set (to avoid staleness for label expressions). It calls **`matches(node)`**, which in turn uses **`node.getAssignedLabels()`** and evaluates the label expression (e.g. for `LabelAtom` it’s a lookup; for compound expressions it’s a small tree walk). So every `canAccept()` that has an assigned label does at least one `matches(node)` per executor.

**Cost:**
For **W** work chunks and **E** executor chunks, **applicableExecutorChunks()** is called once per work chunk and each call does **E** × `canAccept()`. So **W × E** label + permission checks per use. This is invoked:

- In **LeastLoadBalancer:** inside **`getApplicableSortedByLoad(ws)`** for **every** work chunk (lines 240–241), so **W** calls to **applicableExecutorChunks()** → **W × E** `canAccept()` calls per map().
- In **logMappingFailureDiagnostics()** again for each work chunk (diagnostics path).
- In core **LoadBalancer.CONSISTENT_HASH:** once per work chunk (92).

So **multiple W × E** invocations of **canAccept()** (and thus label + optional permission checks) per single **LoadBalancer.map()** call.

**Mitigation:**
- Cache **applicableExecutorChunks()** per work chunk if the same worksheet is queried many times (e.g. in leastload’s **getApplicableSortedByLoad** + diagnostics). Currently each call allocates a new list and recomputes all compatibility.
- Label.matches(node) is already the intended fast path (no full getNodes() iteration); compound label expressions are still more work than a single atom.

---

## 3. `Mapping.isPartiallyValid()` and repeated `canAccept()`

**Location:**
`Mapping.isPartiallyValid()` (291–302) is used during greedy assignment (e.g. in **LeastLoadBalancer.assignGreedily()** and in default **LoadBalancer**). It iterates over the current mapping and for each assigned chunk calls **`ec.canAccept(works(i))`** again.

**Cost:**
During **assignGreedily**, **isPartiallyValid()** is called after every tentative assignment. So the same **canAccept()** (label + permission) logic runs again for each partial mapping state. With many work chunks and many candidate executors, the number of **canAccept()** calls can be large (order of “assignments tried” × “work chunks”).

**Mitigation:**
Compatibility between a (work chunk, executor chunk) pair does not change during one **map()** call. A cache keyed by (work index, executor index) could avoid repeated **canAccept()** inside **isPartiallyValid()** during the same **map()** run. This would require a small API or visibility change (e.g. cache in the worksheet or mapping).

---

## 4. Allocations and iteration in LeastLoadBalancer

**Location:**
LeastLoadBalancer **getApplicableSortedByLoad(ws)** (238–250):

- For **each** work chunk it calls **ws.works(i).applicableExecutorChunks()**, which allocates a new **ArrayList** and iterates all executors.
- It then **shuffle()**s the combined list and **sort()**s it.

So for **W** work chunks we do **W** allocations and **W** full executor iterations, then one shuffle and one sort. Duplicate executor chunks can appear in the list (same chunk can be applicable to multiple work chunks), so the list can be larger than the executor set.

**Cost:**
Allocation and iteration scale with **W × (applicable executors per work)**. Together with the repeated **canAccept()** work above, this is a hot path for label-heavy workloads.

---

## 5. Summary table

| Area | Issue | When it hurts |
|------|--------|----------------|
| **Constructor** | Load prediction: many computers × predictors × up to 100 FutureLoads × Timeline.insert | Tasks with estimated duration; many agents; multiple/heavy LoadPredictors |
| **applicableExecutorChunks()** | No caching; W × E canAccept() (label + permission) per map() | Many work chunks and/or many executor chunks; called multiple times per map() |
| **canAccept()** | Label.contains(node) → matches(node) and optional authenticate2 + hasPermission2 | Every (executor, work) compatibility check |
| **isPartiallyValid()** | Repeats canAccept() for each assigned chunk on every tentative assignment | Deep or wide greedy search (many chunks, many candidates) |
| **getApplicableSortedByLoad** | W separate applicableExecutorChunks() calls + shuffle + sort | Many work chunks; large executor set |

---

## 6. Practical impact

- **Label-based scheduling:** The main hot path is **W × E** compatibility checks (label + permission) with no caching, and **isPartiallyValid()** repeating some of those checks during assignment. This can add up when there are many nodes and/or many work chunks (e.g. matrix jobs).
- **Constructor:** Load prediction can add noticeable time when there are many computers and non-trivial **LoadPredictor** implementations, especially for tasks with a positive estimated duration.
- Together with **Queue.maintain()** running only periodically and the leastload round-robin behavior, slow worksheet construction and repeated compatibility checks can make each **maintain()** run longer and thus amplify the delay before a buildable item gets a node.
