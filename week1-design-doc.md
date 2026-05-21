# UnboundedSource — Week 1 Design Doc / 第一周设计文档

| | |
|---|---|
| **Project / 项目** | Native Python `UnboundedSource` (GSoC 2026, deliverable D1) |
| **Author / 作者** | Elia |
| **Mentor / 导师** | Yi Hu (yhu@apache.org) |
| **Issue** | https://github.com/apache/beam/issues/19137 |
| **Scope / 范围** | Week 1 · 2026-05-25 → 2026-05-31 |

**W1 exit criteria / 本周出口条件**
> *EN:* ABCs drafted; checkpoint coder interface defined; pause/resume and harness-triggered checkpoint note written.
> *中文：* 起草 ABC 抽象基类；定义 checkpoint 编码器接口；写出 pause/resume 与 harness 触发 checkpoint 的设计说明。

---

## 1. Goal / 目标

*EN:* Let Python authors write a simple `Reader`/`Source` and have `beam.io.Read(MySource())` run as a Splittable DoFn — the runner handles checkpointing, splitting, and watermarks. This mirrors Java's `UnboundedSource` + `UnboundedSourceAsSDFWrapperFn`, and reuses the same pattern as the existing bounded SDF wrapper in `iobase.py`. No new public entry point: `Read(source)` simply gains an `isinstance(source, UnboundedSource)` dispatch.

*中文：* 让用户只写简单的 `Reader`/`Source`，`beam.io.Read(MySource())` 即以 Splittable DoFn 运行，由 runner 负责 checkpoint、split、watermark。对标 Java 的 `UnboundedSource` + `UnboundedSourceAsSDFWrapperFn`，并复用 `iobase.py` 中已有的 bounded SDF wrapper 模式。不新增公开入口：`Read(source)` 只是多一条 `isinstance(source, UnboundedSource)` 的分发。

---

## 2. ABCs / 抽象基类  · *(W1: "ABCs drafted")*

*EN:* Three public abstract base classes in `apache_beam.io.iobase`, Java semantics with Pythonic (`snake_case`) names.

*中文：* `apache_beam.io.iobase` 中三个公开抽象基类，沿用 Java 语义、采用 Python 命名（`snake_case`）。

### `CheckpointMark`
*EN:*
- `finalize_checkpoint()` — default **no-op**; called once after the bundle is durably committed (e.g. Pub/Sub ack).
- `get_offset_limit()` — only required if the source enables offset-based dedup.

*中文：*
- `finalize_checkpoint()` —— 默认**空操作**；在 bundle 持久化提交后调用一次（如 Pub/Sub ack）。
- `get_offset_limit()` —— 仅当 source 启用 offset 去重时才需实现。

### `UnboundedReader`
*EN:*
- **Abstract:** `start`, `advance`, `get_current`, `get_current_timestamp`, `get_watermark`, `get_checkpoint_mark`, `close`.
- `get_current_record_id()` — default `b''`; **required** when `requires_deduping()` is `True` (must be stable across checkpoint/restore).
- **Key semantics:** `advance() == False` means *"no record right now"*, **not** "exhausted" → the wrapper pauses and resumes. `get_watermark()` is treated as **monotonic**; a reader that regresses is clamped to the last value.

*中文：*
- **抽象方法：** `start`、`advance`、`get_current`、`get_current_timestamp`、`get_watermark`、`get_checkpoint_mark`、`close`。
- `get_current_record_id()` —— 默认 `b''`；当 `requires_deduping()` 为 `True` 时**必须**实现（需在 checkpoint/restore 间对同一逻辑记录保持稳定）。
- **关键语义：** `advance() == False` 表示*"当前暂无记录"*，**而非**"已耗尽" → wrapper 会暂停后恢复。`get_watermark()` 视为**单调**；若 reader 回退，wrapper 钳制到上一次的值。

### `UnboundedSource`
*EN:*
- **Abstract:** `split`, `create_reader`, `get_checkpoint_mark_coder`.
- `requires_deduping()` / `offset_based_deduplication_supported()` — default `False`.
- `default_output_coder()` — defaults to the generic `object` coder so a minimal user source expands without crashing.

*中文：*
- **抽象方法：** `split`、`create_reader`、`get_checkpoint_mark_coder`。
- `requires_deduping()` / `offset_based_deduplication_supported()` —— 默认 `False`。
- `default_output_coder()` —— 默认回退到通用 `object` coder，保证最小实现的 source 也能正常展开。

---

## 3. Checkpoint coder interface / Checkpoint 编码器接口  · *(W1: "checkpoint coder interface defined")*

*EN:*
- `UnboundedSource.get_checkpoint_mark_coder()` returns the `Coder` for that source's checkpoint marks. Sources without a meaningful mark may return `coders.PickleCoder()` with a no-op mark.
- The restriction coder encodes `(source, checkpoint_mark, watermark, is_done)`. The checkpoint is wrapped with an explicit **null-tag** (`\x00` = none, `\x01` = value) so "no mark" and "a mark that happens to encode to empty bytes" are unambiguous — mirroring Java's `NullableCoder`. The source is pickled by default; the watermark uses `TimestampCoder`. The live reader is **never** serialized.

*中文：*
- `UnboundedSource.get_checkpoint_mark_coder()` 返回该 source 的 checkpoint mark 编码器。无实质 mark 的 source 可返回 `coders.PickleCoder()` 配空操作 mark。
- restriction 编码器编码 `(source, checkpoint_mark, watermark, is_done)`。checkpoint 外面包一层显式**空值标记**（`\x00` = 无，`\x01` = 有），从而区分"无 mark"与"mark 恰好编码为空字节"——对标 Java 的 `NullableCoder`。source 默认 pickle；watermark 用 `TimestampCoder`。**绝不**序列化存活的 reader。

---

## 4. Pause/resume & harness-triggered checkpoint / 暂停恢复与 harness 触发的 checkpoint  · *(W1: "note written")*

### 4.1 Pause / resume / 暂停与恢复
*EN:* When `advance()` returns `False` (no data now), the wrapper yields `ProcessContinuation.resume(delay)` — liveness-safe, no busy spin — and the runner re-invokes later from the persisted restriction. The reader is cached per `(source, checkpoint)` on the same worker so resume does not re-pay reader setup (network/connections). Correctness never depends on the resume delay (it is advisory).

*中文：* 当 `advance()` 返回 `False`（当前无数据），wrapper 产出 `ProcessContinuation.resume(delay)`——保活、无忙等——runner 之后从持久化的 restriction 重新调用。reader 在同一 worker 上按 `(source, checkpoint)` 缓存，使恢复时无需重付 reader 初始化（网络/连接）成本。正确性绝不依赖 resume 的延迟（延迟仅为建议值）。

### 4.2 Harness-triggered checkpoint (dynamic split) / harness 触发的 checkpoint（动态切分）
*EN:*
- Checkpoint-based, **not** fraction-based (a live `UnboundedReader` has no meaningful fraction position). `try_split()` builds a residual from `reader.get_checkpoint_mark()`, marks the primary done, and closes the reader. Before `start()` it returns `None`.
- `defer_remainder()` triggers `try_split(0)`. Because the DirectRunner *also* checkpoints via `try_split(0)`, a private hook `_set_defer_remainder_split` (a small change in `runners/sdf_utils.py`) lets the tracker distinguish the two: a **defer** must produce a residual (else re-raise so the bundle is retried), while a **runner-initiated** split may decline (`None`). *(This is the only change to shared runner code — flagged for review.)*
- `finalize_checkpoint()` is registered via `DoFn.BundleFinalizerParam` inside a `finally` block, so it runs after a durable commit and is never skipped, even on the split/exception paths.

*中文：*
- 基于 checkpoint，**而非**基于 fraction（存活的 `UnboundedReader` 没有有意义的 fraction 位置）。`try_split()` 用 `reader.get_checkpoint_mark()` 构造 residual，将 primary 标记为 done 并关闭 reader。`start()` 之前返回 `None`。
- `defer_remainder()` 会触发 `try_split(0)`。由于 DirectRunner 的 checkpoint *也*走 `try_split(0)`，于是用一个私有钩子 `_set_defer_remainder_split`（`runners/sdf_utils.py` 的一处小改动）让 tracker 区分两者：**defer** 必须产出 residual（否则重新抛出、由 runner 重试），而 **runner 主动发起**的 split 可以拒绝（返回 `None`）。*（这是唯一改动到共享 runner 代码的地方——已标注供 review。）*
- `finalize_checkpoint()` 通过 `DoFn.BundleFinalizerParam` 在 `finally` 块里注册，从而在持久化提交后才执行、且永不被跳过（即使走 split/异常路径）。

---

## 5. W1 deliverable checklist / 本周交付清单

| W1 item / 本周项 | Status / 状态 |
|---|---|
| ABCs drafted / 起草 ABC | ✅ code: `CheckpointMark` / `UnboundedReader` / `UnboundedSource` |
| Checkpoint coder interface / Checkpoint 编码器接口 | ✅ `get_checkpoint_mark_coder()` + restriction coder |
| Pause/resume + harness checkpoint note / 暂停恢复与 harness checkpoint 说明 | ✅ §4 (this doc / 本文) |
| Circulate to mentor & dev@ for confirmation / 交导师与 dev@ 确认 | ⏳ pending / 待办 |
