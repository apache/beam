<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Correlated sub-query decorrelation pre-pass

## Context

`CalciteQueryPlanner.convertToBeamRel(RelNode, ...)` is the single chokepoint
through which every SQL query the Beam SQL extension plans is converted from a
Calcite logical tree into a `BeamRelNode` tree by a single Volcano program.

The SqlToRel converter (driven by `Planner.rel`) already decorrelates most
queries: `SqlToRelConverter` is configured with `withExpand(true)`, and the
default `Planner.rel` path calls `RelDecorrelator.decorrelateQuery` once on the
post-`SqlToRelConverter` tree. For the common correlated-scalar shape this is
enough — the result is a `Join` + `Aggregate[SINGLE_VALUE]` that existing Beam
rules already cover.

However, some correlated shapes survive that first pass:

- An un-expanded `RexSubQuery` inside a PROJECT/JOIN/FILTER condition (e.g. some
  correlated `EXISTS`/`IN` forms), or
- a residual `LogicalCorrelate` the SqlToRel decorrelate pass could not lower.

The Beam Volcano ruleset (`BeamRuleSets.LOGICAL_OPTIMIZATIONS` /
`BEAM_CONVERTERS`) has **no** converter rule for a general `LogicalCorrelate`.
The only consumer is `BeamUnnestRule`, which matches strictly
`LogicalCorrelate(_, Uncollect)` (the `UNNEST` shape). So any other residual
correlate reaching Volcano fails planning with a `CannotPlanException`, surfaced
as `SqlConversionException: Unable to convert relNode to Beam: ...`.

## Design

Add a private `normalizeForVolcano(RelNode)` pre-pass that runs at the very top
of `convertToBeamRel(RelNode, ...)`, **strictly before** both:

1. the Volcano `program.run(...)`, and
2. the metadata-provider swap (`setMetadataProvider` / `setMetadataQuerySupplier`
   / `RRelMetadataQuery.THREAD_PROVIDERS`).

Running it before the metadata swap is deliberate: the pass uses stock Calcite
metadata only, so it cannot trigger the Beam cost path (`BeamCostModel`,
`RelMdNodeStats`, `BeamRelMetadataQuery`) and the cost recursion the Volcano
search guards against.

The pass does two things:

1. A short-lived `HepPlanner` with `FILTER_SUB_QUERY_TO_CORRELATE`,
   `PROJECT_SUB_QUERY_TO_CORRELATE`, and `JOIN_SUB_QUERY_TO_CORRELATE` to turn any
   residual `RexSubQuery` into a `LogicalCorrelate`.
2. `RelDecorrelator.decorrelateQuery(rel, RelBuilder.create(config))` to lower
   correlates into standard `Join`/`Aggregate`/`Project`/`Filter` nodes, using the
   planner's configured `RelBuilder` so produced rels share the cluster type
   factory and traits.

### Pre-flight safety gate

The entire pass is gated on the tree actually **referencing** a correlation
variable: `RelOptUtil.getVariablesUsed(rel).isEmpty()` ⇒ return the input
unchanged. This makes the pass a strict no-op on trees without a referenced
correlate.

The gate intentionally uses `getVariablesUsed`, **not** `getVariablesSet`:

| shape                              | `getVariablesSet` | `getVariablesUsed` |
|------------------------------------|-------------------|--------------------|
| `LogicalCorrelate(_, Uncollect)` (UNNEST) | non-empty (defines an id) | empty (body refs none) |
| correlated scalar / EXISTS / IN    | non-empty         | non-empty          |

Because UNNEST *defines* a correlation id but does not *reference* one in its
body, `getVariablesUsed` is empty for it and the pass is skipped, leaving the
`LogicalCorrelate(_, Uncollect)` intact for `BeamUnnestRule`. A gate keyed on
`getVariablesSet` would wrongly run the decorrelator on UNNEST trees. This
structural guard is preferred over relying on test-only verification.

## Increments

This change is the smallest shippable increment:

- Single edit in `CalciteQueryPlanner.convertToBeamRel(RelNode, ...)` plus one
  private helper. No ruleset changes, no SparkConnect-side changes.
- Deferred (explicitly out of scope): adding the PROJECT/JOIN
  `*_SUB_QUERY_TO_CORRELATE` variants into the Volcano `LOGICAL_OPTIMIZATIONS`
  ruleset; non-equi semi/anti-join lowering for correlated `EXISTS` without an
  equi key (still blocked at `BeamJoinRel.extractJoinRexNodes`).

## Risks

1. **UNNEST regression.** Mitigated by the `getVariablesUsed` gate above and
   verified by the `*Unnest*` test subset (and `BeamSqlDslArrayTest`).
2. **Idempotency.** For trees the SqlToRel pass already fully decorrelated, the
   gate skips the pass entirely (no referenced correl var remains), so there is
   no redundant second decorrelate and no shape churn.
3. **Cost-hang trap.** Avoided by placement strictly before the metadata-provider
   swap and Volcano; the pass never touches the Beam cost model.
4. **Hand-built `RelNode` test fixtures** (join/aggregation DSL tests) flow
   through this method too; with no referenced correl var they hit the gate and
   are untouched. The filtered `*Join*` / `*CalciteQueryPlanner*` subset guards
   this.
