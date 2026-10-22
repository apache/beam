/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.hint.Hintable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.hint.RelHint;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provenance label carried on a {@link RelNode} as a {@link RelHint}, used to name the composite
 * {@link org.apache.beam.sdk.transforms.PTransform} the node expands into.
 *
 * <p>The label records which nodes were fused to produce this one rather than what the node
 * computes. Planner rules collapse a whole chain of relational operations into a single {@code
 * Calc}, so a label like {@code Filter;Project} tells the reader which of them ended up in the
 * transform.
 *
 * <p>A {@link RelHint} is the carrier because hints are a field on every core Calcite rel type and
 * survive both {@code copy()} and physical conversion.
 *
 * <h2>Whether the planner can see a label</h2>
 *
 * <p>Only some rel types put their hints in the digest, and the two halves fail in opposite
 * directions.
 *
 * <p>{@code Project}, {@code Filter} and {@code Join} override {@code deepEquals0}/{@code
 * deepHashCode0} and compare the hints field, so for them two rels differing only in their label
 * are two rels as far as the planner is concerned. A label must therefore be a canonical function
 * of the <em>set</em> of nodes fused into it, never of the order they were matched in: {@code
 * ProjectFilterTransposeRule} and {@code FilterProjectTransposeRule} swap a pair back and forth,
 * and an order-sensitive label would hand the planner an endless supply of "new" rels to explore.
 * That is what the position prefix is for -- see {@link #compose}.
 *
 * <p>{@code Calc}, {@code Aggregate}, {@code SetOp}, {@code Correlate} and {@code TableScan} do not
 * override those methods, and {@code Calc.explainTerms} omits hints, so they fall back to a digest
 * that ignores the label entirely. Relabelling one of those and calling {@code transformTo} finds a
 * digest-equal rel already in the subset and discards the newly labelled object, leaving whichever
 * label was registered first. Because {@link #compose} is a canonical union the two paths almost
 * always agree, but they need not: a one-node rule such as {@code PROJECT_TO_CALC} labels its
 * output {@code Project} where a two-node rule producing the same program would label it {@code
 * Filter;Project}. This is the residual source of name variation, and it is why {@code
 * StageNameTest} asserts that repeated planning of the same query produces identical names.
 *
 * <h2>Labels over-claim</h2>
 *
 * <p>A label is a union that never splits. {@code StageNameRuleCall} stamps the composed label on
 * every node a rule built, so {@code FilterProjectTransposeRule}, which rebuilds both halves of the
 * pair it matched, leaves both of them labelled {@code Filter;Project}. If those halves end up in
 * separate stages rather than being merged, both stages claim both operations and only the {@code
 * #2} suffix tells them apart. That is the honest reason occurrences have to be numbered, and it is
 * the price of the confluence that makes labelling terminate at all: a label that shrank when nodes
 * separated would depend on the order rules fired, which is exactly what the previous paragraph
 * rules out.
 */
@Internal
public class StageName {

  /** Hint name under which the label is stored. */
  private static final String HINT = "BEAM_STAGE";

  /**
   * Experiment restoring the pre-provenance {@code BeamCalcRel_57} style names.
   *
   * <p>Dataflow matches streaming pipelines for update by step name, so a pipeline already running
   * cannot adopt the new names without being drained.
   */
  public static final String LEGACY_EXPERIMENT = "legacy-sql-transform-names";

  private static final String SEPARATOR = ";";
  private static final String POSITION_SEPARATOR = ":";
  private static final String ELLIPSIS = "...";

  /**
   * Longest rendered name. Beyond this the middle of the chain is elided. Names appear in runner
   * UIs and in per-step metric keys, so they cannot grow with plan size.
   */
  private static final int MAX_LENGTH = 100;

  private StageName() {}

  /** The only hint strategy table Beam installs; its identity marks a cluster as registered. */
  private static final HintStrategyTable STRATEGIES =
      HintStrategyTable.builder().hintStrategy(HINT, (hint, rel) -> true).build();

  /**
   * Declares {@link #HINT} to {@code cluster}, which the planner requires before it will tolerate a
   * node carrying it.
   *
   * <p>An unregistered hint trips an assertion the first time the planner tests a matched node for
   * rule exclusion. The predicate admits every node type, since the label is about provenance
   * rather than anything the node can act on.
   *
   * <p>{@link RelOptCluster#setHintStrategies} replaces the table rather than merging into it, and
   * a cluster can reach here more than once. Registering again is a no-op, but finding strategies
   * that Beam did not install is an error rather than something to silently discard: if SQL hint
   * syntax is ever enabled, this has to become a merge, and it is better to find that out from a
   * stack trace than from hints that quietly stop working.
   */
  public static void register(RelOptCluster cluster) {
    HintStrategyTable existing = cluster.getHintStrategies();
    if (existing == STRATEGIES) {
      return;
    }
    checkState(
        existing == HintStrategyTable.EMPTY,
        "Cluster already has hint strategies configured; %s would discard them",
        HINT);
    cluster.setHintStrategies(STRATEGIES);
  }

  /** Hints carrying {@code label}, for passing to a rel constructor or factory. */
  private static ImmutableList<RelHint> hintsFor(String label) {
    return ImmutableList.of(RelHint.builder(HINT).hintOption(label).build());
  }

  /** The label on {@code node} in its stored form, or null if it carries none. */
  public static @Nullable String storedLabel(RelNode node) {
    if (!(node instanceof Hintable)) {
      return null;
    }
    for (RelHint hint : ((Hintable) node).getHints()) {
      if (HINT.equals(hint.hintName) && !hint.listOptions.isEmpty()) {
        return hint.listOptions.get(0);
      }
    }
    return null;
  }

  /** The name to show for {@code node}, or null if it carries no label. */
  public static @Nullable String renderedName(RelNode node) {
    String label = storedLabel(node);
    return label == null ? null : render(label);
  }

  /**
   * Returns {@code node} labelled as having come from a source operation called {@code name}, or
   * {@code node} unchanged if it is already labelled or cannot be labelled.
   *
   * <p>Rels that do not override {@link Hintable#withHints} return themselves, so an un-plumbed rel
   * type silently keeps the default naming instead of failing.
   */
  private static RelNode stamp(RelNode node, String name) {
    if (storedLabel(node) != null) {
      return node;
    }
    // The node's own id orders it against the rest of the plan. Ids are handed out in construction
    // order and backfill builds bottom-up, so ordering by id is data-flow order. Only the relative
    // order matters, which is why an id being different on the next run is harmless.
    //
    // A node built later than its neighbours -- anything a pre-pass such as decorrelation left
    // behind -- gets a larger id and so renders after nodes it actually feeds. The name is still
    // complete; only its internal ordering is off.
    return relabel(node, node.getId() + POSITION_SEPARATOR + sanitize(name));
  }

  /**
   * Returns {@code node} carrying {@code label} in place of whatever label it had.
   *
   * <p>A rule that fuses nodes usually builds its result by copying one of them, which carries that
   * one's label along. The result belongs to all of them, so the composed label has to win.
   */
  public static RelNode relabel(RelNode node, String label) {
    if (!(node instanceof Hintable)) {
      return node;
    }
    // withHints() builds a whole new rel, and for a Calc that means re-validating its program,
    // which renders the program to a string. Rules re-fire on nodes they have already labelled
    // often enough -- transpose rules trade a pair back and forth, CalcMergeRule revisits merged
    // inputs -- that skipping the no-op case is most of the labelling cost on a large plan.
    if (label.equals(storedLabel(node))) {
      return node;
    }
    List<RelHint> hints = new ArrayList<>();
    for (RelHint hint : ((Hintable) node).getHints()) {
      if (!HINT.equals(hint.hintName)) {
        hints.add(hint);
      }
    }
    hints.addAll(hintsFor(label));
    return ((Hintable) node).withHints(hints);
  }

  /**
   * Labels every node in the tree rooted at {@code rel} that carries no label yet, after the kind
   * of node it is.
   *
   * <p>A SQL query has no user-visible operator names for its stages to inherit, and neither does
   * whatever the decorrelation pre-pass leaves behind, so a node is named for its own shape.
   */
  public static RelNode backfill(RelNode rel) {
    List<RelNode> inputs = new ArrayList<>();
    boolean rebuilt = false;
    for (RelNode input : rel.getInputs()) {
      RelNode labelled = backfill(input);
      rebuilt |= labelled != input;
      inputs.add(labelled);
    }
    RelNode result = rebuilt ? rel.copy(rel.getTraitSet(), inputs) : rel;
    return stamp(result, structuralName(result));
  }

  private static String structuralName(RelNode rel) {
    RelOptTable table = rel.getTable();
    if (rel instanceof TableScan && table != null) {
      List<String> qualifiedName = table.getQualifiedName();
      return "Scan(" + qualifiedName.get(qualifiedName.size() - 1) + ")";
    }
    String name = rel.getClass().getSimpleName();
    return name.startsWith("Logical") ? name.substring("Logical".length()) : name;
  }

  /**
   * Merges {@code labels} into the label for a node fused from all of them.
   *
   * <p>The result depends only on the <em>set</em> of source operations, not on the order the rule
   * happened to match them in: each is deduplicated by name and they are re-sorted by the position
   * they held in the original plan. So the label of a fused node is stable no matter which sequence
   * of rules assembled it, which is what keeps the planner from treating a re-derivation of the
   * same node as a new one.
   */
  public static String compose(List<String> labels) {
    Map<String, Integer> positions = new LinkedHashMap<>();
    for (String label : labels) {
      if (label == null) {
        continue;
      }
      for (String part : label.split(SEPARATOR, -1)) {
        if (!part.isEmpty()) {
          positions.merge(nameOf(part), positionOf(part), Math::min);
        }
      }
    }
    return positions.entrySet().stream()
        .sorted(
            (a, b) -> {
              int byPosition = Integer.compare(a.getValue(), b.getValue());
              return byPosition != 0 ? byPosition : a.getKey().compareTo(b.getKey());
            })
        .map(entry -> entry.getValue() + POSITION_SEPARATOR + entry.getKey())
        .reduce((a, b) -> a + SEPARATOR + b)
        .orElse("");
  }

  /** The displayable name for a stored label: its source operations, in plan order. */
  static String render(String label) {
    List<String> names = new ArrayList<>();
    for (String part : label.split(SEPARATOR, -1)) {
      if (!part.isEmpty()) {
        names.add(nameOf(part));
      }
    }
    return truncate(names);
  }

  private static String nameOf(String part) {
    int separator = part.indexOf(POSITION_SEPARATOR);
    return separator < 0 ? part : part.substring(separator + 1);
  }

  /**
   * The position prefix of a stored part. Unprefixed parts sort last; {@link #sanitize} makes them
   * unreachable from {@link #stamp}, but {@link #compose} is public and its input is a string.
   */
  private static int positionOf(String part) {
    int separator = part.indexOf(POSITION_SEPARATOR);
    if (separator < 0) {
      return Integer.MAX_VALUE;
    }
    try {
      return Integer.parseInt(part.substring(0, separator));
    } catch (NumberFormatException e) {
      return Integer.MAX_VALUE;
    }
  }

  /** Keeps a name free of the characters that delimit the stored form. */
  static String sanitize(String name) {
    return name.replace(SEPARATOR, "_").replace(POSITION_SEPARATOR, "_");
  }

  /**
   * Joins {@code names}, eliding the middle if the result would exceed {@link #MAX_LENGTH}. The
   * first and last are the recognizable ones, and the count of what was dropped signals how much
   * fusion happened.
   */
  private static String truncate(List<String> names) {
    String full = String.join(SEPARATOR, names);
    if (full.length() <= MAX_LENGTH) {
      return full;
    }
    if (names.size() < 3) {
      return clip(full);
    }
    String elided =
        names.get(0)
            + SEPARATOR
            + "+"
            + (names.size() - 2)
            + " more"
            + SEPARATOR
            + names.get(names.size() - 1);
    return elided.length() <= MAX_LENGTH ? elided : clip(elided);
  }

  /**
   * Cuts {@code name} down to {@link #MAX_LENGTH}. These names reach runner UIs and per-step metric
   * keys, so the marker is ASCII and the cut never lands inside a surrogate pair.
   */
  private static String clip(String name) {
    int end = MAX_LENGTH - ELLIPSIS.length();
    if (Character.isHighSurrogate(name.charAt(end - 1))) {
      end--;
    }
    return name.substring(0, end) + ELLIPSIS;
  }
}
