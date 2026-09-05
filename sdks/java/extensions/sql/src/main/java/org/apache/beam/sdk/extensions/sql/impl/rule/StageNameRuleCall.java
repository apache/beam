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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.extensions.sql.impl.rel.StageName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelHintsPropagator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RelOptRuleCall} that labels the nodes a rule produces with the composed {@link
 * StageName} of the nodes the rule matched, so that a fused node records what was fused into it.
 *
 * <p>Everything other than {@code transformTo} is delegated to the original call, except {@code
 * getRule}, which hands back the rule being run rather than the {@link StageNameRule} wrapping it.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StageNameRuleCall extends RelOptRuleCall {
  private static final Logger LOG = LoggerFactory.getLogger(StageNameRuleCall.class);

  /** Keeps a rel type that cannot be labelled from filling the log, one line per JVM. */
  private static final AtomicBoolean LOGGED_FAILURE = new AtomicBoolean();

  private final RelOptRuleCall originalCall;
  private final RelOptRule delegate;

  /**
   * Composed label of the matched nodes, computed on the first {@code transformTo}.
   *
   * <p>Volcano constructs one of these per {@code onMatch}, and most matches return without
   * transforming anything. Composing eagerly would run a split, a map and a sort on every one of
   * them.
   */
  private @Nullable String label;

  StageNameRuleCall(RelOptRuleCall originalCall, RelOptRule delegate) {
    super(
        originalCall.getPlanner(),
        originalCall.getOperand0(),
        originalCall.rels,
        ImmutableMap.of(),
        null);
    this.originalCall = originalCall;
    this.delegate = delegate;
  }

  private String label() {
    if (label == null) {
      List<String> labels = new ArrayList<>();
      for (RelNode rel : originalCall.getRelList()) {
        labels.add(StageName.storedLabel(rel));
      }
      label = StageName.compose(labels);
    }
    return label;
  }

  @Override
  public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
    originalCall.transformTo(labelled(rel, equiv), equiv);
  }

  @Override
  public void transformTo(
      RelNode rel, Map<RelNode, RelNode> equiv, RelHintsPropagator hintsPropagator) {
    originalCall.transformTo(labelled(rel, equiv), equiv, hintsPropagator);
  }

  private RelNode labelled(RelNode rel, Map<RelNode, RelNode> equiv) {
    String composed = label();
    if (composed.isEmpty()) {
      return rel;
    }
    try {
      // An equivalence map keys off node instances inside `rel`. Rebuilding the subtree would
      // invalidate those keys, so in that case only the root is labelled -- rewriting the root
      // leaves its inputs untouched.
      return equiv.isEmpty() ? labelSubtree(rel, composed) : StageName.relabel(rel, composed);
    } catch (RuntimeException e) {
      // Naming is cosmetic, so a rel type whose copy() or withHints() rejects this is not worth
      // failing a query over. It is worth hearing about once: the fallback name is silent
      // otherwise, and nobody reads debug in production.
      if (LOGGED_FAILURE.compareAndSet(false, true)) {
        LOG.warn(
            "Could not label rule output with stage name '{}'; {} will fall back to default"
                + " transform naming. Further occurrences are logged at debug.",
            composed,
            rel.getClass().getSimpleName(),
            e);
      } else {
        LOG.debug("Could not label rule output with stage name '{}'", composed, e);
      }
      return rel;
    }
  }

  /**
   * Labels {@code rel} and every descendant the rule built underneath it.
   *
   * <p>Several rules assemble a chain of nodes and hand only its root to {@code transformTo}, so
   * labelling the root alone would leave the intermediates anonymous. {@link RelSubset} and {@link
   * HepRelVertex} mark the boundary between what the rule built and the inputs it was given.
   *
   * <p>That boundary is not exact under Volcano, which hands a multi-operand rule concrete rels
   * rather than subsets. A rule that splices a node it matched straight into its output has that
   * node copied and relabelled here. The label it receives is the composed one, which already
   * covers it, so the name stays correct -- but it is an extra rel in the memo.
   */
  private RelNode labelSubtree(RelNode rel, String label) {
    if (rel instanceof RelSubset || rel instanceof HepRelVertex) {
      return rel;
    }
    List<RelNode> inputs = new ArrayList<>();
    boolean rebuilt = false;
    for (RelNode input : rel.getInputs()) {
      RelNode labelled = labelSubtree(input, label);
      rebuilt |= labelled != input;
      inputs.add(labelled);
    }
    RelNode result = rebuilt ? rel.copy(rel.getTraitSet(), inputs) : rel;
    return StageName.relabel(result, label);
  }

  // Methods that are delegated to originalCall.

  @Override
  public RelOptRuleOperand getOperand0() {
    return originalCall.getOperand0();
  }

  /**
   * The rule whose {@code onMatch} is running, not the {@link StageNameRule} the planner dispatched
   * through. Rule bodies cast this to their own type.
   */
  @Override
  public RelOptRule getRule() {
    return delegate;
  }

  @Override
  public List<RelNode> getRelList() {
    return originalCall.getRelList();
  }

  @Override
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T extends RelNode> T rel(int ordinal) {
    return originalCall.rel(ordinal);
  }

  @Override
  public List<RelNode> getChildRels(RelNode rel) {
    return originalCall.getChildRels(rel);
  }

  @Override
  public RelOptPlanner getPlanner() {
    return originalCall.getPlanner();
  }

  @Override
  public RelMetadataQuery getMetadataQuery() {
    return originalCall.getMetadataQuery();
  }

  @Override
  public List<RelNode> getParents() {
    return originalCall.getParents();
  }

  @Override
  public boolean isRuleExcluded() {
    return originalCall.isRuleExcluded();
  }

  @Override
  public RelBuilder builder() {
    return originalCall.builder();
  }
}
