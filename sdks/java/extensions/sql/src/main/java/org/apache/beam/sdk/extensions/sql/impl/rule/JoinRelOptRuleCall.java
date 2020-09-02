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

import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelHintsPropagator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilder;

/**
 * This is a class to catch the built join and check if it is a legal join before passing it to the
 * actual RelOptRuleCall.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class JoinRelOptRuleCall extends RelOptRuleCall {
  private final RelOptRuleCall originalCall;
  private final JoinChecker checker;

  JoinRelOptRuleCall(RelOptRuleCall originalCall, JoinChecker checker) {
    super(originalCall.getPlanner(), originalCall.getOperand0(), originalCall.rels, null, null);
    this.originalCall = originalCall;
    this.checker = checker;
  }

  // This is the only method that is different than originalCall. Everything else is delegated to
  // originalCall.
  @Override
  public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
    if (checker.check(rel)) {
      originalCall.transformTo(rel, equiv);
    }
  }

  @Override
  public void transformTo(
      RelNode relNode, Map<RelNode, RelNode> map, RelHintsPropagator relHintsPropagator) {
    if (checker.check(relNode)) {
      // FIXME: CHECK IF THIS IS CORRECT
      originalCall.transformTo(relNode, map, relHintsPropagator);
    }
  }

  /** This is a function gets the output relation and checks if it is a legal relational node. */
  public interface JoinChecker {
    boolean check(RelNode rel);
  }

  // Methods that are delegated to originalCall.

  @Override
  public RelOptRuleOperand getOperand0() {
    return originalCall.getOperand0();
  }

  @Override
  public RelOptRule getRule() {
    return originalCall.getRule();
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
  public RelBuilder builder() {
    return originalCall.builder();
  }
}
