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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelRecordType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLocalRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilderFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

public class BeamIOPushDownRule extends RelOptRule {
  // ~ Static fields/initializers ---------------------------------------------

  public static final BeamIOPushDownRule INSTANCE =
      new BeamIOPushDownRule(RelFactories.LOGICAL_BUILDER);

  // ~ Constructors -----------------------------------------------------------

  public BeamIOPushDownRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Calc.class, operand(BeamIOSourceRel.class, any())), relBuilderFactory, null);
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final BeamIOSourceRel ioSourceRel = call.rel(1);
    final BeamSqlTable beamSqlTable = ioSourceRel.getBeamSqlTable();

    // Nested rows are not supported at the moment
    for (RelDataTypeField field : ioSourceRel.getRowType().getFieldList()) {
      if (field.getType() instanceof RelRecordType) {
        return;
      }
    }

    final Calc calc = call.rel(0);
    final RexProgram program = calc.getProgram();
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();
    final RelDataType calcInputRowType = program.getInputRowType();

    // When predicate push-down is not supported - all filters are unsupported.
    final BeamSqlTableFilter tableFilter = beamSqlTable.constructFilter(projectFilter.right);
    if (!beamSqlTable.supportsProjects() && tableFilter instanceof DefaultTableFilter) {
      // Either project or filter push-down must be supported by the IO.
      return;
    }

    if (!(tableFilter instanceof DefaultTableFilter) && !beamSqlTable.supportsProjects()) {
      // TODO(BEAM-8508): add support for standalone filter push-down.
      // Filter push-down without project push-down is not supported for now.
      return;
    }

    // Find all input refs used by projects
    Set<String> usedFields = new LinkedHashSet<>();
    for (RexNode project : projectFilter.left) {
      findUtilizedInputRefs(calcInputRowType, project, usedFields);
    }

    // Find all input refs used by filters
    for (RexNode filter : tableFilter.getNotSupported()) {
      findUtilizedInputRefs(calcInputRowType, filter, usedFields);
    }

    FieldAccessDescriptor resolved =
        FieldAccessDescriptor.withFieldNames(usedFields)
            .withOrderByFieldInsertionOrder()
            .resolve(beamSqlTable.getSchema());
    Schema newSchema =
        SelectHelpers.getOutputSchema(ioSourceRel.getBeamSqlTable().getSchema(), resolved);
    RelDataType calcInputType =
        CalciteUtils.toCalciteRowType(newSchema, ioSourceRel.getCluster().getTypeFactory());

    // Check if the calc can be dropped:
    // 1. Calc only does projects and renames.
    //    And
    // 2. Predicate can be completely pushed-down to IO level.
    if (isProjectRenameOnlyProgram(program) && tableFilter.getNotSupported().isEmpty()) {
      // Tell the optimizer to not use old IO, since the new one is better.
      call.getPlanner().setImportance(ioSourceRel, 0.0);
      call.transformTo(ioSourceRel.copy(calc.getRowType(), newSchema.getFieldNames(), tableFilter));
      return;
    }

    // Already most optimal case:
    // Calc contains all unsupported filters.
    // IO only projects fields utilised by a calc.
    if (tableFilter.getNotSupported().equals(projectFilter.right)
        && usedFields.size() == ioSourceRel.getRowType().getFieldCount()) {
      return;
    }

    BeamIOSourceRel newIoSourceRel =
        ioSourceRel.copy(calcInputType, newSchema.getFieldNames(), tableFilter);
    RelBuilder relBuilder = call.builder();
    relBuilder.push(newIoSourceRel);

    List<RexNode> newProjects = new ArrayList<>();
    List<RexNode> newFilter = new ArrayList<>();
    // Ex: let's say the original fields are (number before each element is the index):
    // {0:unused1, 1:id, 2:name, 3:unused2},
    // where only 'id' and 'name' are being used. Then the new calcInputType should be as follows:
    // {0:id, 1:name}.
    // A mapping list will contain 2 entries: {0:1, 1:2},
    // showing how used field names map to the original fields.
    List<Integer> mapping =
        resolved.getFieldsAccessed().stream()
            .map(FieldDescriptor::getFieldId)
            .collect(Collectors.toList());

    // Map filters to new RexInputRef.
    for (RexNode filter : tableFilter.getNotSupported()) {
      newFilter.add(reMapRexNodeToNewInputs(filter, mapping));
    }
    // Map projects to new RexInputRef.
    for (RexNode project : projectFilter.left) {
      newProjects.add(reMapRexNodeToNewInputs(project, mapping));
    }

    relBuilder.filter(newFilter);
    relBuilder.project(
        newProjects, calc.getRowType().getFieldNames(), true); // Always preserve named projects.

    RelNode result = relBuilder.build();

    if (newFilter.size() < projectFilter.right.size()) {
      // Smaller Calc programs are indisputably better.
      // Tell the optimizer not to use old Calc and IO.
      call.getPlanner().setImportance(calc, 0.0);
      call.getPlanner().setImportance(ioSourceRel, 0.0);
      call.transformTo(result);
    } else if (newFilter.size() == projectFilter.right.size()) {
      // But we can consider something with the same number of filters.
      call.transformTo(result);
    }
  }

  /**
   * Given a {@code RexNode}, find all {@code RexInputRef}s a node or it's children nodes use.
   *
   * @param inputRowType {@code RelDataType} used for looking up names of {@code RexInputRef}.
   * @param startNode A node to start at.
   * @param usedFields Names of {@code RexInputRef}s are added to this list.
   */
  @VisibleForTesting
  void findUtilizedInputRefs(RelDataType inputRowType, RexNode startNode, Set<String> usedFields) {
    Queue<RexNode> prerequisites = new ArrayDeque<>();
    prerequisites.add(startNode);

    // Assuming there are no cyclic nodes, traverse dependency tree until all RexInputRefs are found
    while (!prerequisites.isEmpty()) {
      RexNode node = prerequisites.poll();

      if (node instanceof RexCall) { // Composite expression, example: "=($t11, $t12)"
        RexCall compositeNode = (RexCall) node;

        // Expression from example above contains 2 operands: $t11, $t12
        prerequisites.addAll(compositeNode.getOperands());
      } else if (node instanceof RexInputRef) { // Input reference
        // Find a field in an inputRowType for the input reference
        int inputFieldIndex = ((RexInputRef) node).getIndex();
        RelDataTypeField field = inputRowType.getFieldList().get(inputFieldIndex);

        // If we have not seen it before - add it to the list (hash set)
        usedFields.add(field.getName());
      } else if (node instanceof RexLiteral) {
        // Does not contain information about columns utilized by a Calc
      } else {
        throw new RuntimeException(
            "Unexpected RexNode encountered: " + node.getClass().getSimpleName());
      }
    }
  }

  /**
   * Recursively reconstruct a {@code RexNode}, mapping old RexInputRefs to new.
   *
   * @param node {@code RexNode} to reconstruct.
   * @param inputRefMapping Mapping from old {@code RexInputRefNode} indexes to new, where list
   *     index is the new {@code RexInputRefNode} and the value is old {@code RexInputRefNode}.
   * @return reconstructed {@code RexNode} with {@code RexInputRefNode} remapped to new values.
   */
  @VisibleForTesting
  RexNode reMapRexNodeToNewInputs(RexNode node, List<Integer> inputRefMapping) {
    if (node instanceof RexInputRef) {
      int oldInputIndex = ((RexInputRef) node).getIndex();
      int newInputIndex = inputRefMapping.indexOf(oldInputIndex);

      // Create a new input reference pointing to a new input field
      return new RexInputRef(newInputIndex, node.getType());
    } else if (node instanceof RexCall) { // Composite expression, example: "=($t11, $t12)"
      RexCall compositeNode = (RexCall) node;
      List<RexNode> newOperands = new ArrayList<>();

      for (RexNode operand : compositeNode.getOperands()) {
        newOperands.add(reMapRexNodeToNewInputs(operand, inputRefMapping));
      }

      return compositeNode.clone(compositeNode.getType(), newOperands);
    }

    // If node is anything else - return it as is (ex: Literal)
    checkArgument(
        node instanceof RexLiteral,
        "RexLiteral node expected, but was: " + node.getClass().getSimpleName());
    return node;
  }

  /**
   * Determine whether a program only performs renames and/or projects. RexProgram#isTrivial is not
   * sufficient in this case, because number of projects does not need to be the same as inputs.
   *
   * @param program A program to check.
   * @return True when program performs only projects (w/o any modifications), false otherwise.
   */
  @VisibleForTesting
  boolean isProjectRenameOnlyProgram(RexProgram program) {
    int fieldCount = program.getInputRowType().getFieldCount();
    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() >= fieldCount) {
        return false;
      }
    }

    return true;
  }
}
