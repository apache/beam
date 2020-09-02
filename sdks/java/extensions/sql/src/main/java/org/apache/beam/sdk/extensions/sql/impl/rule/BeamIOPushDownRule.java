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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelRecordType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLocalRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
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

    if (ioSourceRel instanceof BeamPushDownIOSourceRel) {
      return;
    }

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
    if (!beamSqlTable.supportsProjects().isSupported()
        && tableFilter instanceof DefaultTableFilter) {
      // Either project or filter push-down must be supported by the IO.
      return;
    }

    Set<String> usedFields = new LinkedHashSet<>();
    if (!(tableFilter instanceof DefaultTableFilter)
        && !beamSqlTable.supportsProjects().isSupported()) {
      // When applying standalone filter push-down all fields must be project by an IO.
      // With a single exception: Calc projects all fields (in the same order) and does nothing
      // else.
      usedFields.addAll(calcInputRowType.getFieldNames());
    } else {
      // Find all input refs used by projects
      for (RexNode project : projectFilter.left) {
        findUtilizedInputRefs(calcInputRowType, project, usedFields);
      }

      // Find all input refs used by filters
      for (RexNode filter : tableFilter.getNotSupported()) {
        findUtilizedInputRefs(calcInputRowType, filter, usedFields);
      }
    }

    if (usedFields.isEmpty()) {
      // No need to do push-down for queries like this: "select UPPER('hello')".
      return;
    }

    // Already most optimal case:
    // Calc contains all unsupported filters.
    // IO only projects fields utilized by a calc.
    if (tableFilter.getNotSupported().containsAll(projectFilter.right)
        && usedFields.containsAll(ioSourceRel.getRowType().getFieldNames())) {
      return;
    }

    FieldAccessDescriptor resolved = FieldAccessDescriptor.withFieldNames(usedFields);
    resolved = resolved.resolve(beamSqlTable.getSchema());

    if (canDropCalc(program, beamSqlTable.supportsProjects(), tableFilter)) {
      // Tell the optimizer to not use old IO, since the new one is better.
      call.getPlanner().prune(ioSourceRel);
      call.transformTo(
          ioSourceRel.createPushDownRel(
              calc.getRowType(),
              resolved.getFieldsAccessed().stream()
                  .map(FieldDescriptor::getFieldName)
                  .collect(Collectors.toList()),
              tableFilter));
      return;
    }

    // Already most optimal case:
    // Calc contains all unsupported filters.
    // IO only projects fields utilised by a calc.
    if (tableFilter.getNotSupported().equals(projectFilter.right)
        && usedFields.containsAll(ioSourceRel.getRowType().getFieldNames())) {
      return;
    }

    RelNode result =
        constructNodesWithPushDown(
            resolved,
            call.builder(),
            ioSourceRel,
            tableFilter,
            calc.getRowType(),
            projectFilter.left);

    if (tableFilter.getNotSupported().size() <= projectFilter.right.size()
        || usedFields.size() < calcInputRowType.getFieldCount()) {
      // Smaller Calc programs are indisputably better, as well as IOs with less projected fields.
      // We can consider something with the same number of filters.
      // Tell the optimizer not to use old Calc and IO.
      call.getPlanner().prune(ioSourceRel);
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
        throw new UnsupportedOperationException(
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
   * Calc should NOT be dropped in the following cases:<br>
   * 1. Projected fields are manipulated (ex: 'select field1+10').<br>
   * 2. When the same field projected more than once.<br>
   * 3. When an IO does not supports field reordering and projects fields in a different (from
   * schema) order.
   *
   * @param program A program to check.
   * @param projectReorderingSupported Whether project push-down supports field reordering.
   * @return True when program performs only projects (w/o any modifications), false otherwise.
   */
  @VisibleForTesting
  boolean isProjectRenameOnlyProgram(RexProgram program, boolean projectReorderingSupported) {
    int fieldCount = program.getInputRowType().getFieldCount();
    Set<Integer> projectIndex = new HashSet<>();
    int previousIndex = -1;
    for (RexLocalRef ref : program.getProjectList()) {
      int index = ref.getIndex();
      if (index >= fieldCount // Projected values are InputRefs.
          || !projectIndex.add(ref.getIndex()) // Each field projected once.
          || (!projectReorderingSupported && index <= previousIndex)) { // In the same order.
        return false;
      }
      previousIndex = index;
    }

    return true;
  }

  /**
   * Perform a series of checks to determine whether a Calc can be dropped. Following conditions
   * need to be met in order for that to happen (logical AND):<br>
   * 1. Program should do simple projects, project each field once, and project fields in the same
   * order when field reordering is not supported.<br>
   * 2. Predicate can be completely pushed-down.<br>
   * 3. Project push-down is supported by the IO or all fields are projected by a Calc.
   *
   * @param program A {@code RexProgram} of a {@code Calc}.
   * @param projectSupport An enum containing information about IO project push-down capabilities.
   * @param tableFilter A class containing information about IO predicate push-down capabilities.
   * @return True when Calc can be dropped, false otherwise.
   */
  private boolean canDropCalc(
      RexProgram program, ProjectSupport projectSupport, BeamSqlTableFilter tableFilter) {
    RelDataType calcInputRowType = program.getInputRowType();

    // Program should do simple projects, project each field once, and project fields in the same
    // order when field reordering is not supported.
    boolean fieldReorderingSupported = projectSupport.withFieldReordering();
    if (!isProjectRenameOnlyProgram(program, fieldReorderingSupported)) {
      return false;
    }
    // Predicate can be completely pushed-down
    if (!tableFilter.getNotSupported().isEmpty()) {
      return false;
    }
    // Project push-down is supported by the IO or all fields are projected by a Calc.
    boolean isProjectSupported = projectSupport.isSupported();
    boolean allFieldsProjected =
        program.getProjectList().stream()
            .map(ref -> program.getInputRowType().getFieldList().get(ref.getIndex()).getName())
            .collect(Collectors.toList())
            .equals(calcInputRowType.getFieldNames());
    return isProjectSupported || allFieldsProjected;
  }

  /**
   * Construct a new {@link BeamIOSourceRel} with predicate and/or project pushed-down and a new
   * {@code Calc} to do field reordering/field duplication/complex projects.
   *
   * @param resolved A descriptor of fields used by a {@code Calc}.
   * @param relBuilder A {@code RelBuilder} for constructing {@code Project} and {@code Filter} Rel
   *     nodes with operations unsupported by the IO.
   * @param ioSourceRel Original {@code BeamIOSourceRel} we are attempting to perform push-down for.
   * @param tableFilter A class containing information about IO predicate push-down capabilities.
   * @param calcDataType A Calcite output schema of an original {@code Calc}.
   * @param calcProjects A list of projected {@code RexNode}s by a {@code Calc}.
   * @return An alternative {@code RelNode} with supported filters/projects pushed-down to IO Rel.
   */
  private RelNode constructNodesWithPushDown(
      FieldAccessDescriptor resolved,
      RelBuilder relBuilder,
      BeamIOSourceRel ioSourceRel,
      BeamSqlTableFilter tableFilter,
      RelDataType calcDataType,
      List<RexNode> calcProjects) {
    Schema newSchema =
        SelectHelpers.getOutputSchema(ioSourceRel.getBeamSqlTable().getSchema(), resolved);
    RelDataType calcInputType =
        CalciteUtils.toCalciteRowType(newSchema, ioSourceRel.getCluster().getTypeFactory());

    BeamIOSourceRel newIoSourceRel =
        ioSourceRel.createPushDownRel(calcInputType, newSchema.getFieldNames(), tableFilter);
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
    for (RexNode project : calcProjects) {
      newProjects.add(reMapRexNodeToNewInputs(project, mapping));
    }

    relBuilder.filter(newFilter);
    // Force to preserve named projects.
    relBuilder.project(newProjects, calcDataType.getFieldNames(), true);

    return relBuilder.build();
  }
}
