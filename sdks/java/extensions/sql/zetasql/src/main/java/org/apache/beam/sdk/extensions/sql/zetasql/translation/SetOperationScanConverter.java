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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.EXCEPT_ALL;
import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.EXCEPT_DISTINCT;
import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.INTERSECT_ALL;
import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.INTERSECT_DISTINCT;
import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.UNION_ALL;
import static com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType.UNION_DISTINCT;
import static java.util.stream.Collectors.toList;

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSetOperationItem;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedSetOperationScan;
import com.google.zetasql.resolvedast.ResolvedSetOperationScanEnums.SetOperationType;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Converts set operations. */
class SetOperationScanConverter extends RelConverter<ResolvedSetOperationScan> {
  private enum Type {
    DISTINCT,
    ALL
  }

  private static final ImmutableMap<SetOperationType, Function<List<RelNode>, RelNode>>
      SET_OPERATION_FACTORIES =
          ImmutableMap.<SetOperationType, Function<List<RelNode>, RelNode>>builder()
              .put(UNION_ALL, createFactoryFor(LogicalUnion::create, Type.ALL))
              .put(UNION_DISTINCT, createFactoryFor(LogicalUnion::create, Type.DISTINCT))
              .put(INTERSECT_ALL, createFactoryFor(LogicalIntersect::create, Type.ALL))
              .put(INTERSECT_DISTINCT, createFactoryFor(LogicalIntersect::create, Type.DISTINCT))
              .put(EXCEPT_ALL, createFactoryFor(LogicalMinus::create, Type.ALL))
              .put(EXCEPT_DISTINCT, createFactoryFor(LogicalMinus::create, Type.DISTINCT))
              .build();

  /**
   * A little closure to wrap the invocation of the factory method (e.g. LogicalUnion::create) for
   * the set operation node.
   */
  private static Function<List<RelNode>, RelNode> createFactoryFor(
      BiFunction<List<RelNode>, Boolean, RelNode> setOperationFactory, Type type) {
    return (List<RelNode> inputs) -> createRel(setOperationFactory, type == Type.ALL, inputs);
  }

  SetOperationScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedSetOperationScan zetaNode) {
    return zetaNode.getInputItemList().stream()
        .map(ResolvedSetOperationItem::getScan)
        .collect(toList());
  }

  @Override
  public RelNode convert(ResolvedSetOperationScan zetaNode, List<RelNode> inputs) {
    if (!SET_OPERATION_FACTORIES.containsKey(zetaNode.getOpType())) {
      throw new UnsupportedOperationException(
          "Operation " + zetaNode.getOpType() + " is unsupported");
    }

    return SET_OPERATION_FACTORIES.get(zetaNode.getOpType()).apply(inputs);
  }

  /** Beam set operations rel expects two inputs, so we are constructing a binary tree here. */
  private static RelNode createRel(
      BiFunction<List<RelNode>, Boolean, RelNode> factory, boolean all, List<RelNode> inputs) {
    return inputs.stream()
        .skip(2)
        .reduce(
            // start with creating a set node for two first inputs
            invokeFactory(factory, inputs.get(0), inputs.get(1), all),
            // create another operation node with previous op node and the next input
            (setOpNode, nextInput) -> invokeFactory(factory, setOpNode, nextInput, all));
  }

  /**
   * Creates a set operation rel with two inputs.
   *
   * <p>Factory is, for example, LogicalUnion::create.
   */
  private static RelNode invokeFactory(
      BiFunction<List<RelNode>, Boolean, RelNode> factory,
      RelNode input1,
      RelNode input2,
      boolean all) {
    return factory.apply(ImmutableList.of(input1, input2), all);
  }
}
