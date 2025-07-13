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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * A {@link BeamSqlTableFilter} for ParquetIO that classifies filters as supported or unsupported.
 */
public class ParquetFilter implements BeamSqlTableFilter {
  // The set of operators that can be pushed down.
  private static final ImmutableSet<SqlKind> SUPPORTED_OPS =
      ImmutableSet.of(
          SqlKind.AND,
          SqlKind.OR,
          SqlKind.NOT,
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.IN,
          SqlKind.IS_NULL,
          SqlKind.IS_NOT_NULL);

  private final List<RexNode> supported;
  private final List<RexNode> unsupported;

  public ParquetFilter(List<RexNode> predicateCNF) {
    Pair<List<RexNode>, List<RexNode>> classifiedFilters = classify(predicateCNF);
    this.supported = classifiedFilters.getLeft();
    this.unsupported = classifiedFilters.getRight();
  }

  public Set<String> getReferencedFields(Schema beamSchema) {
    Set<String> fields = new HashSet<>();
    for (RexNode node : supported) {
      collectReferencedFields(node, beamSchema, fields);
    }
    return fields;
  }

  private static void collectReferencedFields(RexNode node, Schema beamSchema, Set<String> fields) {
    if (node instanceof RexInputRef) {
      fields.add(beamSchema.getField(((RexInputRef) node).getIndex()).getName());
    } else if (node instanceof RexCall) {
      for (RexNode operand : ((RexCall) node).getOperands()) {
        collectReferencedFields(operand, beamSchema, fields);
      }
    }
  }

  /** Static helper method to classify filters. */
  private static Pair<List<RexNode>, List<RexNode>> classify(List<RexNode> predicates) {
    ImmutableList.Builder<RexNode> supportedBuilder = ImmutableList.builder();
    ImmutableList.Builder<RexNode> unsupportedBuilder = ImmutableList.builder();

    for (RexNode node : predicates) {
      if (isSupported(node).getLeft()) {
        supportedBuilder.add(node);
      } else {
        unsupportedBuilder.add(node);
      }
    }
    return Pair.of(supportedBuilder.build(), unsupportedBuilder.build());
  }

  @Override
  public List<RexNode> getNotSupported() {
    return unsupported;
  }

  @Override
  public int numSupported() {
    return BeamSqlTableFilter.expressionsInFilter(checkStateNotNull(supported));
  }

  public List<RexNode> getSupported() {
    return supported;
  }

  private static Pair<Boolean, Integer> isSupported(RexNode node) {
    if (!(node instanceof RexCall)) {
      return Pair.of(node instanceof RexLiteral || node instanceof RexInputRef, 0);
    }

    RexCall call = (RexCall) node;
    if (!SUPPORTED_OPS.contains(call.getKind())) {
      return Pair.of(false, 0);
    }

    boolean allOperandsSupported = true;
    int inputRefCount = 0;
    for (RexNode operand : call.getOperands()) {
      if (operand instanceof RexInputRef) {
        inputRefCount++;
      } else if (operand instanceof RexCall) {
        Pair<Boolean, Integer> childSupport = isSupported(operand);
        if (!childSupport.getLeft()) {
          allOperandsSupported = false;
          break;
        }
        inputRefCount += childSupport.getRight();
      }
    }

    boolean isStructureSupported = inputRefCount <= 1;

    return Pair.of(allOperandsSupported && isStructureSupported, inputRefCount);
  }
}
