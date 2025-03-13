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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.byteStringUtf8;
import static org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.bigtable.v2.RowFilter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;

/**
 * BigtableFilter for queries with WHERE clause.
 *
 * <p>Currently only queries with a single LIKE statement by key field with <a
 * href=https://github.com/google/re2/wiki/Syntax>RE2 Syntax</a> regex type are supported, e.g.
 * `SELECT * FROM table WHERE key LIKE '^key\d'`
 */
class BigtableFilter implements BeamSqlTableFilter {
  private final List<RexNode> supported;
  private final List<RexNode> unsupported;
  private final Schema schema;

  BigtableFilter(List<RexNode> predicateCNF, Schema schema) {
    supported = predicateCNF.stream().filter(BigtableFilter::isSupported).collect(toList());
    unsupported =
        predicateCNF.stream().filter(predicate -> !isSupported(predicate)).collect(toList());
    this.schema = schema;
  }

  @Override
  public List<RexNode> getNotSupported() {
    return unsupported;
  }

  @Override
  public int numSupported() {
    return BeamSqlTableFilter.expressionsInFilter(supported);
  }

  public List<RexNode> getSupported() {
    return supported;
  }

  @Override
  public String toString() {
    String supStr = supported.stream().map(RexNode::toString).collect(Collectors.joining());
    String unsupStr = unsupported.stream().map(RexNode::toString).collect(Collectors.joining());
    return String.format("[supported{%s}, unsupported{%s}]", supStr, unsupStr);
  }

  RowFilter getFilters() {
    checkArgument(
        supported.size() == 1,
        String.format("Only one LIKE operation is allowed. Got %s operations", supported.size()));
    return translateRexNodeToRowFilter(supported.get(0));
  }

  private RowFilter translateRexNodeToRowFilter(RexNode node) {
    checkNodeIsCoposite(node);
    checkArgument(LIKE.equals(node.getKind()), "Only LIKE operation is supported.");

    List<RexLiteral> literals = filterOperands((RexCall) node, RexLiteral.class);
    List<RexInputRef> inputRefs = filterOperands((RexCall) node, RexInputRef.class);

    checkArgument(literals.size() == 1);
    checkArgument(inputRefs.size() == 1);

    checkFieldIsKey(inputRefs.get(0));
    String literal = literals.get(0).getValueAs(String.class);

    if (literal == null) {
      throw new IllegalArgumentException("Expected non-null String literal");
    }

    return RowFilter.newBuilder().setRowKeyRegexFilter(byteStringUtf8(literal)).build();
  }

  private void checkFieldIsKey(RexInputRef inputRef) {
    String inputFieldName = schema.getField(inputRef.getIndex()).getName();
    checkArgument(
        KEY.equals(inputFieldName),
        "Only 'key' queries are supported. Got field " + inputFieldName);
  }

  private static boolean isSupported(RexNode node) {
    checkNodeIsCoposite(node);
    if (!LIKE.equals(node.getKind())) {
      return false;
    }

    long literalsCount = countOperands((RexCall) node, RexLiteral.class);
    long fieldsCount = countOperands((RexCall) node, RexInputRef.class);

    return literalsCount == 1 && fieldsCount == 1;
  }

  private <T extends RexNode> List<T> filterOperands(RexCall compositeNode, Class<T> clazz) {
    return compositeNode.getOperands().stream()
        .filter(clazz::isInstance)
        .map(clazz::cast)
        .collect(toList());
  }

  private static <T extends RexNode> long countOperands(RexCall compositeNode, Class<T> clazz) {
    return compositeNode.getOperands().stream().filter(clazz::isInstance).count();
  }

  private static void checkNodeIsCoposite(RexNode node) {
    checkArgument(
        node instanceof RexCall,
        String.format(
            "Encountered an unexpected node type: %s. Should be %s",
            node.getClass().getSimpleName(), RexCall.class.getSimpleName()));
  }
}
