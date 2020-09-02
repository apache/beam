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

import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_AGGREGATE_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_ARRAY_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_FILTER_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_JOIN_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_LIMIT_OFFSET_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_ORDER_BY_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_PROJECT_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_SET_OPERATION_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_SINGLE_ROW_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_TABLE_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_TVFSCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_WITH_REF_SCAN;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_WITH_SCAN;
import static java.util.stream.Collectors.toList;

import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedQueryStmt;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;

/**
 * Converts a resolved Zeta SQL query represented by a tree to corresponding Calcite representation.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class QueryStatementConverter extends RelConverter<ResolvedQueryStmt> {

  /** Conversion rules, multimap from node kind to conversion rule. */
  private final ImmutableMultimap<ResolvedNodeKind, RelConverter> rules;

  public static RelNode convertRootQuery(ConversionContext context, ResolvedQueryStmt query) {
    return new QueryStatementConverter(context).convert(query, Collections.emptyList());
  }

  private QueryStatementConverter(ConversionContext context) {
    super(context);
    this.rules =
        ImmutableMultimap.<ResolvedNodeKind, RelConverter>builder()
            .put(RESOLVED_AGGREGATE_SCAN, new AggregateScanConverter(context))
            .put(RESOLVED_ARRAY_SCAN, new ArrayScanToJoinConverter(context))
            .put(RESOLVED_ARRAY_SCAN, new ArrayScanLiteralToUncollectConverter(context))
            .put(RESOLVED_ARRAY_SCAN, new ArrayScanColumnRefToUncollect(context))
            .put(RESOLVED_FILTER_SCAN, new FilterScanConverter(context))
            .put(RESOLVED_JOIN_SCAN, new JoinScanConverter(context))
            .put(RESOLVED_LIMIT_OFFSET_SCAN, new LimitOffsetScanToLimitConverter(context))
            .put(RESOLVED_LIMIT_OFFSET_SCAN, new LimitOffsetScanToOrderByLimitConverter(context))
            .put(RESOLVED_ORDER_BY_SCAN, new OrderByScanUnsupportedConverter(context))
            .put(RESOLVED_PROJECT_SCAN, new ProjectScanConverter(context))
            .put(RESOLVED_SET_OPERATION_SCAN, new SetOperationScanConverter(context))
            .put(RESOLVED_SINGLE_ROW_SCAN, new SingleRowScanConverter(context))
            .put(RESOLVED_TABLE_SCAN, new TableScanConverter(context))
            .put(RESOLVED_WITH_REF_SCAN, new WithRefScanConverter(context))
            .put(RESOLVED_WITH_SCAN, new WithScanConverter(context))
            .put(RESOLVED_TVFSCAN, new TVFScanConverter(context))
            .build();
  }

  @Override
  public RelNode convert(ResolvedQueryStmt zetaNode, List<RelNode> inputs) {
    if (zetaNode.getIsValueTable()) {
      throw new UnsupportedOperationException("Value Tables are not supported");
    }

    getTrait().addOutputColumnList(zetaNode.getOutputColumnList());

    return convertNode(zetaNode.getQuery());
  }

  /**
   * Convert node.
   *
   * <p>Finds a matching rule, uses the rule to extract inputs from the node, then converts the
   * inputs (recursively), then converts the node using the converted inputs.
   */
  private RelNode convertNode(ResolvedNode zetaNode) {
    RelConverter nodeConverter = getConverterRule(zetaNode);
    List<ResolvedNode> inputs = nodeConverter.getInputs(zetaNode);
    List<RelNode> convertedInputs = inputs.stream().map(this::convertNode).collect(toList());
    return nodeConverter.convert(zetaNode, convertedInputs);
  }

  private RelConverter getConverterRule(ResolvedNode zetaNode) {
    if (!rules.containsKey(zetaNode.nodeKind())) {
      throw new UnsupportedOperationException(
          String.format("Conversion of %s is not supported", zetaNode.nodeKind()));
    }

    return rules.get(zetaNode.nodeKind()).stream()
        .filter(relConverter -> relConverter.canConvert(zetaNode))
        .findFirst()
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format("Cannot find a conversion rule for: %s", zetaNode)));
  }
}
