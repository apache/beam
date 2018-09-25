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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.autoCastField;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * {@code BeamRelNode} to replace a {@code Values} node.
 *
 * <p>{@code BeamValuesRel} will be used in the following SQLs:
 *
 * <ul>
 *   <li>{@code insert into t (name, desc) values ('hello', 'world')}
 *   <li>{@code select 1, '1', LOCALTIME}
 * </ul>
 */
public class BeamValuesRel extends Values implements BeamRelNode {

  public BeamValuesRel(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
  }

  @Override
  public Map<String, String> getPipelineOptions() {
    return ImmutableMap.of();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 0,
          "Should not have received input for %s: %s",
          BeamValuesRel.class.getSimpleName(),
          pinput);

      if (tuples.isEmpty()) {
        throw new IllegalStateException("Values with empty tuples!");
      }

      Schema schema = CalciteUtils.toSchema(getRowType());

      List<Row> rows = tuples.stream().map(tuple -> tupleToRow(schema, tuple)).collect(toList());

      return pinput.getPipeline().begin().apply(Create.of(rows)).setRowSchema(schema);
    }
  }

  private Row tupleToRow(Schema schema, ImmutableList<RexLiteral> tuple) {
    return IntStream.range(0, tuple.size())
        .mapToObj(i -> autoCastField(schema.getField(i), tuple.get(i).getValue()))
        .collect(toRow(schema));
  }
}
