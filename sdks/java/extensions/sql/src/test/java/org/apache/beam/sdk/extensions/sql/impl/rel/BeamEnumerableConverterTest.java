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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.junit.Test;

/** Test for {@code BeamEnumerableConverter}. */
public class BeamEnumerableConverterTest {
  static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  static RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  static PipelineOptions options = PipelineOptionsFactory.create();
  static RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

  @Test
  public void testToEnumerable_collectSingle() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ZERO)));
    BeamRelNode node = new BeamValuesRel(cluster, type, tuples, null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(options, node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    assertEquals(0L, enumerator.current());
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testToEnumerable_collectMultiple() {
    Schema schema = Schema.builder().addInt64Field("id").addInt64Field("otherid").build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE)));
    BeamRelNode node = new BeamValuesRel(cluster, type, tuples, null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(options, node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = (Object[]) enumerator.current();
    assertEquals(2, row.length);
    assertEquals(0L, row[0]);
    assertEquals(1L, row[1]);
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testToListRow_collectMultiple() {
    Schema schema = Schema.builder().addInt64Field("id").addInt64Field("otherid").build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                rexBuilder.makeBigintLiteral(BigDecimal.ZERO),
                rexBuilder.makeBigintLiteral(BigDecimal.ONE)));
    BeamRelNode node = new BeamValuesRel(cluster, type, tuples, null);

    List<Row> rowList = BeamEnumerableConverter.toRowList(options, node);
    assertTrue(rowList.size() == 1);
    assertEquals(Row.withSchema(schema).addValues(0L, 1L).build(), rowList.get(0));
  }

  @Test
  public void testToEnumerable_collectNullValue() {
    Schema schema = Schema.builder().addNullableField("id", FieldType.INT64).build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                rexBuilder.makeNullLiteral(
                    CalciteUtils.toRelDataType(TYPE_FACTORY, FieldType.INT64))));
    BeamRelNode node = new BeamValuesRel(cluster, type, tuples, null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(options, node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object row = enumerator.current();
    assertEquals(null, row);
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  private static class FakeTable extends BaseBeamTable {
    public FakeTable() {
      super(null);
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return null;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      return null;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      input.apply(
          ParDo.of(
              new DoFn<Row, Void>() {
                @ProcessElement
                public void processElement(ProcessContext context) {}
              }));
      return PDone.in(input.getPipeline());
    }
  }

  @Test
  public void testToEnumerable_count() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    RelDataType type = CalciteUtils.toCalciteRowType(schema, TYPE_FACTORY);
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ZERO)),
            ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ONE)));
    BeamRelNode node =
        new BeamIOSinkRel(
            cluster,
            RelOptTableImpl.create(null, type, ImmutableList.of(), null),
            null,
            new BeamValuesRel(cluster, type, tuples, null),
            null,
            null,
            null,
            false,
            new FakeTable(),
            null);

    Enumerable<Object> enumerable = BeamEnumerableConverter.toEnumerable(options, node);
    Enumerator<Object> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    assertEquals(2L, enumerator.current());
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }
}
