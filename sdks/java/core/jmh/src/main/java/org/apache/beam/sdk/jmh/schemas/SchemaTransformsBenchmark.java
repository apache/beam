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
package org.apache.beam.sdk.jmh.schemas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Cast;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmarks for schema transform steady-state execution paths ({@link RowSelector} / {@link
 * SelectHelpers}, {@link Group} {@code SchemaAggregateFn}, and {@link Cast#castRow}).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings({"nullness", "rawtypes", "unchecked"})
public class SchemaTransformsBenchmark {

  @State(Scope.Thread)
  public static class TransformState {
    Schema wideSchema;
    Row wideRow;
    RowSelector wildcardSelector;
    RowSelector flatSubsetSelector;

    Schema nestedContainerSchema;
    Row nestedContainerRow;
    RowSelector listProjectSelector;
    RowSelector mapProjectSelector;

    Schema aggInputSchema;
    Row[] aggInputRows;
    CombineFn<Row, Object, Row> flat4AggFn;

    Schema castInputSchema;
    Schema castIdenticalSchema;
    Schema castWidenedSchema;
    Row castRow;

    @Setup
    public void setUp() {
      Schema.Builder wb = Schema.builder();
      Object[] wv = new Object[30];
      for (int i = 0; i < 30; i++) {
        if (i % 3 == 0) {
          wb.addInt64Field("l_" + i);
          wv[i] = 100L + i;
        } else if (i % 3 == 1) {
          wb.addDoubleField("d_" + i);
          wv[i] = 1.5 * i;
        } else {
          wb.addStringField("s_" + i);
          wv[i] = "str_" + i;
        }
      }
      wideSchema = wb.build();
      wideRow = Row.withSchema(wideSchema).addValues(wv).build();
      wildcardSelector =
          SelectHelpers.getRowSelectorOptimized(
              wideSchema, FieldAccessDescriptor.withAllFields().resolve(wideSchema));
      flatSubsetSelector =
          SelectHelpers.getRowSelectorOptimized(
              wideSchema,
              FieldAccessDescriptor.withFieldNames("l_0", "d_1", "s_2").resolve(wideSchema));

      Schema elemSchema =
          Schema.builder()
              .addStringField("a")
              .addInt64Field("b")
              .addDoubleField("c")
              .addBooleanField("d")
              .build();
      nestedContainerSchema =
          Schema.builder()
              .addInt64Field("id")
              .addArrayField("items", FieldType.row(elemSchema))
              .addMapField("entries", FieldType.STRING, FieldType.row(elemSchema))
              .build();
      List<Row> items = new ArrayList<>(16);
      Map<String, Row> entries = new HashMap<>();
      for (int i = 0; i < 16; i++) {
        Row r =
            Row.withSchema(elemSchema).addValues("n" + i, (long) i, i * 2.5, i % 2 == 0).build();
        items.add(r);
        entries.put("k" + i, r);
      }
      nestedContainerRow =
          Row.withSchema(nestedContainerSchema).addValues(1L, items, entries).build();
      listProjectSelector =
          SelectHelpers.getRowSelectorOptimized(
              nestedContainerSchema,
              FieldAccessDescriptor.withFieldNames("items.a", "items.b", "items.c")
                  .resolve(nestedContainerSchema));
      mapProjectSelector =
          SelectHelpers.getRowSelectorOptimized(
              nestedContainerSchema,
              FieldAccessDescriptor.withFieldNames("entries.a", "entries.b", "entries.c")
                  .resolve(nestedContainerSchema));

      aggInputSchema =
          Schema.builder()
              .addStringField("userId")
              .addStringField("country")
              .addInt64Field("cost")
              .addDoubleField("duration")
              .addStringField("payload")
              .build();
      aggInputRows = new Row[16];
      for (int i = 0; i < aggInputRows.length; i++) {
        aggInputRows[i] =
            Row.withSchema(aggInputSchema)
                .addValues("user-1", "US", 10L * (i + 1), 1.5 * (i + 1), "extra-payload-" + i)
                .build();
      }
      flat4AggFn = extractSchemaAggregateFn(aggInputSchema);

      Schema childIn = Schema.builder().addStringField("sku").addInt32Field("qty").build();
      Schema childWide = Schema.builder().addStringField("sku").addInt64Field("qty").build();
      castInputSchema =
          Schema.builder()
              .addInt32Field("id")
              .addStringField("name")
              .addRowField("child", childIn)
              .addArrayField("tags", FieldType.STRING)
              .addMapField("counts", FieldType.STRING, FieldType.INT32)
              .build();
      castIdenticalSchema =
          Schema.builder()
              .addInt32Field("id")
              .addStringField("name")
              .addRowField("child", childIn)
              .addArrayField("tags", FieldType.STRING)
              .addMapField("counts", FieldType.STRING, FieldType.INT32)
              .build();
      castWidenedSchema =
          Schema.builder()
              .addInt64Field("id")
              .addStringField("name")
              .addRowField("child", childWide)
              .addArrayField("tags", FieldType.STRING)
              .addMapField("counts", FieldType.STRING, FieldType.INT64)
              .build();
      List<String> tags = new ArrayList<>(8);
      Map<String, Integer> counts = new HashMap<>();
      for (int i = 0; i < 8; i++) {
        tags.add("tag-" + i);
        counts.put("k" + i, i);
      }
      castRow =
          Row.withSchema(castInputSchema)
              .addValues(
                  42, "alice", Row.withSchema(childIn).addValues("sku-1", 5).build(), tags, counts)
              .build();
    }

    private static CombineFn<Row, Object, Row> extractSchemaAggregateFn(Schema schema) {
      Pipeline p = Pipeline.create();
      PCollection<Row> input =
          p.apply(Create.empty(org.apache.beam.sdk.coders.RowCoder.of(schema)))
              .setRowSchema(schema);
      input.apply(
          Group.<Row>byFieldNames("userId", "country")
              .aggregateField("cost", Sum.ofLongs(), "total_cost")
              .aggregateField("cost", Min.ofLongs(), "min_cost")
              .aggregateField("cost", Max.ofLongs(), "max_cost")
              .aggregateField("cost", Count.combineFn(), "bid_count")
              .aggregateField("duration", Sum.ofDoubles(), "total_duration"));
      AtomicReference<CombineFn<Row, Object, Row>> found = new AtomicReference<>();
      p.traverseTopologically(
          new PipelineVisitor.Defaults() {
            @Override
            public CompositeBehavior enterCompositeTransform(Node node) {
              if (node.getTransform() instanceof Combine.PerKey) {
                Combine.PerKey<?, ?, ?> perKey = (Combine.PerKey<?, ?, ?>) node.getTransform();
                found.set((CombineFn<Row, Object, Row>) perKey.getFn());
              }
              return CompositeBehavior.ENTER_TRANSFORM;
            }
          });
      if (found.get() == null) {
        throw new IllegalStateException("Could not find SchemaAggregateFn inside Group transform");
      }
      return found.get();
    }
  }

  @Benchmark
  public Row rowSelectorWildcard30(TransformState s) {
    return s.wildcardSelector.select(s.wideRow);
  }

  @Benchmark
  public Row rowSelectorSubset3Of30(TransformState s) {
    return s.flatSubsetSelector.select(s.wideRow);
  }

  @Benchmark
  public Row rowSelectorNestedList16(TransformState s) {
    return s.listProjectSelector.select(s.nestedContainerRow);
  }

  @Benchmark
  public Row rowSelectorNestedMap16(TransformState s) {
    return s.mapProjectSelector.select(s.nestedContainerRow);
  }

  @Benchmark
  public Row schemaAggregateFn16Rows(TransformState s) {
    Object acc = s.flat4AggFn.createAccumulator();
    for (Row r : s.aggInputRows) {
      acc = s.flat4AggFn.addInput(acc, r);
    }
    return s.flat4AggFn.extractOutput(acc);
  }

  /**
   * Benchmarks creating 4 single-row accumulators and merging them via {@code mergeAccumulators}
   * (measures create + 4x addInput + mergeAccumulators to avoid mutating shared state in-place).
   */
  @Benchmark
  public Object schemaAggregateFnMerge4(TransformState s) {
    // Re-create accumulators to avoid mutating shared benchmark state in-place across iterations.
    List<Object> fresh = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      Object acc = s.flat4AggFn.createAccumulator();
      acc = s.flat4AggFn.addInput(acc, s.aggInputRows[i]);
      fresh.add(acc);
    }
    return s.flat4AggFn.mergeAccumulators(fresh);
  }

  @Benchmark
  public Row castRowIdenticalSchema(TransformState s) {
    return Cast.castRow(s.castRow, s.castInputSchema, s.castIdenticalSchema);
  }

  @Benchmark
  public Row castRowWidenedSchema(TransformState s) {
    return Cast.castRow(s.castRow, s.castInputSchema, s.castWidenedSchema);
  }
}
