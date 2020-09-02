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

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.CharType;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.ConventionTraitDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BeamRelNode to replace a {@code Enumerable} node. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamEnumerableConverter extends ConverterImpl implements EnumerableRel {
  private static final Logger LOG = LoggerFactory.getLogger(BeamEnumerableConverter.class);

  public BeamEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // This should always be a last resort.
    return planner.getCostFactory().makeHugeCost();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
    final BlockBuilder list = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, prefer.preferArray());
    final Expression node = implementor.stash((BeamRelNode) getInput(), BeamRelNode.class);
    list.add(Expressions.call(BeamEnumerableConverter.class, "toEnumerable", node));
    return implementor.result(physType, list.toBlock());
  }

  public static Enumerable<Object> toEnumerable(BeamRelNode node) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      final PipelineOptions options = createPipelineOptions(node.getPipelineOptions());
      return toEnumerable(options, node);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public static List<Row> toRowList(BeamRelNode node) {
    return toRowList(node, Collections.emptyMap());
  }

  public static List<Row> toRowList(BeamRelNode node, Map<String, String> otherOptions) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      final Map<String, String> optionsMap = new HashMap<>();
      optionsMap.putAll(node.getPipelineOptions());
      optionsMap.putAll(otherOptions);
      return toRowList(createPipelineOptions(optionsMap), node);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public static PipelineOptions createPipelineOptions(Map<String, String> map) {
    final String[] args = new String[map.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      args[i++] = "--" + entry.getKey() + "=" + entry.getValue();
    }
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    FileSystems.setDefaultPipelineOptions(options);
    options.as(ApplicationNameOptions.class).setAppName("BeamSql");
    return options;
  }

  static List<Row> toRowList(PipelineOptions options, BeamRelNode node) {
    if (node instanceof BeamIOSinkRel) {
      throw new UnsupportedOperationException("Does not support BeamIOSinkRel in toRowList.");
    } else if (isLimitQuery(node)) {
      throw new UnsupportedOperationException("Does not support queries with LIMIT in toRowList.");
    }
    return collectRows(options, node).stream().collect(Collectors.toList());
  }

  static Enumerable<Object> toEnumerable(PipelineOptions options, BeamRelNode node) {
    if (node instanceof BeamIOSinkRel) {
      return count(options, node);
    } else if (isLimitQuery(node)) {
      return limitCollect(options, node);
    }
    return Linq4j.asEnumerable(rowToAvaticaAndUnboxValues((collectRows(options, node))));
  }

  private static PipelineResult limitRun(
      PipelineOptions options,
      BeamRelNode node,
      DoFn<Row, Void> doFn,
      Queue<Row> values,
      int limitCount) {
    options.as(DirectOptions.class).setBlockOnRun(false);
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> resultCollection = BeamSqlRelUtils.toPCollection(pipeline, node);
    resultCollection.apply(ParDo.of(doFn));

    PipelineResult result = pipeline.run();

    State state;
    while (true) {
      // Check pipeline state in every second
      state = result.waitUntilFinish(Duration.standardSeconds(1));
      if (state != null && state.isTerminal()) {
        if (PipelineResult.State.FAILED.equals(state)) {
          throw new RuntimeException("Pipeline failed for unknown reason");
        }
        break;
      }

      try {
        if (values.size() >= limitCount) {
          result.cancel();
          break;
        }
      } catch (IOException e) {
        LOG.warn(e.toString());
        break;
      }
    }

    return result;
  }

  private static void runCollector(PipelineOptions options, BeamRelNode node) {
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> resultCollection = BeamSqlRelUtils.toPCollection(pipeline, node);
    resultCollection.apply(ParDo.of(new Collector()));
    PipelineResult result = pipeline.run();
    if (PipelineResult.State.FAILED.equals(result.waitUntilFinish())) {
      throw new RuntimeException("Pipeline failed for unknown reason");
    }
  }

  private static Queue<Row> collectRows(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    Queue<Row> values = new ConcurrentLinkedQueue<>();

    checkArgument(
        options
            .getRunner()
            .getCanonicalName()
            .equals("org.apache.beam.runners.direct.DirectRunner"),
        "collectRowList is only available in direct runner.");

    Collector.globalValues.put(id, values);

    runCollector(options, node);

    Collector.globalValues.remove(id);
    return values;
  }

  private static Enumerable<Object> limitCollect(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    ConcurrentLinkedQueue<Row> values = new ConcurrentLinkedQueue<>();

    checkArgument(
        options
            .getRunner()
            .getCanonicalName()
            .equals("org.apache.beam.runners.direct.DirectRunner"),
        "SELECT without INSERT is only supported in DirectRunner in SQL Shell.");

    int limitCount = getLimitCount(node);

    Collector.globalValues.put(id, values);
    limitRun(options, node, new Collector(), values, limitCount);
    Collector.globalValues.remove(id);

    // remove extra retrieved values
    while (values.size() > limitCount) {
      values.remove();
    }

    return Linq4j.asEnumerable(rowToAvaticaAndUnboxValues(values));
  }

  private static class Collector extends DoFn<Row, Void> {

    // This will only work on the direct runner.
    private static final Map<Long, Queue<Row>> globalValues = new ConcurrentHashMap<>();

    private volatile @Nullable Queue<Row> values;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      long id = context.getPipelineOptions().getOptionsId();
      values = globalValues.get(id);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      values.add(context.element());
    }
  }

  private static List<Object> rowToAvaticaAndUnboxValues(Queue<Row> values) {
    return values.stream()
        .map(
            row -> {
              Object[] objects = rowToAvatica(row);
              if (objects.length == 1) {
                // if objects.length == 1, that means input Row contains only 1 column/element,
                // then an Object instead of Object[] should be returned because of
                // CalciteResultSet's behaviour that tries to convert one column row to an Object.
                return objects[0];
              } else {
                return objects;
              }
            })
        .collect(Collectors.toList());
  }

  private static Object[] rowToAvatica(Row row) {
    Schema schema = row.getSchema();
    Object[] convertedColumns = new Object[schema.getFields().size()];
    int i = 0;
    for (Schema.Field field : schema.getFields()) {
      convertedColumns[i] = fieldToAvatica(field.getType(), row.getBaseValue(i, Object.class));
      ++i;
    }
    return convertedColumns;
  }

  private static Object fieldToAvatica(Schema.FieldType type, Object beamValue) {
    if (beamValue == null) {
      return null;
    }

    switch (type.getTypeName()) {
      case LOGICAL_TYPE:
        String logicalId = type.getLogicalType().getIdentifier();
        if (SqlTypes.TIME.getIdentifier().equals(logicalId)) {
          if (beamValue instanceof Long) { // base type
            return (Long) beamValue;
          } else { // input type
            return ((LocalTime) beamValue).toNanoOfDay();
          }
        } else if (SqlTypes.DATE.getIdentifier().equals(logicalId)) {
          if (beamValue instanceof Long) { // base type
            return ((Long) beamValue).intValue();
          } else { // input type
            return (int) (((LocalDate) beamValue).toEpochDay());
          }
        } else if (CharType.IDENTIFIER.equals(logicalId)) {
          return beamValue;
        } else {
          throw new UnsupportedOperationException("Unknown DateTime type " + logicalId);
        }
      case DATETIME:
        return ((ReadableInstant) beamValue).getMillis();
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
      case BYTES:
        return beamValue;
      case ARRAY:
        return ((List<?>) beamValue)
            .stream()
                .map(elem -> fieldToAvatica(type.getCollectionElementType(), elem))
                .collect(Collectors.toList());
      case ITERABLE:
        return StreamSupport.stream(((Iterable<?>) beamValue).spliterator(), false)
            .map(elem -> fieldToAvatica(type.getCollectionElementType(), elem))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<?, ?>) beamValue)
            .entrySet().stream()
                .collect(
                    Collectors.toMap(
                        entry -> entry.getKey(),
                        entry ->
                            fieldToAvatica(type.getCollectionElementType(), entry.getValue())));
      case ROW:
        // TODO: needs to be a Struct
        return beamValue;
      default:
        throw new IllegalStateException(
            String.format("Unreachable case for Beam typename %s", type.getTypeName()));
    }
  }

  private static Enumerable<Object> count(PipelineOptions options, BeamRelNode node) {
    Pipeline pipeline = Pipeline.create(options);
    BeamSqlRelUtils.toPCollection(pipeline, node).apply(ParDo.of(new RowCounter()));
    PipelineResult result = pipeline.run();

    long count = 0;
    if (!containsUnboundedPCollection(pipeline)) {
      if (PipelineResult.State.FAILED.equals(result.waitUntilFinish())) {
        throw new RuntimeException("Pipeline failed for unknown reason");
      }
      MetricQueryResults metrics =
          result
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named(BeamEnumerableConverter.class, "rows"))
                      .build());
      Iterator<MetricResult<Long>> iterator = metrics.getCounters().iterator();
      if (iterator.hasNext()) {
        count = iterator.next().getAttempted();
      }
    }
    return Linq4j.singletonEnumerable(count);
  }

  private static class RowCounter extends DoFn<Row, Void> {
    final Counter rows = Metrics.counter(BeamEnumerableConverter.class, "rows");

    @ProcessElement
    public void processElement(ProcessContext context) {
      rows.inc();
    }
  }

  private static boolean isLimitQuery(BeamRelNode node) {
    return (node instanceof BeamSortRel && ((BeamSortRel) node).isLimitOnly())
        || (node instanceof AbstractBeamCalcRel
            && ((AbstractBeamCalcRel) node).isInputSortRelAndLimitOnly());
  }

  private static int getLimitCount(BeamRelNode node) {
    if (node instanceof BeamSortRel) {
      return ((BeamSortRel) node).getCount();
    } else if (node instanceof AbstractBeamCalcRel) {
      return ((AbstractBeamCalcRel) node).getLimitCountOfSortRel();
    }

    throw new IllegalArgumentException(
        "Cannot get limit count from RelNode tree with root " + node.getRelTypeName());
  }

  private static boolean containsUnboundedPCollection(Pipeline p) {
    class BoundednessVisitor extends PipelineVisitor.Defaults {
      IsBounded boundedness = IsBounded.BOUNDED;

      @Override
      public void visitValue(PValue value, Node producer) {
        if (value instanceof PCollection) {
          boundedness = boundedness.and(((PCollection) value).isBounded());
        }
      }
    }

    BoundednessVisitor visitor = new BoundednessVisitor();
    p.traverseTopologically(visitor);
    return visitor.boundedness == IsBounded.UNBOUNDED;
  }
}
