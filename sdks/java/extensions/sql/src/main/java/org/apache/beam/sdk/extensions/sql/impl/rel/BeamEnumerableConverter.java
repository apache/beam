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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.joda.time.Duration;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BeamRelNode to replace a {@code Enumerable} node. */
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
    final PipelineOptions options = createPipelineOptions(node.getPipelineOptions());
    return toEnumerable(options, node);
  }

  public static PipelineOptions createPipelineOptions(Map<String, String> map) {
    final String[] args = new String[map.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      args[i++] = "--" + entry.getKey() + "=" + entry.getValue();
    }
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    options.as(ApplicationNameOptions.class).setAppName("BeamSql");
    return options;
  }

  public static Enumerable<Object> toEnumerable(PipelineOptions options, BeamRelNode node) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      if (node instanceof BeamIOSinkRel) {
        return count(options, node);
      } else if (isLimitQuery(node)) {
        return limitCollect(options, node);
      }

      return collect(options, node);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private static PipelineResult limitRun(
      PipelineOptions options,
      BeamRelNode node,
      DoFn<Row, Void> doFn,
      Queue<Object> values,
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

  private static Enumerable<Object> collect(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    Queue<Object> values = new ConcurrentLinkedQueue<Object>();

    checkArgument(
        options
            .getRunner()
            .getCanonicalName()
            .equals("org.apache.beam.runners.direct.DirectRunner"),
        "SELECT without INSERT is only supported in DirectRunner in SQL Shell.");

    Collector.globalValues.put(id, values);

    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> resultCollection = BeamSqlRelUtils.toPCollection(pipeline, node);
    resultCollection.apply(ParDo.of(new Collector()));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    Collector.globalValues.remove(id);

    return Linq4j.asEnumerable(values);
  }

  private static Enumerable<Object> limitCollect(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    Queue<Object> values = new ConcurrentLinkedQueue<Object>();

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

    return Linq4j.asEnumerable(values);
  }

  private static class Collector extends DoFn<Row, Void> {

    // This will only work on the direct runner.
    private static final Map<Long, Queue<Object>> globalValues =
        new ConcurrentHashMap<Long, Queue<Object>>();

    @Nullable private volatile Queue<Object> values;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      long id = context.getPipelineOptions().getOptionsId();
      values = globalValues.get(id);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Object[] avaticaRow = rowToAvatica(context.element());
      if (avaticaRow.length == 1) {
        values.add(avaticaRow[0]);
      } else {
        values.add(avaticaRow);
      }
    }
  }

  private static Object[] rowToAvatica(Row row) {
    Schema schema = row.getSchema();
    Object[] convertedColumns = new Object[schema.getFields().size()];
    int i = 0;
    for (Schema.Field field : schema.getFields()) {
      convertedColumns[i] = fieldToAvatica(field.getType(), row.getValue(i));
      ++i;
    }
    return convertedColumns;
  }

  private static Object fieldToAvatica(Schema.FieldType type, Object beamValue) {
    switch (type.getTypeName()) {
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
        return beamValue;
      case ARRAY:
        return ((List<?>) beamValue)
            .stream()
            .map(elem -> fieldToAvatica(type.getCollectionElementType(), elem))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<?, ?>) beamValue)
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> fieldToAvatica(type.getCollectionElementType(), entry.getValue())));
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
      result.waitUntilFinish();
      MetricQueryResults metrics =
          result
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named(BeamEnumerableConverter.class, "rows"))
                      .build());
      count = metrics.getCounters().iterator().next().getAttempted();
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
        || (node instanceof BeamCalcRel && ((BeamCalcRel) node).isInputSortRelAndLimitOnly());
  }

  private static int getLimitCount(BeamRelNode node) {
    if (node instanceof BeamSortRel) {
      return ((BeamSortRel) node).getCount();
    } else if (node instanceof BeamCalcRel) {
      return ((BeamCalcRel) node).getLimitCountOfSortRel();
    }

    throw new RuntimeException(
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
