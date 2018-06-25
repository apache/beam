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
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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

  enum LimitState {
    REACHED,
    NOT_REACHED
  }

  private static class LimitStateVar implements Serializable {
    private LimitState state;

    public LimitStateVar() {
      state = LimitState.NOT_REACHED;
    }

    public void setReached() {
      state = LimitState.REACHED;
    }

    public boolean isReached() {
      return state == LimitState.REACHED;
    }
  }

  private static PipelineResult run(
      PipelineOptions options, BeamRelNode node, DoFn<Row, Void> doFn) {
    Pipeline pipeline = Pipeline.create(options);
    BeamSqlRelUtils.toPCollection(pipeline, node).apply(ParDo.of(doFn));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    return result;
  }

  private static PipelineResult limitRun(
      PipelineOptions options,
      BeamRelNode node,
      DoFn<Row, KV<String, Row>> collectDoFn,
      DoFn<KV<String, Row>, Void> limitCounterDoFn,
      LimitStateVar limitStateVar) {
    options.as(DirectOptions.class).setBlockOnRun(false);
    Pipeline pipeline = Pipeline.create(options);
    BeamSqlRelUtils.toPCollection(pipeline, node)
        .apply(ParDo.of(collectDoFn))
        .apply(ParDo.of(limitCounterDoFn));

    PipelineResult result = pipeline.run();

    State state;
    while (true) {
      // Check pipeline state in every second
      state = result.waitUntilFinish(Duration.standardSeconds(1));
      if (state != null && state.isTerminal()) {
        break;
      }
      // If state is null, or state is not null and indicates pipeline is not terminated
      // yet, check continue checking the limit var.
      try {
        if (limitStateVar.isReached()) {
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
    run(options, node, new Collector());
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

    LimitStateVar limitStateVar = new LimitStateVar();
    int limitCount = getLimitCount(node);

    LimitCanceller.globalLimitArguments.put(id, limitCount);
    LimitCanceller.globalStates.put(id, limitStateVar);
    LimitCollector.globalValues.put(id, values);
    limitRun(options, node, new LimitCollector(), new LimitCanceller(), limitStateVar);
    LimitCanceller.globalLimitArguments.remove(id);
    LimitCanceller.globalStates.remove(id);
    LimitCollector.globalValues.remove(id);

    return Linq4j.asEnumerable(values);
  }

  private static class LimitCanceller extends DoFn<KV<String, Row>, Void> {
    private static final Map<Long, Integer> globalLimitArguments =
        new ConcurrentHashMap<Long, Integer>();
    private static final Map<Long, LimitStateVar> globalStates =
        new ConcurrentHashMap<Long, LimitStateVar>();

    @Nullable private volatile Integer count;
    @Nullable private volatile LimitStateVar limitStateVar;

    @StateId("counter")
    private final StateSpec<ValueState<Integer>> counter = StateSpecs.value(VarIntCoder.of());

    @StartBundle
    public void startBundle(StartBundleContext context) {
      long id = context.getPipelineOptions().getOptionsId();
      count = globalLimitArguments.get(id);
      limitStateVar = globalStates.get(id);
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId("counter") ValueState<Integer> counter) {
      int current = (counter.read() != null ? counter.read() : 0);
      current += 1;
      if (current >= count && !limitStateVar.isReached()) {
        // if current count reaches the limit count but limitStateVar has not been set, flip
        // the var.
        limitStateVar.setReached();
      }

      counter.write(current);
    }
  }

  private static class LimitCollector extends DoFn<Row, KV<String, Row>> {
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
      Object[] input = context.element().getValues().toArray();
      if (input.length == 1) {
        values.add(input[0]);
      } else {
        values.add(input);
      }
      context.output(KV.of("DummyKey", context.element()));
    }
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
      Object[] input = context.element().getValues().toArray();
      if (input.length == 1) {
        values.add(input[0]);
      } else {
        values.add(input);
      }
    }
  }

  private static Enumerable<Object> count(PipelineOptions options, BeamRelNode node) {
    PipelineResult result = run(options, node, new RowCounter());
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(BeamEnumerableConverter.class, "rows"))
                    .build());
    long count = metrics.getCounters().iterator().next().getAttempted();
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
}
