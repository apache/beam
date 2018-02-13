/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.*;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowOptimizer;
import cz.seznam.euphoria.flink.FlowTranslator;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import cz.seznam.euphoria.flink.batch.io.DataSinkWrapper;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Translate flow for Flink Batch Mode. Only first translation match is used in flow
 */
public class BatchFlowTranslator extends FlowTranslator {

  public interface SplitAssignerFactory
      extends BiFunction<LocatableInputSplit[], Integer, InputSplitAssigner>, Serializable {
  }

  public static final SplitAssignerFactory DEFAULT_SPLIT_ASSIGNER_FACTORY =
      (splits, partitions) -> new LocatableInputSplitAssigner(splits);

  private static class Translation<O extends Operator<?, ?>> {
    final BatchOperatorTranslator<O> translator;
    final UnaryPredicate<O> accept;

    private Translation(
        BatchOperatorTranslator<O> translator, UnaryPredicate<O> accept) {
      this.translator = Objects.requireNonNull(translator);
      this.accept = accept;
    }

    static <O extends Operator<?, ?>> void add(
        Map<Class, List<Translation>> idx,
        Class<O> type, BatchOperatorTranslator<O> translator) {
      add(idx, type, translator, null);
    }

    static <O extends Operator<?, ?>> void add(
        Map<Class, List<Translation>> idx,
        Class<O> type, BatchOperatorTranslator<O> translator, UnaryPredicate<O> accept) {
      idx.putIfAbsent(type, new ArrayList<>());
      idx.get(type).add(new Translation<>(translator, accept));
    }
  }

  private final Map<Class, List<Translation>> translations = new IdentityHashMap<>();

  private final Settings settings;
  private final ExecutionEnvironment env;
  private final FlinkAccumulatorFactory accumulatorFactory;

  public BatchFlowTranslator(Settings settings,
                             ExecutionEnvironment env,
                             FlinkAccumulatorFactory accumulatorFactory) {
    this(settings, env, accumulatorFactory, DEFAULT_SPLIT_ASSIGNER_FACTORY);
  }

  public BatchFlowTranslator(Settings settings,
                             ExecutionEnvironment env,
                             FlinkAccumulatorFactory accumulatorFactory,
                             SplitAssignerFactory splitAssignerFactory) {
    this.settings = Objects.requireNonNull(settings);
    this.env = Objects.requireNonNull(env);
    this.accumulatorFactory = Objects.requireNonNull(accumulatorFactory);

    // basic operators
    Translation.add(translations, FlowUnfolder.InputOperator.class, new InputTranslator(
        splitAssignerFactory));
    Translation.add(translations, FlatMap.class, new FlatMapTranslator());
    Translation.add(translations, ReduceStateByKey.class, new ReduceStateByKeyTranslator());
    Translation.add(translations, Union.class, new UnionTranslator());

    // derived operators
    Translation.add(translations, ReduceByKey.class, new ReduceByKeyTranslator(),
        ReduceByKeyTranslator::wantTranslate);

    // ~ batch broadcast join for a very small left side
    Translation.add(translations, Join.class, new BroadcastHashJoinTranslator(),
        BroadcastHashJoinTranslator::wantTranslate);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Collection<TranslateAcceptor> getAcceptors() {
    return translations.entrySet().stream()
        .flatMap((entry) -> entry.getValue()
            .stream()
            .map(translator -> new TranslateAcceptor(entry.getKey(), translator.accept)))
        .collect(Collectors.toList());
  }

  @Override
  protected FlowOptimizer createOptimizer() {
    FlowOptimizer opt = new FlowOptimizer();
    opt.setMaxParallelism(env.getParallelism());
    return opt;
  }

  /**
   * Take only first translation operator
   * @param flow the user defined flow to be translated
   *
   * @return all output sinks
   */
  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<Operator<?, ?>>> dag = flowToDag(flow);

    BatchExecutorContext executorContext = new BatchExecutorContext(env, (DAG) dag,
        accumulatorFactory, settings);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      List<Translation> txs = this.translations.get(originalOp.getClass());
      if (txs.isEmpty()) {
        throw new UnsupportedOperationException(
            "Operator " + op.getClass().getSimpleName() + " not supported");
      }
      // ~ verify the flowToDag translation
      Translation<Operator<?, ?>> firstMatch = null;
      for (Translation<Operator<?, ?>> tx : txs) {
        if (tx.accept == null || tx.accept.apply(originalOp)) {
          firstMatch = tx;
          break;
        }
      }
      final DataSet<?> out;
      if (firstMatch != null) {
        out = firstMatch.translator.translate(op, executorContext);
        // save output of current operator to context
        executorContext.setOutput(op, out);
      } else {
        throw new IllegalStateException("No matching translation.");
      }
    });

    // process all sinks in the DAG (leaf nodes)
    final List<DataSink<?>> sinks = new ArrayList<>();
    dag.getLeafs()
        .stream()
        .map(Node::get)
        .filter(op -> op.output().getOutputSink() != null)
        .forEach(op -> {

          final DataSink<?> sink = op.output().getOutputSink();
          sinks.add(sink);
          DataSet<?> flinkOutput =
              Objects.requireNonNull(executorContext.getOutputStream(op));

          flinkOutput.output(new DataSinkWrapper<>((DataSink) sink));
        });

    return sinks;
  }
}
