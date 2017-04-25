/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Sort;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlowOptimizer;
import cz.seznam.euphoria.flink.FlowTranslator;
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

public class BatchFlowTranslator extends FlowTranslator {
  
  public interface SplitAssignerFactory
  extends BiFunction<LocatableInputSplit[], Integer, InputSplitAssigner>, Serializable {}
  
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

    static <O extends Operator<?, ?>> void set(
        Map<Class, Translation> idx,
        Class<O> type, BatchOperatorTranslator<O> translator)
    {
      set(idx, type, translator, null);
    }

    static <O extends Operator<?, ?>> void set(
        Map<Class, Translation> idx,
        Class<O> type, BatchOperatorTranslator<O> translator, UnaryPredicate<O> accept)
    {
      idx.put(type, new Translation<>(translator, accept));
    }
  }

  private final Map<Class, Translation> translations = new IdentityHashMap<>();
  private final ExecutionEnvironment env;

  public BatchFlowTranslator(Settings settings, ExecutionEnvironment env) {
    this(settings, env, DEFAULT_SPLIT_ASSIGNER_FACTORY);
  }

  public BatchFlowTranslator(Settings settings, ExecutionEnvironment env, 
                             SplitAssignerFactory splitAssignerFactory) {
    this.env = Objects.requireNonNull(env);

    // basic operators
    Translation.set(translations, FlowUnfolder.InputOperator.class, new InputTranslator(splitAssignerFactory));
    Translation.set(translations, FlatMap.class, new FlatMapTranslator());
    Translation.set(translations, Repartition.class, new RepartitionTranslator());
    Translation.set(translations, ReduceStateByKey.class, new ReduceStateByKeyTranslator(settings, env));
    Translation.set(translations, Union.class, new UnionTranslator());

    // derived operators
    Translation.set(translations, ReduceByKey.class, new ReduceByKeyTranslator(),
        ReduceByKeyTranslator::wantTranslate);
    Translation.set(translations, Sort.class, new SortTranslator(),
        SortTranslator::wantTranslate);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Collection<TranslateAcceptor> getAcceptors() {
    return translations.entrySet().stream()
        .map(e -> new TranslateAcceptor(e.getKey(), e.getValue().accept))
        .collect(Collectors.toList());
  }

  @Override
  protected FlowOptimizer createOptimizer() {
    FlowOptimizer opt = new FlowOptimizer();
    opt.setMaxParallelism(env.getParallelism());
    return opt;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<DataSink<?>> translateInto(Flow flow) {
    // transform flow to acyclic graph of supported operators
    DAG<FlinkOperator<Operator<?, ?>>> dag = flowToDag(flow);

    BatchExecutorContext executorContext = new BatchExecutorContext(env, (DAG) dag);

    // translate each operator to proper Flink transformation
    dag.traverse().map(Node::get).forEach(op -> {
      Operator<?, ?> originalOp = op.getOriginalOperator();
      Translation<Operator<?, ?>> tx = translations.get(originalOp.getClass());
      if (tx == null) {
        throw new UnsupportedOperationException(
                "Operator " + op.getClass().getSimpleName() + " not supported");
      }
      // ~ verify the flowToDag translation
      Preconditions.checkState(
          tx.accept == null || Boolean.TRUE.equals(tx.accept.apply(originalOp)));

      DataSet<?> out = tx.translator.translate(op, executorContext);

      // save output of current operator to context
      executorContext.setOutput(op, out);
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
