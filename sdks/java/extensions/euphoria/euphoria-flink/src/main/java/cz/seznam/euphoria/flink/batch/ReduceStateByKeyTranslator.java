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

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.greduce.GroupReducer;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.HashMap;
import java.util.Map;

public class ReduceStateByKeyTranslator implements BatchOperatorTranslator<ReduceStateByKey> {

  private StateContext stateContext;

  private void loadConfig(Settings settings, ExecutionEnvironment env) {
    int maxMemoryElements = settings.getInt(
        CFG_LIST_STORAGE_MAX_MEMORY_ELEMS_KEY,
        CFG_LIST_STORAGE_MAX_MEMORY_ELEMS_DEFAULT);
    this.stateContext = new BatchStateContext(settings, env, maxMemoryElements);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceStateByKey> operator,
                           BatchExecutorContext context) {
    loadConfig(context.getSettings(), context.getExecutionEnvironment());

    int inputParallelism = Iterables.getOnlyElement(context.getInputOperators(operator)).getParallelism();
    DataSet input = Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    final Windowing windowing =
        origOperator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : origOperator.getWindowing();

    final UnaryFunction udfKey = origOperator.getKeyExtractor();
    final UnaryFunction udfValue = origOperator.getValueExtractor();

    DataSet<BatchElement> wAssigned =
            input.flatMap((i, c) -> {
              BatchElement wel = (BatchElement) i;
              Iterable<Window> assigned = windowing.assignWindowsToElement(wel);
              for (Window wid : assigned) {
                Object el = wel.getElement();
                c.collect(new BatchElement<>(
                        wid,
                        wel.getTimestamp(),
                        Pair.of(udfKey.apply(el), udfValue.apply(el))));
              }
            })
            .returns(BatchElement.class)
            .name(operator.getName() + "::map-input")
            .setParallelism(inputParallelism);

    // ~ reduce the data now
    DataSet<BatchElement<?, Pair>> reduced =
        wAssigned.groupBy((KeySelector)
            Utils.wrapQueryable(
                // ~ FIXME if the underlying windowing is "non merging" we can group by
                // "key _and_ window", thus, better utilizing the available resources

                // ~ Grouping by key hashcode will effectively group elements with the same key
                // to the one bucket. Although there may occur some collisions that we need
                // to be aware of in later processing.
                (BatchElement<?, Pair> we) -> we.getElement().getFirst().hashCode(),
                Integer.class))
            .sortGroup(Utils.wrapQueryable(
                (KeySelector<BatchElement<?, ?>, Long>)
                        BatchElement::getTimestamp, Long.class),
                Order.ASCENDING)
            .reduceGroup(new RSBKReducer(origOperator, stateContext, windowing,
                    context.getAccumulatorFactory(), context.getSettings()))
            .setParallelism(operator.getParallelism())
            .name(operator.getName() + "::reduce");

    return reduced;
  }

  static class RSBKReducer
          extends RichGroupReduceFunction<BatchElement<?, Pair>, BatchElement<?, Pair>>
          implements  ResultTypeQueryable<BatchElement<?, Pair>> {

    private final StateFactory<?, ?, State<?, ?>> stateFactory;
    private final StateMerger<?, ?, State<?, ?>> stateCombiner;
    private final StateContext stateContext;
    private final Windowing windowing;
    private final Trigger trigger;
    private final FlinkAccumulatorFactory accumulatorFactory;
    private final Settings settings;

    // mapping of [Key -> GroupReducer]
    private transient Map<Object, GroupReducer> activeReducers;

    @SuppressWarnings("unchecked")
    RSBKReducer(
        ReduceStateByKey operator,
        StateContext stateContext,
        Windowing windowing,
        FlinkAccumulatorFactory accumulatorFactory,
        Settings settings) {

      this.stateFactory = operator.getStateFactory();
      this.stateCombiner = operator.getStateMerger();
      this.stateContext = stateContext;
      this.windowing = windowing;
      this.trigger = windowing.getTrigger();
      this.accumulatorFactory = accumulatorFactory;
      this.settings = settings;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reduce(Iterable<BatchElement<?, Pair>> values,
                       org.apache.flink.util.Collector<BatchElement<?, Pair>> out)
    {
      activeReducers = new HashMap<>();
      for (BatchElement<?, Pair> batchElement : values) {
        Object key = batchElement.getElement().getFirst();

        GroupReducer reducer = activeReducers.get(key);
        if (reducer == null) {
          reducer = new GroupReducer(
                  stateFactory,
                  stateCombiner,
                  stateContext,
                  BatchElement::new,
                  windowing,
                  trigger,
                  elem -> out.collect((BatchElement) elem),
                  accumulatorFactory.create(settings, getRuntimeContext()));
          activeReducers.put(key, reducer);
        }

        reducer.process(batchElement);
      }

      flushStates();
    }

    private void flushStates() {
      for (Map.Entry<Object, GroupReducer> e : activeReducers.entrySet()) {
        GroupReducer reducer = e.getValue();
        reducer.close();
      }
      activeReducers.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<BatchElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) BatchElement.class);
    }
  }
}
