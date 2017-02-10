/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.WindowProperties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Iterator;

class ReduceStateByKeyTranslator implements StreamingOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceStateByKey> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    StateFactory<?, State> stateFactory = origOperator.getStateFactory();

    final Windowing windowing = origOperator.getWindowing();
    final UnaryFunction keyExtractor = origOperator.getKeyExtractor();
    final UnaryFunction valueExtractor = origOperator.getValueExtractor();
    final UnaryFunction eventTimeAssigner = origOperator.getEventTimeAssigner();

    FlinkStreamingStateStorageProvider storageProvider
        = new FlinkStreamingStateStorageProvider();

    DataStream<StreamingWindowedElement<?, Pair>> folded;
    // apply windowing first
    if (windowing == null) {
      WindowedStream windowed =
          context.attachedWindowStream((DataStream) input, keyExtractor, valueExtractor);
      // equivalent operation to "left fold"
      folded = windowed.apply(new RSBKWindowFunction(storageProvider, stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    } else {
      WindowedStream<MultiWindowedElement<?, Pair>, Object, FlinkWindow>
          windowed = context.flinkWindow(
              (DataStream) input, keyExtractor, valueExtractor, windowing, eventTimeAssigner);
      // equivalent operation to "left fold"
      folded = windowed.apply(new RSBKWindowFunction(storageProvider, stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    }

    // FIXME partitioner should be applied during "keyBy" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default
    if (!origOperator.getPartitioning().hasDefaultPartitioner()) {
      folded = folded.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.getElement().getKey());
    }

    return folded;
  }

  private static class RSBKWindowFunction<
          WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window,
          KEY, VALUEIN, VALUEOUT,
          W extends Window & WindowProperties<WID>>
          extends RichWindowFunction<ElementProvider<? extends Pair<KEY, VALUEIN>>,
          StreamingWindowedElement<WID, Pair<KEY, VALUEOUT>>,
          KEY, W> {

    private final StateFactory<?, State> stateFactory;
    private final FlinkStreamingStateStorageProvider storageProvider;

    RSBKWindowFunction(
        FlinkStreamingStateStorageProvider storageProvider,
        StateFactory<?, State> stateFactory) {

      this.stateFactory = stateFactory;
      this.storageProvider = storageProvider;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      storageProvider.initialize(getRuntimeContext());
    }

    @Override
    public void apply(
        KEY key,
        W window,
        Iterable<ElementProvider<? extends Pair<KEY, VALUEIN>>> input,
        Collector<StreamingWindowedElement<WID, Pair<KEY, VALUEOUT>>> out)
        throws Exception {

      Iterator<ElementProvider<? extends Pair<KEY, VALUEIN>>> it = input.iterator();
      // read the first element to obtain window metadata and key
      ElementProvider<? extends Pair<KEY, VALUEIN>> element = it.next();
      WID wid = window.getWindowID();
      long emissionWatermark = window.getEmissionWatermark();

      @SuppressWarnings("unchecked")
      State<VALUEIN, VALUEOUT> state = stateFactory.apply(
          new Context() {
            @Override
            public void collect(Object elem) {
              out.collect(new StreamingWindowedElement(wid, emissionWatermark, Pair.of(key, elem)));
            }
            @Override
            public Object getWindow() {
              return wid;
            }
          },
          storageProvider);

      // add the first element to the state
      state.add(element.getElement().getValue());

      while (it.hasNext()) {
        state.add(it.next().getElement().getValue());
      }
      state.flush();
      state.close();
    }
  }
}
