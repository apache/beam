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
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlinkElement;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindowing;
import cz.seznam.euphoria.flink.streaming.windowing.KeyedMultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.KeyedMultiWindowedElementWindowOperator;
import cz.seznam.euphoria.flink.streaming.windowing.WindowedElementWindowOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

class ReduceStateByKeyTranslator implements StreamingOperatorTranslator<ReduceStateByKey> {

  static String CFG_VALUE_OF_AFTER_SHUFFLE_KEY = "euphoria.flink.streaming.windowing.only.after.shuffle";
  static boolean CFG_VALUE_OF_AFTER_SHUFFLE_DEFAULT = false;

  private boolean valueOfAfterShuffle = true;

  public ReduceStateByKeyTranslator(Settings settings) {
    this.valueOfAfterShuffle =
            settings.getBoolean(CFG_VALUE_OF_AFTER_SHUFFLE_KEY, CFG_VALUE_OF_AFTER_SHUFFLE_DEFAULT);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceStateByKey> operator,
                                 StreamingExecutorContext context)
  {
    DataStream input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    StateFactory<?, State> stateFactory = origOperator.getStateFactory();
    CombinableReduceFunction stateCombiner = origOperator.getStateCombiner();

    Windowing windowing = origOperator.getWindowing();
    if (windowing == null) {
      // use attached windowing when no windowing explicitly defined
      windowing = new AttachedWindowing<>();
    }

    final UnaryFunction keyExtractor = origOperator.getKeyExtractor();
    final UnaryFunction valueExtractor = origOperator.getValueExtractor();
    final UnaryFunction eventTimeAssigner = origOperator.getEventTimeAssigner();

    if (eventTimeAssigner != null) {
      input = input.assignTimestampsAndWatermarks(
              new EventTimeAssigner(context.getAllowedLateness(), eventTimeAssigner));
    }

    DataStream<FlinkElement<?, Pair>> reduced;
    WindowAssigner elMapper =
            new WindowAssigner(windowing, keyExtractor, valueExtractor, eventTimeAssigner);
    if (valueOfAfterShuffle) {
      reduced = input.keyBy(new UnaryFunctionKeyExtractor(keyExtractor))
                     .transform(operator.getName(), TypeInformation.of(FlinkElement.class),
                                new WindowedElementWindowOperator(elMapper, windowing, stateFactory, stateCombiner, context.isLocalMode()))
                     .setParallelism(operator.getParallelism());
    } else {
      // assign windows
      DataStream<KeyedMultiWindowedElement> windowed = input.map(elMapper)
              // ~ execute in the same chain of the input's processing
              // so far, thereby, avoiding an unnecessary shuffle
              .setParallelism(input.getParallelism());
      reduced = (DataStream) windowed.keyBy(new KeyedMultiWindowedElementKeyExtractor())
              .transform(operator.getName(), TypeInformation.of(FlinkElement.class),
                      new KeyedMultiWindowedElementWindowOperator<>(windowing, stateFactory, stateCombiner, context.isLocalMode()))
              .setParallelism(operator.getParallelism());
    }

    // FIXME partitioner should be applied during "keyBy" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default
    if (!origOperator.getPartitioning().hasDefaultPartitioner()) {
      reduced = reduced.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.getElement().getKey());
    }

    return reduced;
  }

  private static class EventTimeAssigner
          extends BoundedOutOfOrdernessTimestampExtractor<FlinkElement>
  {
    private final UnaryFunction<Object, Long> eventTimeFn;

    EventTimeAssigner(Duration allowedLateness, UnaryFunction<Object, Long> eventTimeFn) {
      super(millisTime(allowedLateness.toMillis()));
      this.eventTimeFn = Objects.requireNonNull(eventTimeFn);
    }

    @Override
    public long extractTimestamp(FlinkElement element) {
      return eventTimeFn.apply(element.getElement());
    }

    private static org.apache.flink.streaming.api.windowing.time.Time
    millisTime(long millis) {
      return org.apache.flink.streaming.api.windowing.time.Time.milliseconds(millis);
    }
  }

  private static class WindowAssigner implements MapFunction<FlinkElement, KeyedMultiWindowedElement>,
          ResultTypeQueryable<KeyedMultiWindowedElement> {

    private final Windowing windowing;
    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final UnaryFunction eventTimeAssigner;

    public WindowAssigner(Windowing windowing,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          UnaryFunction eventTimeAssigner) {
      this.windowing = windowing;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyedMultiWindowedElement map(FlinkElement el) throws Exception {
      if (eventTimeAssigner != null) {
        el.setTimestamp((long) eventTimeAssigner.apply(el.getElement()));
      }
      Set windows = windowing.assignWindowsToElement(el);

      return new KeyedMultiWindowedElement<>(
              keyExtractor.apply(el.getElement()),
              valueExtractor.apply(el.getElement()),
              el.getTimestamp(),
              windows);
    }

    @Override
    public TypeInformation<KeyedMultiWindowedElement> getProducedType() {
      return TypeInformation.of(KeyedMultiWindowedElement.class);
    }
  }

  private static class UnaryFunctionKeyExtractor
          implements KeySelector<FlinkElement, Object>,
                     ResultTypeQueryable<Object> {
    private final UnaryFunction keyExtractor;

    public UnaryFunctionKeyExtractor(UnaryFunction keyExtractor) {
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getKey(FlinkElement value) throws Exception {
      return keyExtractor.apply(value.getElement());
    }

    @Override
    public TypeInformation<Object> getProducedType() {
      return TypeInformation.of(Object.class);
    }
  }

  private static class KeyedMultiWindowedElementKeyExtractor
          implements KeySelector<KeyedMultiWindowedElement, Object>,
                     ResultTypeQueryable<Object> {

    @Override
    public Object getKey(KeyedMultiWindowedElement el) throws Exception {
      return el.getKey();
    }

    @Override
    public TypeInformation<Object> getProducedType() {
      return TypeInformation.of(Object.class);
    }
  }
}
