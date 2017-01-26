/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

import java.time.Duration;
import java.util.Objects;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
public class StreamWindower {

  private final Duration allowedLateness;

  public StreamWindower(Duration allowedLateness) {
    this.allowedLateness = Objects.requireNonNull(allowedLateness);
  }

  @SuppressWarnings("unchecked")
  public <WID extends Window, T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<WID, Pair<KEY, VALUE>>,
                 KEY,
                 AttachedWindow<WID>>
  attachedWindow(DataStream<StreamingWindowedElement<WID, T>> input,
                      UnaryFunction<T, KEY> keyFn,
                      UnaryFunction<T, VALUE> valFn)
  {
    DataStream<StreamingWindowedElement<WID, Pair<KEY, VALUE>>> mapped
        = input.map(i -> {
          T elem = i.getElement();
          KEY key = keyFn.apply(elem);
          VALUE val = valFn.apply(elem);
          WID wid = i.getWindow();
          return new StreamingWindowedElement<>(
                  wid,
                  // ~ forward the emission watermark
                  i.getTimestamp(),
                  Pair.of(key, val));
        })
        .setParallelism(input.getParallelism())
        .returns((Class) StreamingWindowedElement.class);
    final KeyedStream<StreamingWindowedElement<WID, Pair<KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed.window(new AttachedWindowAssigner<>());
  }

  @SuppressWarnings("unchecked")
  public <T, WID extends Window, KEY, VALUE>
  WindowedStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>, KEY, FlinkWindow<WID>>
  window(DataStream<StreamingWindowedElement<?, T>> input,
      UnaryFunction<T, KEY> keyFn,
      UnaryFunction<T, VALUE> valFn,
      Windowing<T, WID> windowing,
      UnaryFunction<T, Long> eventTimeAssigner) {

    if (eventTimeAssigner != null) {
        input = input.assignTimestampsAndWatermarks(
            new EventTimeAssigner<>(allowedLateness, (UnaryFunction) eventTimeAssigner));
    }

    DataStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>>
        elementsWithWindow =
        input.map(i -> {
          if (eventTimeAssigner != null) {
            i.setTimestamp(eventTimeAssigner.apply(i.getElement()));
          }

          return new MultiWindowedElement<>(
                  windowing.assignWindowsToElement(i),
                  Pair.of(keyFn.apply(i.getElement()), valFn.apply(i.getElement())));
        })
        .setParallelism(input.getParallelism())
        .returns((Class) MultiWindowedElement.class);

    KeyedStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((MultiWindowedElement<WID, Pair<KEY, VALUE>> in)
                -> in.getElement().getFirst()));

    WindowAssigner wassign;
    if (windowing instanceof MergingWindowing) {
      wassign = new FlinkMergingWindowAssigner((MergingWindowing) windowing);
    } else {
      wassign = new FlinkWindowAssigner<>(windowing);
    }
    return keyed.window(wassign);
  }

  private static org.apache.flink.streaming.api.windowing.time.Time
  millisTime(long millis) {
    return org.apache.flink.streaming.api.windowing.time.Time.milliseconds(millis);
  }

  static final class WeKeySelector<WID extends Window, KEY, VALUE> implements
      KeySelector<StreamingWindowedElement<WID, Pair<KEY, VALUE>>, KEY>
  {
    @Override
    public KEY getKey(StreamingWindowedElement<WID, Pair<KEY, VALUE>> value)
          throws Exception
    {
      return value.getElement().getKey();
    }
  }

  static class EventTimeAssigner<T>
      extends BoundedOutOfOrdernessTimestampExtractor<StreamingWindowedElement<?, T>>
  {
    private final UnaryFunction<T, Long> eventTimeFn;

    EventTimeAssigner(Duration allowedLateness, UnaryFunction<T, Long> eventTimeFn) {
      super(millisTime(allowedLateness.toMillis()));
      this.eventTimeFn = Objects.requireNonNull(eventTimeFn);
    }

    @Override
    public long extractTimestamp(StreamingWindowedElement<?, T> element) {
      return eventTimeFn.apply(element.getElement());
    }
  }
}
