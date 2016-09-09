
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindowAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
class StreamWindower {

  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY, W>
  window(DataStream<StreamingWindowedElement<?, ?, T>> input, UnaryFunction<T, KEY> keyExtractor,
         UnaryFunction<T, VALUE> valueExtractor,
         Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof Time<?>) {
      return (WindowedStream) timeWindow(
          input, keyExtractor, valueExtractor, (Time<T>) windowing);
    } else if (windowing instanceof TimeSliding<?>) {
      return (WindowedStream) timeSlidingWindow(
          input, keyExtractor, valueExtractor, (TimeSliding<T>) windowing);
    } else {
      throw new UnsupportedOperationException("Not yet supported: " + windowing);
    }
  }


  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY, FlinkWindow>
  genericWindow(DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof TimeSliding<?>) {
      UnaryFunction eventTimeFn = ((TimeSliding) windowing).getEventTimeFn();
      if (!(eventTimeFn instanceof Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<StreamingWindowedElement<?, ?, T>>(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
              @Override
              public long extractTimestamp(StreamingWindowedElement<?, ?, T> element) {
                return (long) eventTimeFn.apply(element.get());
              }
            });
      }
    }


    Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>> genericWindowing
        = (Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>>) windowing;
    DataStream<StreamingWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>> elementsWithWindow =
        input.flatMap((i, c) -> {
          for (WindowID<GROUP, LABEL> w : genericWindowing.assignWindowsToElement(i)) {
            KEY key = keyExtractor.apply(i.get());
            VALUE value = valueExtractor.apply(i.get());
            c.collect(new StreamingWindowedElement(w, WindowedPair.of(w.getLabel(), key, value)));
          }
        })
        .returns((Class) StreamingWindowedElement.class);

    KeyedStream<StreamingWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((StreamingWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>> in) -> {
              return in.get().getFirst();
            }));

    // XXX needs to provide our own emissionTracking window-assigner
    return keyed.window((WindowAssigner) new FlinkWindowAssigner<>(genericWindowing));
  }



  private  <T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<Void, Time.TimeInterval,
        WindowedPair<Time.TimeInterval, KEY, VALUE>>, KEY, TimeWindow>
  timeWindow(
     DataStream<StreamingWindowedElement<?, ?, T>> input,
     UnaryFunction<T, KEY> keyExtractor,
     UnaryFunction<T, VALUE> valueExtractor,
     Time<T> windowing)
  {
    KeyedStream<StreamingWindowedElement<Void, Time.TimeInterval,
                    WindowedPair<Time.TimeInterval, KEY, VALUE>>, KEY> keyed;
    keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);

    // XXX needs to provide our own emissionTracking window-assigner
    return keyed.timeWindow(
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(
            windowing.getDuration()));
  }


  private <T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<Void, Long,
        WindowedPair<Long, KEY, VALUE>>, KEY, TimeWindow>
  timeSlidingWindow(
      DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      TimeSliding<T> windowing)
  {
    KeyedStream<StreamingWindowedElement<Void, Long, WindowedPair<Long, KEY, VALUE>>, KEY> keyed;
    keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);

    // XXX needs to provide our own emissionTracking window-assigner
    return keyed.timeWindow(
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(
            windowing.getDuration()),
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(
            windowing.getSlide()));
  }


  private <T, GROUP, LABEL, KEY, VALUE, W extends WindowContext<GROUP, LABEL>>
  KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY>
  keyTimeWindow(
      Windowing<T, GROUP, LABEL, W> windowing,
      UnaryFunction<T, Long> eventTimeFn,
      DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor) {

    if (!(eventTimeFn instanceof Time.ProcessingTime)) {
      input = input.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor<StreamingWindowedElement<?, ?, T>>(
              org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
            @Override public long extractTimestamp(StreamingWindowedElement<?, ?, T> element) {
              return eventTimeFn.apply(element.get());
            }
          });
    }

    DataStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>> mapped;
    mapped = input
        .map(i -> {
          T elem = i.get();
          KEY key = keyExtractor.apply(elem);
          VALUE val = valueExtractor.apply(elem);
          WindowID<GROUP, LABEL> wid = windowing.assignWindowsToElement(i).iterator().next();
          return new StreamingWindowedElement<>(wid, WindowedPair.of(wid.getLabel(), key, val));
        })
        .returns((Class) StreamingWindowedElement.class);
    final KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed;
  }

  static final class WeKeySelector<GROUP, LABEL, KEY, VALUE> implements
      KeySelector<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY>
  {
    @Override
    public KEY getKey(StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>> value)
          throws Exception
    {
      return value.get().getKey();
    }
  }
}
