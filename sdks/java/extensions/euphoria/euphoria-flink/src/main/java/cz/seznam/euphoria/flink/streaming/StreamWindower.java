
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
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
  WindowedStream<WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY, W>
  window(DataStream<WindowedElement<?, ?, T>> input, UnaryFunction<T, KEY> keyExtractor,
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
  WindowedStream<WindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY, FlinkWindow>
  genericWindow(DataStream<WindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof TimeSliding<?>) {
      UnaryFunction eventTimeFn = ((TimeSliding) windowing).getEventTimeFn();
      if (!(eventTimeFn instanceof Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<WindowedElement<?, ?, T>>(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
              @Override
              public long extractTimestamp(WindowedElement<?, ?, T> element) {
                return (long) eventTimeFn.apply(element.get());
              }
            });
      }
    }


    Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>> genericWindowing
        = (Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>>) windowing;
    DataStream<WindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>> elementsWithWindow =
        input.flatMap((i, c) -> {
          for (WindowID<GROUP, LABEL> w : genericWindowing.assignWindowsToElement(i)) {
            c.collect(new WindowedElement(w,
                Pair.of(keyExtractor.apply(i.get()), valueExtractor.apply(i.get()))));
          }
        })
        .returns((Class) WindowedElement.class);

    KeyedStream<WindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((WindowedElement<GROUP, LABEL, Pair<KEY, VALUE>> in) -> {
              return in.get().getFirst();
            }));

    return keyed.window((WindowAssigner) new FlinkWindowAssigner<>(genericWindowing));
  }



  private  <T, KEY, VALUE>
  WindowedStream<WindowedElement<Void, Time.TimeInterval,
      WindowedPair<Time.TimeInterval, KEY, VALUE>>, KEY, TimeWindow>
  timeWindow(
     DataStream<WindowedElement<?, ?, T>> input,
     UnaryFunction<T, KEY> keyExtractor,
     UnaryFunction<T, VALUE> valueExtractor,
     Time<T> windowing)
  {
    KeyedStream<WindowedElement<Void, Time.TimeInterval,
                WindowedPair<Time.TimeInterval, KEY, VALUE>>, KEY> keyed;
    keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);
    return keyed.timeWindow(
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(
            windowing.getDuration()));
  }


  private <T, KEY, VALUE>
  WindowedStream<WindowedElement<Void, Long,
      WindowedPair<Long, KEY, VALUE>>, KEY, TimeWindow>
  timeSlidingWindow(
      DataStream<WindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      TimeSliding<T> windowing)
  {
    KeyedStream<WindowedElement<Void, Long, WindowedPair<Long, KEY, VALUE>>, KEY> keyed;
    keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);

    return keyed.timeWindow(
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(windowing.getDuration()),
        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(windowing.getSlide()));

  }


  private <T, GROUP, LABEL, KEY, VALUE, W extends WindowContext<GROUP, LABEL>>
  KeyedStream<WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY>
  keyTimeWindow(
      Windowing<T, GROUP, LABEL, W> windowing,
      UnaryFunction<T, Long> eventTimeFn,
      DataStream<WindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor) {

    if (!(eventTimeFn instanceof Time.ProcessingTime)) {
      input = input.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor<WindowedElement<?, ?, T>>(
              org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
            @Override public long extractTimestamp(WindowedElement<?, ?, T> element) {
              return eventTimeFn.apply(element.get());
            }
          });
    }
    
    DataStream<WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>> mapped;
    mapped = input
        .map(i -> {
          T elem = i.get();
          KEY key = keyExtractor.apply(elem);
          VALUE val = valueExtractor.apply(elem);
          WindowID<GROUP, LABEL> wid = windowing.assignWindowsToElement(i).iterator().next();
          return new WindowedElement<>(wid, WindowedPair.of(wid.getLabel(), key, val));
        })
        .returns((Class) WindowedElement.class);
    final KeyedStream<WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed;
  }

  static final class WeKeySelector<GROUP, LABEL, KEY, VALUE> implements
      KeySelector<WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY>
  {
    @Override
    public KEY getKey(WindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>> value)
          throws Exception
    {
      return value.get().getKey();
    }
  }
}
