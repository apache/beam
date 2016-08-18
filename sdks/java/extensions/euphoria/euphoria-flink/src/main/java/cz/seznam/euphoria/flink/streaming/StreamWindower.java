
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindowAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
class StreamWindower {

  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends org.apache.flink.streaming.api.windowing.windows.Window>
  WindowedStream<WindowedPair<LABEL, KEY, VALUE>, KEY, W>
  window(DataStream<T> input, UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof Windowing.Time<?>) {
      return (WindowedStream) timeWindow(
          input, keyExtractor, valueExtractor, (Windowing.Time<T>) windowing);
    } else {
      throw new UnsupportedOperationException("Not yet supported");
    }
  }


  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends org.apache.flink.streaming.api.windowing.windows.Window>
  WindowedStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, KEY, FlinkWindow>
  genericWindow(DataStream<T> input, UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof Windowing.TimeSliding<?>) {
      UnaryFunction eventTimeFn = ((Windowing.TimeSliding) windowing).getEventTimeFn();
      if (!(eventTimeFn instanceof Windowing.Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<T>(Time.seconds(1)) {
              @Override public long extractTimestamp(T element) {
                return (long) eventTimeFn.apply(element);
              }
            });
      }
    }


    Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>> genericWindowing
        = (Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>>) windowing;
    DataStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>> elementsWithWindow = input
        .flatMap((i, c) -> {
          for (WindowID<GROUP, LABEL> w : genericWindowing.assignWindows(i)) {
            c.collect(Pair.of(w, Pair.of(keyExtractor.apply(i), valueExtractor.apply(i))));
          }
        })
        .returns((Class) Pair.class);

    KeyedStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>> in) -> {
              return in.getSecond().getFirst();
            }));

    FlinkWindowAssigner<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, T, GROUP, LABEL> wassigner
        = new FlinkWindowAssigner<>(
            genericWindowing,
            Pair::getFirst);

    return keyed.window(wassigner);
  }



  private  <T, GROUP, KEY, VALUE>
  WindowedStream<WindowedPair<Windowing.Time.TimeInterval, KEY, VALUE>, KEY, TimeWindow> timeWindow(
     DataStream<T> input,
     UnaryFunction<T, KEY> keyExtractor,
     UnaryFunction<T, VALUE> valueExtractor,
     Windowing.Time<T> windowing) {
    
    KeyedStream<WindowedPair<Windowing.Time.TimeInterval, KEY, VALUE>, KEY> keyed;
    keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);

    return keyed.timeWindow(Time.milliseconds(windowing.getDuration()));
  }


  private <T, KEY, VALUE>
  WindowedStream<WindowedPair<Long, KEY, VALUE>, KEY, TimeWindow> timeSlidingWindow(
      DataStream<T> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing.TimeSliding<T> windowing) {

    KeyedStream<WindowedPair<Long, KEY, VALUE>, KEY> keyed = keyTimeWindow(
        windowing, windowing.getEventTimeFn(),
        input, keyExtractor, valueExtractor);

    return keyed.timeWindow(Time.milliseconds(windowing.getDuration()),
        Time.milliseconds(windowing.getSlide()));

  }


  private <T, GROUP, LABEL, KEY, VALUE, W extends WindowContext<GROUP, LABEL>>
  KeyedStream<WindowedPair<LABEL, KEY, VALUE>, KEY> keyTimeWindow(
      Windowing<T, GROUP, LABEL, W> windowing,
      UnaryFunction<T, Long> eventTimeFn,
      DataStream<T> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor) {

    if (!(eventTimeFn instanceof Windowing.Time.ProcessingTime)) {
      input = input.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor<T>(Time.seconds(1)) {
            @Override public long extractTimestamp(T element) {
              return eventTimeFn.apply(element);
            }
          });
    }
    
    DataStream<WindowedPair<LABEL, KEY, VALUE>> mapped = input
        .map(i -> WindowedPair.of(windowing.assignWindows(i).iterator().next().getLabel(),
            keyExtractor.apply(i), valueExtractor.apply(i)))
        .returns((Class) WindowedPair.class);
    final KeyedStream<WindowedPair<LABEL, KEY, VALUE>, KEY> keyed;
    keyed = mapped.keyBy(Utils.keyByPairFirst());
    return keyed;
  }


}
