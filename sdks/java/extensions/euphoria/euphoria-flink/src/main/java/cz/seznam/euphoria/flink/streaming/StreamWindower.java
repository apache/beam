
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
class StreamWindower {

  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends org.apache.flink.streaming.api.windowing.windows.Window>
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
      throw new UnsupportedOperationException("Not yet supported");
    }
  }


//  @SuppressWarnings("unchecked")
//  <T, LABEL, GROUP, KEY, VALUE, W extends org.apache.flink.streaming.api.windowing.windows.Window>
//  WindowedStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, KEY, FlinkWindow>
//  genericWindow(DataStream<T> input, UnaryFunction<T, KEY> keyExtractor,
//      UnaryFunction<T, VALUE> valueExtractor,
//      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {
//
//    if (windowing instanceof TimeSliding<?>) {
//      UnaryFunction eventTimeFn = ((TimeSliding) windowing).getEventTimeFn();
//      if (!(eventTimeFn instanceof Time.ProcessingTime)) {
//        input = input.assignTimestampsAndWatermarks(
//            new BoundedOutOfOrdernessTimestampExtractor<T>(
//                org.apache.flink.streaming.api.windowing.time.Time.seconds(1)) {
//              @Override public long extractTimestamp(T element) {
//                return (long) eventTimeFn.apply(element);
//              }
//            });
//      }
//    }
//
//
//    Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>> genericWindowing
//        = (Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>>) windowing;
//    DataStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>> elementsWithWindow =
//        input.flatMap((i, c) -> {
//          // FIXME extract "window-id" from the input element itself before passing
//          // it downstream to the windowing strategy
//          WindowedElement<GROUP, LABEL, T> wi = new WindowedElement<>(null, i);
//          for (WindowID<GROUP, LABEL> w : genericWindowing.assignWindowsToElement(wi)) {
//            c.collect(Pair.of(w, Pair.of(keyExtractor.apply(i), valueExtractor.apply(i))));
//          }
//        })
//        .returns((Class) Pair.class);
//
//    KeyedStream<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, KEY> keyed
//        = elementsWithWindow.keyBy(
//            Utils.wrapQueryable((Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>> in) -> {
//              return in.getSecond().getFirst();
//            }));
//
//    FlinkWindowAssigner<Pair<WindowID<GROUP, LABEL>, Pair<KEY, VALUE>>, T, GROUP, LABEL> wassigner
//        = new FlinkWindowAssigner<>(
//            genericWindowing,
//            Pair::getFirst);
//
//    return keyed.window(wassigner);
//  }



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
          // FIXME extract "window-id" from the input element itself before passing
          // it downstream to the windowing strategy
          T elem = i.get();
          KEY key = keyExtractor.apply(elem);
          VALUE val = valueExtractor.apply(elem);
          WindowID<GROUP, LABEL> wid = windowing.assignWindowsToElement(i).iterator().next();
          return new WindowedElement<>(wid, WindowedPair.of(wid, key, val));
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
