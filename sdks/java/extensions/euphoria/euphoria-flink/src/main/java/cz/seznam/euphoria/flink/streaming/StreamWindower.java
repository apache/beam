
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
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindow;
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindowAssigner;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindowAssigner;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindowAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.timestamps
    .BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;
import java.util.Objects;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
class StreamWindower {

  private final Duration allowedLateness;

  public StreamWindower() {
     this(Duration.ofMillis(0));
  }

  public StreamWindower(Duration allowedLateness) {
    this.allowedLateness = Objects.requireNonNull(allowedLateness);
  }

  <GROUP, LABEL, T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>,
                 KEY,
                 AttachedWindow<GROUP, LABEL>>
  attachedWindow(DataStream<StreamingWindowedElement<GROUP, LABEL, T>> input,
                      UnaryFunction<T, KEY> keyFn,
                      UnaryFunction<T, VALUE> valFn)
  {
    DataStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>> mapped;
    mapped = input
        .map(i -> {
          T elem = i.get();
          KEY key = keyFn.apply(elem);
          VALUE val = valFn.apply(elem);
          WindowID<GROUP, LABEL> wid = i.getWindowID();
          return new StreamingWindowedElement<>(wid, WindowedPair.of(wid.getLabel(), key, val))
              // ~ forward the emission watermark
              .withEmissionWatermark(i.getEmissionWatermark());
        })
        .returns((Class) StreamingWindowedElement.class);
    final KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed.window(new AttachedWindowAssigner<>());
  }


  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY, EmissionWindow<W>>
  window(DataStream<StreamingWindowedElement<?, ?, T>> input,
         UnaryFunction<T, KEY> keyExtractor,
         UnaryFunction<T, VALUE> valueExtractor,
         Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof Time<?>) {
      Time<T> twin = (Time<T>) windowing;
      return (WindowedStream) keyWindow(twin, twin.getEventTimeFn(),
          input, keyExtractor, valueExtractor,
          TumblingEventTimeWindows.of(millisTime(twin.getDuration())));
    } else if (windowing instanceof TimeSliding<?>) {
      TimeSliding<T> twin = (TimeSliding<T>) windowing;
      return (WindowedStream) keyWindow(twin, twin.getEventTimeFn(),
          input, keyExtractor, valueExtractor,
          SlidingEventTimeWindows.of(
              millisTime(twin.getDuration()),
              millisTime(twin.getSlide())));
    } else {
      throw new UnsupportedOperationException("Not yet supported: " + windowing);
    }
  }


  @SuppressWarnings("unchecked")
  <T, LABEL, GROUP, KEY, VALUE, W extends Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY, EmissionWindow<FlinkWindow>>
  genericWindow(DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    if (windowing instanceof TimeSliding<?>) {
      UnaryFunction eventTimeFn = ((TimeSliding) windowing).getEventTimeFn();
      if (!(eventTimeFn instanceof Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new EventTimeAssigner<T>(allowedLateness, eventTimeFn));
      }
    }

    Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>> genericWindowing
        = (Windowing<T, GROUP, LABEL, WindowContext<GROUP, LABEL>>) windowing;
    DataStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>>
        elementsWithWindow =
        input.flatMap((i, c) -> {
          for (WindowID<GROUP, LABEL> w : genericWindowing.assignWindowsToElement(i)) {
            KEY key = keyExtractor.apply(i.get());
            VALUE value = valueExtractor.apply(i.get());
            c.collect(new StreamingWindowedElement(w, WindowedPair.of(w.getLabel(), key, value)));
          }
        })
        .returns((Class) StreamingWindowedElement.class);

    KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((StreamingWindowedElement<
                GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>> in) -> {
              return in.get().getFirst();
            }));

    return keyed.window((WindowAssigner)
        new EmissionWindowAssigner<>(
            new FlinkWindowAssigner<>(genericWindowing)));
  }



  @SuppressWarnings("unchecked")
  private <T, GROUP, LABEL, KEY, VALUE, WC extends WindowContext<GROUP, LABEL>, FW extends Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY, EmissionWindow<FW>>
  keyWindow(
      Windowing<T, GROUP, LABEL, WC> windowing,
      UnaryFunction<T, Long> eventTimeFn,
      DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyFn,
      UnaryFunction<T, VALUE> valFn,
      WindowAssigner<? super T, FW> flinkWindowAssigner)
  {
    if (!(eventTimeFn instanceof Time.ProcessingTime)) {
      input = input.assignTimestampsAndWatermarks(
          new EventTimeAssigner<T>(allowedLateness, eventTimeFn));
    }

    DataStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>> mapped;
    mapped = input
        .map(i -> {
          T elem = i.get();
          KEY key = keyFn.apply(elem);
          VALUE val = valFn.apply(elem);
          // FIXME this is not right; we cannot just use the first window retrieved; see #windowIdFromSlidingFlinkWindow
          WindowID<GROUP, LABEL> wid = windowing.assignWindowsToElement(i).iterator().next();
          return new StreamingWindowedElement<>(wid, WindowedPair.of(wid.getLabel(), key, val));
        })
        .returns((Class) StreamingWindowedElement.class);
    final KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed.window(new EmissionWindowAssigner(flinkWindowAssigner));
  }

  private static org.apache.flink.streaming.api.windowing.time.Time
  millisTime(long millis) {
    return org.apache.flink.streaming.api.windowing.time.Time.milliseconds(millis);
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

  static class EventTimeAssigner<T>
      extends BoundedOutOfOrdernessTimestampExtractor<StreamingWindowedElement<?, ?, T>>
  {
    private final UnaryFunction<T, Long> eventTimeFn;

    EventTimeAssigner(Duration allowedLateness, UnaryFunction<T, Long> eventTimeFn) {
      super(millisTime(allowedLateness.toMillis()));
      this.eventTimeFn = Objects.requireNonNull(eventTimeFn);
    }

    @Override
    public long extractTimestamp(StreamingWindowedElement<?, ?, T> element) {
      return eventTimeFn.apply(element.get());
    }
  }

  // FIXME: this does not need to exist and indeed is just a hack! it attempts to fix
  // the problem that for TimeSliding StreamWindower assigns only one window to the element
  public static WindowID<?, ?> windowIdFromSlidingFlinkWindow(Class<Windowing> type, Window flinkWindow) {
    if (TimeSliding.class.isAssignableFrom(type)) {
      final TimeWindow tw = (TimeWindow) flinkWindow;
      return WindowID.aligned(tw.getStart());
    }
    return null;
  }
}
