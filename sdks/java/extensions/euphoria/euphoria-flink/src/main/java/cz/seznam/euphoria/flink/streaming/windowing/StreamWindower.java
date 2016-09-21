package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.timestamps
    .BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Class creating {@code WindowedStream} from {@code DataStream}s.
 */
public class StreamWindower {

  private final Duration allowedLateness;

  public StreamWindower(Duration allowedLateness) {
    this.allowedLateness = Objects.requireNonNull(allowedLateness);
  }

  @SuppressWarnings("unchecked")
  public <GROUP, LABEL, T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>,
                 KEY,
                 AttachedWindow<GROUP, LABEL>>
  attachedWindow(DataStream<StreamingWindowedElement<GROUP, LABEL, T>> input,
                      UnaryFunction<T, KEY> keyFn,
                      UnaryFunction<T, VALUE> valFn)
  {
    DataStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>> mapped
        = input.map(i -> {
          T elem = i.get();
          KEY key = keyFn.apply(elem);
          VALUE val = valFn.apply(elem);
          WindowID<GROUP, LABEL> wid = i.getWindowID();
          return new StreamingWindowedElement<>(wid, WindowedPair.of(wid.getLabel(), key, val))
              // ~ forward the emission watermark
              .withEmissionWatermark(i.getEmissionWatermark());
        })
        .setParallelism(input.getParallelism())
        .returns((Class) StreamingWindowedElement.class);
    final KeyedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY> keyed;
    keyed = mapped.keyBy(Utils.wrapQueryable(new WeKeySelector<>()));
    return keyed.window(new AttachedWindowAssigner<>());
  }

  @SuppressWarnings("unchecked")
  public <T, LABEL, GROUP, KEY, VALUE>
  WindowedStream<MultiWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY, FlinkWindow<GROUP, LABEL>>
  window(DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyFn,
      UnaryFunction<T, VALUE> valFn,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    Optional<UnaryFunction<T, Long>> tsAssign = windowing.getTimestampAssigner();
    if (tsAssign.isPresent()) {
      UnaryFunction tsFn = tsAssign.get();
      if (!(tsFn instanceof Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new EventTimeAssigner<T>(allowedLateness, tsFn));
      }
    }

    DataStream<MultiWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>>
        elementsWithWindow =
        input.map(i ->
            new MultiWindowedElement<>(
                windowing.assignWindowsToElement(i),
                Pair.of(keyFn.apply(i.get()), valFn.apply(i.get()))))
        .setParallelism(input.getParallelism())
        .returns((Class) MultiWindowedElement.class);

    // XXX try to get rid of the Pair<KEY,VALUE>; prefer merely VALUE
    KeyedStream<MultiWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((MultiWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>> in)
                -> in.get().getFirst()));

    return keyed.window((WindowAssigner)
        new FlinkWindowAssigner<>(windowing, keyed.getExecutionConfig()));
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
}
