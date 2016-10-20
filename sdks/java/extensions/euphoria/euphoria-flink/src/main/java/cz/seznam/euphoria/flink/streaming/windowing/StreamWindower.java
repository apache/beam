package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
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
          T elem = i.get();
          KEY key = keyFn.apply(elem);
          VALUE val = valFn.apply(elem);
          WID wid = i.getWindow();
          return new StreamingWindowedElement<>(wid, Pair.of(key, val))
              // ~ forward the emission watermark
              .withEmissionWatermark(i.getEmissionWatermark());
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
      Windowing<T, WID> windowing) {

    Optional<UnaryFunction<T, Long>> tsAssign = windowing.getTimestampAssigner();
    if (tsAssign.isPresent()) {
      UnaryFunction tsFn = tsAssign.get();
      if (!(tsFn instanceof Time.ProcessingTime)) {
        input = input.assignTimestampsAndWatermarks(
            new EventTimeAssigner<>(allowedLateness, tsFn));
      }
    }

    DataStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>>
        elementsWithWindow =
        input.map(i -> new MultiWindowedElement<>(
                windowing.assignWindowsToElement((StreamingWindowedElement) i),
                Pair.of(keyFn.apply(i.get()), valFn.apply(i.get()))))
        .setParallelism(input.getParallelism())
        .returns((Class) MultiWindowedElement.class);

    KeyedStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>, KEY> keyed
        = elementsWithWindow.keyBy(
            Utils.wrapQueryable((MultiWindowedElement<WID, Pair<KEY, VALUE>> in)
                -> in.get().getFirst()));

    return keyed.window((WindowAssigner) new FlinkWindowAssigner<>(windowing));
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
      return value.get().getKey();
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
      return eventTimeFn.apply(element.get());
    }
  }
}
