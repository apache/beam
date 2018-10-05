package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;

/**
 * Windowing function to extract the emission watermark from the window being
 * emitted and forward it along the emitted element(s). Further ensures that
 * the emitted window-id stored on the elements corresponds correctly to the
 * emitted window.
 */
public class MultiWindowedElementWindowFunction<LABEL, KEY, VALUE>
    implements WindowFunction<
    MultiWindowedElement<?, Pair<KEY, VALUE>>,
    StreamingWindowedElement<LABEL, Pair<KEY, VALUE>>,
    KEY,
    FlinkWindow<LABEL>> {

  @Override
  public void apply(
      KEY key,
      FlinkWindow<LABEL> window,
      Iterable<MultiWindowedElement<?, Pair<KEY, VALUE>>> input,
      Collector<StreamingWindowedElement<LABEL, Pair<KEY, VALUE>>> out) {
    for (MultiWindowedElement<?, Pair<KEY, VALUE>> i : input) {
      WindowID<LABEL> wid = window.getWindowID();
      out.collect(
          new StreamingWindowedElement<>(
              wid,
              Pair.of(i.get().getFirst(), i.get().getSecond()))
              .withEmissionWatermark(window.getEmissionWatermark()));
    }
  }
}