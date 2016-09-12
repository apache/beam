package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindow;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamingExecutorContext
    extends ExecutorContext<StreamExecutionEnvironment, DataStream<?>> {

  private final StreamWindower windower = new StreamWindower();

  public StreamingExecutorContext(StreamExecutionEnvironment env,
                                  DAG<FlinkOperator<?>> dag)
  {
    super(env, dag);
  }

  /**
   * Creates a windowed stream based on euphoria windowing and key assigner.
   *
   * The returned windowed stream must be processed such that
   * {@link StreamingWindowedElement#emissionWatermark} is set to the current
   * watermark upon emission of the windows. Attached windowing is relying on that.
   */
  <T, LABEL, GROUP, KEY, VALUE, W extends org.apache.flink.streaming.api.windowing.windows.Window>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>, KEY, EmissionWindow<W>>
  windowStream(DataStream<StreamingWindowedElement<?, ?, T>> input,
      UnaryFunction<T, KEY> keyExtractor,
      UnaryFunction<T, VALUE> valueExtractor,
      Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {

    return windower.window(input, keyExtractor, valueExtractor, windowing);
  }

  /**
   * Creates an attached window stream, presuming a preceding non-attached
   * windowing on the input data stream forwarding
   * {@link StreamingWindowedElement#emissionWatermark} of the windows to attach to.
   */
  <GROUP, LABEL, T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY,
      VALUE>>, KEY, AttachedWindow<GROUP, LABEL>>
  attachedWindowStream(DataStream<StreamingWindowedElement<GROUP, LABEL, T>> input,
                       UnaryFunction<T, KEY> keyFn,
                       UnaryFunction<T, VALUE> valFn)
  {
    return windower.attachedWindow(input, keyFn, valFn);
  }
}
