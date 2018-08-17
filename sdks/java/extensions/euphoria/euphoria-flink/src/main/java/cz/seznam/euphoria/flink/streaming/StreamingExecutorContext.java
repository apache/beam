package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElementWindowFunction;
import cz.seznam.euphoria.flink.streaming.windowing.StreamWindower;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;


public class StreamingExecutorContext
    extends ExecutorContext<StreamExecutionEnvironment, DataStream<?>> {

  private final StreamWindower windower;

  public StreamingExecutorContext(StreamExecutionEnvironment env,
                                  DAG<FlinkOperator<?>> dag,
                                  StreamWindower streamWindower)
  {
    super(env, dag);
    this.windower = Objects.requireNonNull(streamWindower);
  }

  /**
   * Creates a windowed stream based on euphoria windowing and key assigner.
   *
   * The returned windowed stream must be post processed using
   * {@link MultiWindowedElementWindowFunction}.
   * Attached windowing is relying on its effects.
   */
  public <T, LABEL, GROUP, KEY, VALUE>
  WindowedStream<MultiWindowedElement<GROUP, LABEL, Pair<KEY, VALUE>>,
      KEY, FlinkWindow<GROUP, LABEL>>
  flinkWindow(DataStream<StreamingWindowedElement<?, ?, T>> input,
              UnaryFunction<T, KEY> keyFn,
              UnaryFunction<T, VALUE> valFn,
              Windowing<T, GROUP, LABEL, ? extends WindowContext<GROUP, LABEL>> windowing) {
    return windower.window(input, keyFn, valFn, windowing);
  }

  /**
   * Creates an attached window stream, presuming a preceding non-attached
   * windowing on the input data stream forwarding
   * {@link StreamingWindowedElement#emissionWatermark} of the windows to attach to.
   */
  <GROUP, LABEL, T, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUE>>,
      KEY, AttachedWindow<GROUP, LABEL>>
  attachedWindowStream(DataStream<StreamingWindowedElement<GROUP, LABEL, T>> input,
                       UnaryFunction<T, KEY> keyFn,
                       UnaryFunction<T, VALUE> valFn)
  {
    return windower.attachedWindow(input, keyFn, valFn);
  }
}
