/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
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
   * <p>
   * The returned windowed stream must be post processed using
   * {@link MultiWindowedElementWindowFunction}. Attached windowing is relying
   * on its effects.
   *
   * @param <T> the type of input elements
   * @param <WID> the type of windows produced
   * @param <KEY> the type of the elements' keys
   * @param <VALUE> the type of the elements' values
   *
   * @param input the input stream to be windowed
   * @param keyFn the key extraction function
   * @param valFn the value extraction function
   * @param windowing the windowing strategy to apply
   * @param eventTimeAssigner the event time extraction function
   *
   * @return a windowed stream prepared for consumption by {@link MultiWindowedElementWindowFunction}
   */
  public <T, WID extends Window, KEY, VALUE>
  WindowedStream<MultiWindowedElement<WID, Pair<KEY, VALUE>>,
      KEY, FlinkWindow<WID>>
  flinkWindow(DataStream<StreamingWindowedElement<?, T>> input,
              UnaryFunction<T, KEY> keyFn,
              UnaryFunction<T, VALUE> valFn,
              Windowing<T, ? extends Window> windowing,
              UnaryFunction<T, Long> eventTimeAssigner) {
    return windower.window((DataStream) input, keyFn, valFn, windowing, eventTimeAssigner);
  }

  /**
   * Creates an attached window stream, presuming a preceding non-attached
   * windowing on the input data stream forwarding
   * {@link StreamingWindowedElement#getTimestamp} of the windows to attach to.
   *
   * @param <T> the type of input elements
   * @param <WID> the type of windows produced
   * @param <KEY> the type of the elements' keys
   * @param <VALUE> the type of the elements' values
   *
   * @param input the input stream to be windowed
   * @param keyFn the key extraction function
   * @param valFn the value extraction function
   *
   * @return a windowed stream attached to the windowing of the original input stream
   */
  <T, WID extends Window, KEY, VALUE>
  WindowedStream<StreamingWindowedElement<WID, Pair<KEY, VALUE>>,
      KEY, AttachedWindow<WID>>
  attachedWindowStream(DataStream<StreamingWindowedElement<WID, T>> input,
                       UnaryFunction<T, KEY> keyFn,
                       UnaryFunction<T, VALUE> valFn)
  {
    return windower.attachedWindow(input, keyFn, valFn);
  }
}
