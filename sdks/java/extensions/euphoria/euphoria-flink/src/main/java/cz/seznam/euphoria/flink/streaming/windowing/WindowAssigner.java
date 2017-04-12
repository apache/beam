/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Assigns windows to element and extracts key and value.
 *
 * @param <I>     type of input data
 * @param <KEY>   type of output key
 * @param <VALUE> type of output value
 * @param <W>     type of output window
 */
public class WindowAssigner<I, KEY, VALUE, W extends Window>
        implements Function<StreamRecord<StreamingElement<?, I>>, KeyedMultiWindowedElement<W, KEY, VALUE>>,
        Serializable {

  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;

  private transient TimestampedElement reuse;

  public WindowAssigner(Windowing windowing,
                        UnaryFunction keyExtractor,
                        UnaryFunction valueExtractor) {
    this.windowing = windowing;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public KeyedMultiWindowedElement<W, KEY, VALUE> apply(StreamRecord<StreamingElement<?, I>> record) {
    StreamingElement el = record.getValue();

    if (reuse == null) {
      reuse = new TimestampedElement();
    }
    reuse.setTimestamp(record.getTimestamp());
    reuse.setStreamingElement(el);
    Iterable windows = windowing.assignWindowsToElement(reuse);

    return new KeyedMultiWindowedElement<>(
            keyExtractor.apply(el.getElement()),
            valueExtractor.apply(el.getElement()),
            windows);
  }

  private static class TimestampedElement<W extends Window, T>
          implements WindowedElement<W, T> {

    private long timestamp;
    private StreamingElement<W, T> element;

    @Override
    public W getWindow() {
      return element.getWindow();
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    private void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public T getElement() {
      return element.getElement();
    }

    private void setStreamingElement(StreamingElement<W, T> element) {
      this.element = element;
    }
  }
}
