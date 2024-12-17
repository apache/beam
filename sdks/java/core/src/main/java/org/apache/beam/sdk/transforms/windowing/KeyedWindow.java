/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.windowing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Internal
public class KeyedWindow<K, W extends @NonNull BoundedWindow> extends BoundedWindow {

  private final K key;
  private final W window;

  public KeyedWindow(K key, W window) {
    this.key = key;
    this.window = window;
  }

  public K getKey() {
    return key;
  }

  public W getWindow() {
    return window;
  }

  @Override
  public Instant maxTimestamp() {
    return window.maxTimestamp();
  }

  @Override
  public String toString() {
    return "NamedWindow{" + "name='" + key + '\'' + ", window=" + window + '}';
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyedWindow)) {
      return false;
    }
    KeyedWindow<?, ?> that = (KeyedWindow<?, ?>) o;
    return Objects.equals(key, that.key) && Objects.equals(window, that.window);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, window);
  }

  @Internal
  public static class KeyedWindowFn<K, V, W extends BoundedWindow>
      extends WindowFn<KV<K, V>, KeyedWindow<K, W>> {

    private final WindowFn<V, W> windowFn;
    private final Coder<K> keyCoder;

    public KeyedWindowFn(Coder<K> keyCoder, WindowFn<?, ?> windowFn) {
      this.keyCoder = keyCoder;
      this.windowFn = (WindowFn<V, W>) windowFn;
    }

    public WindowFn<V, W> getInnerWindowFn() {
      return windowFn;
    }

    @Override
    public Collection<KeyedWindow<K, W>> assignWindows(
        WindowFn<KV<K, V>, KeyedWindow<K, W>>.AssignContext c) throws Exception {

      return windowFn
          .assignWindows(
              new WindowFn<V, W>.AssignContext() {

                @Override
                public V element() {
                  return c.element().getValue();
                }

                @Override
                public Instant timestamp() {
                  return c.timestamp();
                }

                @Override
                public BoundedWindow window() {
                  return c.window();
                }
              })
          .stream()
          .map(window -> new KeyedWindow<>(c.element().getKey(), window))
          .collect(Collectors.toList());
    }

    @Override
    public void mergeWindows(WindowFn<KV<K, V>, KeyedWindow<K, W>>.MergeContext c)
        throws Exception {
      if (windowFn instanceof NonMergingWindowFn) {
        return;
      }
      HashMap<K, List<W>> keyToWindow = new HashMap<>();
      c.windows()
          .forEach(
              keyedWindow -> {
                List<W> windows =
                    keyToWindow.computeIfAbsent(keyedWindow.getKey(), k -> new ArrayList<>());
                windows.add(keyedWindow.getWindow());
              });
      for (Entry<K, List<W>> entry : keyToWindow.entrySet()) {
        K key = entry.getKey();
        List<W> windows = entry.getValue();
        windowFn.mergeWindows(
            new WindowFn<V, W>.MergeContext() {
              @Override
              public Collection<W> windows() {
                return windows;
              }

              @Override
              public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
                List<KeyedWindow<K, W>> toMergedKeyedWindows =
                    toBeMerged.stream()
                        .map(window -> new KeyedWindow<>(key, window))
                        .collect(Collectors.toList());
                c.merge(toMergedKeyedWindows, new KeyedWindow<>(key, mergeResult));
              }
            });
      }
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return (other instanceof KeyedWindowFn)
          && windowFn.isCompatible(((KeyedWindowFn<?, ?, ?>) other).windowFn);
    }

    @Override
    public Coder<KeyedWindow<K, W>> windowCoder() {
      return new KeyedWindowCoder<>(keyCoder, windowFn.windowCoder());
    }

    @Override
    public WindowMappingFn<KeyedWindow<K, W>> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException("KeyedWindow not supported with side inputs");
    }

    @Override
    public boolean isNonMerging() {
      return windowFn.isNonMerging();
    }

    @Override
    public boolean assignsToOneWindow() {
      return windowFn.assignsToOneWindow();
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (other instanceof KeyedWindowFn) {
        windowFn.verifyCompatibility(((KeyedWindowFn<?, ?, ?>) other).windowFn);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      windowFn.populateDisplayData(builder);
    }
  }

  @Internal
  public static class KeyedWindowCoder<K, W extends BoundedWindow>
      extends Coder<KeyedWindow<K, W>> {

    private final KvCoder<K, W> coder;

    public KeyedWindowCoder(Coder<K> keyCoder, Coder<W> windowCoder) {
      // :TODO consider swapping the order for improved state locality
      this.coder = KvCoder.of(keyCoder, windowCoder);
    }

    @Override
    public void encode(KeyedWindow<K, W> value, OutputStream outStream) throws IOException {
      coder.encode(KV.of(value.getKey(), value.getWindow()), outStream);
    }

    @Override
    public KeyedWindow<K, W> decode(InputStream inStream) throws IOException {
      KV<K, W> decode = coder.decode(inStream);
      return new KeyedWindow<>(decode.getKey(), decode.getValue());
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return coder.getCoderArguments();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      coder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      return coder.getValueCoder().consistentWithEquals();
    }
  }
}
