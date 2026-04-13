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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues.FullWindowedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Reader that receives input data from a Windmill server, and returns a singleton iterable
 * containing the work item.
 */
@Internal
class WindowingWindmillReader<K, T> extends NativeReader<WindowedValue<KeyedWorkItem<K, T>>> {

  private final Coder<K> keyCoder;
  private final Coder<T> valueCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private StreamingModeExecutionContext context;
  private final ValueProvider<Boolean> skipUndecodableElements;

  WindowingWindmillReader(
      Coder<WindowedValue<KeyedWorkItem<K, T>>> coder,
      StreamingModeExecutionContext context,
      ValueProvider<Boolean> skipUndecodableElements) {
    FullWindowedValueCoder<KeyedWorkItem<K, T>> inputCoder =
        (FullWindowedValueCoder<KeyedWorkItem<K, T>>) coder;
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.windowCoder = inputCoder.getWindowCoder();
    WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<K, T> keyedWorkItemCoder =
        (WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<K, T>) inputCoder.getValueCoder();
    this.keyCoder = keyedWorkItemCoder.getKeyCoder();
    this.valueCoder = keyedWorkItemCoder.getElementCoder();
    this.context = context;
    this.skipUndecodableElements = skipUndecodableElements;
  }

  /** A {@link ReaderFactory.Registrar} for grouping windmill sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      Factory factory = new Factory();
      return ImmutableMap.of(
          "WindowingWindmillReader",
          factory,
          "org.apache.beam.runners.dataflow.worker.WindowingWindmillReader",
          factory,
          "org.apache.beam.runners.dataflow.worker.BucketingWindmillSource",
          factory);
    }
  }

  static class Factory implements ReaderFactory {
    @Override
    @SuppressWarnings("rawtypes")
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext context,
        DataflowOperationContext operationContext)
        throws Exception {
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<KeyedWorkItem<Object, Object>>> typedCoder =
          (Coder<WindowedValue<KeyedWorkItem<Object, Object>>>) checkArgumentNotNull(coder);
      @Nullable
      ValueProvider<Boolean> skipUndecodableElements =
          (options != null)
              ? options
                  .as(DataflowStreamingPipelineOptions.class)
                  .getSkipInputElementsWithDecodingExceptions()
              : null;
      return WindowingWindmillReader.create(
          typedCoder,
          (StreamingModeExecutionContext) checkArgumentNotNull(context),
          skipUndecodableElements != null
              ? skipUndecodableElements
              : ValueProvider.StaticValueProvider.of(false));
    }
  }

  /**
   * Creates a {@link WindowingWindmillReader} from the provided {@link Coder} and {@link
   * StreamingModeExecutionContext}.
   */
  public static <K, T> WindowingWindmillReader<K, T> create(
      Coder<WindowedValue<KeyedWorkItem<K, T>>> coder,
      StreamingModeExecutionContext context,
      ValueProvider<Boolean> skipUndecodableElements) {
    return new WindowingWindmillReader<>(coder, context, skipUndecodableElements);
  }

  @Override
  public NativeReaderIterator<WindowedValue<KeyedWorkItem<K, T>>> iterator() throws IOException {
    final K key =
        keyCoder.decode(
            checkStateNotNull(context.getSerializedKey()).newInput(), Coder.Context.OUTER);
    final WorkItem workItem = context.getWorkItem();
    KeyedWorkItem<K, T> keyedWorkItem =
        new WindmillKeyedWorkItem<>(
            key,
            workItem,
            windowCoder,
            windowsCoder,
            valueCoder,
            context.getWindmillTagEncoding(),
            context.getDrainMode(),
            skipUndecodableElements.isAccessible()
                && Boolean.TRUE.equals(skipUndecodableElements.get()));
    final boolean isEmptyWorkItem =
        (Iterables.isEmpty(keyedWorkItem.timersIterable())
            && Iterables.isEmpty(keyedWorkItem.elementsIterable()));
    final WindowedValue<KeyedWorkItem<K, T>> value = new ValueInEmptyWindows<>(keyedWorkItem);

    // Return a noop iterator when current workitem is an empty workitem.
    if (isEmptyWorkItem) {
      return new NativeReaderIterator<WindowedValue<KeyedWorkItem<K, T>>>() {
        @Override
        public boolean start() throws IOException {
          return false;
        }

        @Override
        public boolean advance() throws IOException {
          return false;
        }

        @Override
        public WindowedValue<KeyedWorkItem<K, T>> getCurrent() {
          throw new NoSuchElementException();
        }
      };
    } else {
      return new NativeReaderIterator<WindowedValue<KeyedWorkItem<K, T>>>() {
        private @Nullable WindowedValue<KeyedWorkItem<K, T>> current = null;

        @Override
        public boolean start() throws IOException {
          current = value;
          return true;
        }

        @Override
        public boolean advance() throws IOException {
          current = null;
          return false;
        }

        @Override
        public WindowedValue<KeyedWorkItem<K, T>> getCurrent() {
          if (current == null) {
            throw new NoSuchElementException();
          }
          return value;
        }
      };
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
