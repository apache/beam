/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.KeyedWorkItem.KeyedWorkItemCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItem;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A Reader that receives input data from a Windmill server, and returns a singleton iterable
 * containing the work item.
 */
class WindowingWindmillReader<T> extends NativeReader<WindowedValue<KeyedWorkItem<T>>> {

  private final KvCoder<?, T> kvCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private StreamingModeExecutionContext context;

  WindowingWindmillReader(Coder<WindowedValue<KeyedWorkItem<T>>> coder,
                          StreamingModeExecutionContext context) {
    FullWindowedValueCoder<KeyedWorkItem<T>> inputCoder =
        (FullWindowedValueCoder<KeyedWorkItem<T>>) coder;
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.windowCoder = inputCoder.getWindowCoder();
    Coder<T> elementCoder = ((KeyedWorkItemCoder<T>) inputCoder.getValueCoder()).getElementCoder();
    if (!(elementCoder instanceof KvCoder)) {
      throw new IllegalArgumentException(
          "WindowingWindmillReader only works with KvCoders.");
    }
    @SuppressWarnings("unchecked")
    KvCoder<?, T> kvCoder = (KvCoder<?, T>)
        elementCoder;
    this.kvCoder = kvCoder;
    this.context = context;
  }

  static class Factory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext context,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName)
            throws Exception {
      @SuppressWarnings({"rawtypes", "unchecked"})
      Coder<WindowedValue<KeyedWorkItem<Object>>> typedCoder =
          (Coder<WindowedValue<KeyedWorkItem<Object>>>) coder;
      return WindowingWindmillReader.create(typedCoder, (StreamingModeExecutionContext) context);
    }
  }

  /**
   * Creates a {@link WindowingWindmillReader} from the provided {@link Coder}
   * and {@link StreamingModeExecutionContext}.
   */
  public static <T> WindowingWindmillReader<T> create(
      Coder<WindowedValue<KeyedWorkItem<T>>> coder,
      StreamingModeExecutionContext context) {
    return new WindowingWindmillReader<T>(coder, context);
  }

  @Override
  public NativeReaderIterator<WindowedValue<KeyedWorkItem<T>>> iterator() throws IOException {
    final Object key = kvCoder.getKeyCoder().decode(
        context.getSerializedKey().newInput(), Coder.Context.OUTER);
    final WorkItem workItem = context.getWork();

    return new LegacyReaderIterator<WindowedValue<KeyedWorkItem<T>>>() {
      boolean consumed = false;

      @Override
      public boolean hasNext() throws IOException {
        return !consumed;
      }

      @Override
      public WindowedValue<KeyedWorkItem<T>> next() throws IOException, NoSuchElementException {
        consumed = true;
        return WindowedValue.valueInEmptyWindows(
            KeyedWorkItem.<T>workItem(
                key, workItem, windowCoder, windowsCoder, kvCoder.getValueCoder()));
      }
    };
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
