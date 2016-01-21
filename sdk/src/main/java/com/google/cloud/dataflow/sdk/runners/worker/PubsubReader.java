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
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * A Reader that receives elements from Pubsub, via a Windmill server.
 */
class PubsubReader<T> extends NativeReader<WindowedValue<T>> {
  private final ValueOnlyWindowedValueCoder<?> coder;
  private StreamingModeExecutionContext context;

  PubsubReader(Coder<WindowedValue<T>> coder, StreamingModeExecutionContext context) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    ValueOnlyWindowedValueCoder<?> typedCoder = (ValueOnlyWindowedValueCoder) coder;
    this.coder = typedCoder;
    this.context = context;
  }

  static class Factory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject cloudSourceSpec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext executionContext,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName)
            throws Exception {
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<Object>> typedCoder = (Coder<WindowedValue<Object>>) coder;
      return new PubsubReader<>(typedCoder, (StreamingModeExecutionContext) executionContext);
    }
  }

  @Override
  public NativeReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return new PubsubReaderIterator(context.getWork());
  }

  class PubsubReaderIterator extends WindmillReaderIteratorBase<T> {
    protected PubsubReaderIterator(Windmill.WorkItem work) {
      super(work);
    }

    @Override
    protected WindowedValue<T> decodeMessage(Windmill.Message message) throws IOException {
      long timestampMillis = TimeUnit.MICROSECONDS.toMillis(message.getTimestamp());
      InputStream data = message.getData().newInput();
      notifyElementRead(data.available());
      @SuppressWarnings("unchecked")
      T value = (T) coder.getValueCoder().decode(data, Coder.Context.OUTER);
      return WindowedValue.timestampedValueInGlobalWindow(value, new Instant(timestampMillis));
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
