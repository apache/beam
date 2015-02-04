/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A Reader that receives input data from a Windmill server, and returns it as
 * individual elements.
 */
class UngroupedWindmillReader<T> extends Reader<WindowedValue<T>> {
  private final Coder<T> valueCoder;
  private final Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private StreamingModeExecutionContext context;

  UngroupedWindmillReader(Coder<WindowedValue<T>> coder, StreamingModeExecutionContext context) {
    FullWindowedValueCoder<T> inputCoder = (FullWindowedValueCoder<T>) coder;
    this.valueCoder = inputCoder.getValueCoder();
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.context = context;
  }

  public static <T> UngroupedWindmillReader<T> create(PipelineOptions options,
                                             CloudObject spec,
                                             Coder coder,
                                             ExecutionContext context) {
    return new UngroupedWindmillReader<>(coder, (StreamingModeExecutionContext) context);
  }

  @Override
  public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return new UngroupedWindmillReaderIterator();
  }

  class UngroupedWindmillReaderIterator extends AbstractReaderIterator<WindowedValue<T>> {
    private int bundleIndex = 0;
    private int messageIndex = 0;

    @Override
    public boolean hasNext() throws IOException {
      Windmill.WorkItem work = context.getWork();
      return bundleIndex < work.getMessageBundlesCount() &&
          messageIndex < work.getMessageBundles(bundleIndex).getMessagesCount();
    }

    @Override
    public WindowedValue<T> next() throws IOException {
      Windmill.Message message =
          context.getWork().getMessageBundles(bundleIndex).getMessages(messageIndex);
      if (messageIndex >=
          context.getWork().getMessageBundles(bundleIndex).getMessagesCount() - 1) {
        messageIndex = 0;
        bundleIndex++;
      } else {
        messageIndex++;
      }
      Instant timestampMillis = new Instant(TimeUnit.MICROSECONDS.toMillis(message.getTimestamp()));
      InputStream data = message.getData().newInput();
      InputStream metadata = message.getMetadata().newInput();
      Collection<? extends BoundedWindow> windows = decode(windowsCoder, metadata);
      if (valueCoder instanceof KvCoder) {
        KvCoder kvCoder = (KvCoder) valueCoder;
        InputStream key = context.getSerializedKey().newInput();
        notifyElementRead(key.available() + data.available() + metadata.available());
        return WindowedValue.of((T) KV.of(decode(kvCoder.getKeyCoder(), key),
                                          decode(kvCoder.getValueCoder(), data)),
                                timestampMillis,
                                windows);
      } else {
        notifyElementRead(data.available() + metadata.available());
        return WindowedValue.of(decode(valueCoder, data), timestampMillis, windows);
      }
    }

    private <S> S decode(Coder<S> coder, InputStream input) throws IOException {
      return coder.decode(input, Coder.Context.OUTER);
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
