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
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A Reader that receives elements from Pubsub, via a Windmill server.
 */
class PubsubReader<T> extends Reader<WindowedValue<T>> {
  private final ValueOnlyWindowedValueCoder coder;
  private StreamingModeExecutionContext context;

  PubsubReader(Coder<WindowedValue<T>> coder, StreamingModeExecutionContext context) {
    this.coder = (ValueOnlyWindowedValueCoder) coder;
    this.context = context;
  }

  public static <T> PubsubReader<T> create(
      PipelineOptions options,
      CloudObject spec,
      Coder<WindowedValue<T>> coder,
      ExecutionContext context) {
    return new PubsubReader<>(coder, (StreamingModeExecutionContext) context);
  }

  @Override
  public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return new PubsubReaderIterator();
  }

  class PubsubReaderIterator extends AbstractReaderIterator<WindowedValue<T>> {
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
      long timestampMillis = TimeUnit.MICROSECONDS.toMillis(message.getTimestamp());
      InputStream data = message.getData().newInput();
      notifyElementRead(data.available());
      T value = (T) coder.getValueCoder().decode(data, Coder.Context.OUTER);
      return WindowedValue.of(value,
                              new Instant(timestampMillis),
                              Arrays.asList(GlobalWindow.INSTANCE));
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
