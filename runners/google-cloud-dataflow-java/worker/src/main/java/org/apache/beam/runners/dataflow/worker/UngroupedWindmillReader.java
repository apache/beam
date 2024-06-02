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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A Reader that receives input data from a Windmill server, and returns it as individual elements.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class UngroupedWindmillReader<T> extends NativeReader<WindowedValue<T>> {
  private final Coder<T> valueCoder;
  private final Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private StreamingModeExecutionContext context;

  UngroupedWindmillReader(Coder<WindowedValue<T>> coder, StreamingModeExecutionContext context) {
    FullWindowedValueCoder<T> inputCoder = (FullWindowedValueCoder<T>) coder;
    this.valueCoder = inputCoder.getValueCoder();
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.context = context;
  }

  /** A {@link ReaderFactory.Registrar} for ungrouped windmill sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      Factory factory = new Factory();
      return ImmutableMap.of(
          "UngroupedWindmillReader", factory,
          "org.apache.beam.runners.dataflow.worker.UngroupedWindmillSource", factory,
          "org.apache.beam.runners.dataflow.worker.UngroupedWindmillReader", factory);
    }
  }

  static class Factory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      coder = checkArgumentNotNull(coder);
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<Object>> typedCoder = (Coder<WindowedValue<Object>>) coder;
      return new UngroupedWindmillReader<>(
          typedCoder, (StreamingModeExecutionContext) executionContext);
    }
  }

  @Override
  public NativeReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return new UngroupedWindmillReaderIterator(context.getWorkItem());
  }

  class UngroupedWindmillReaderIterator extends WindmillReaderIteratorBase {
    UngroupedWindmillReaderIterator(Windmill.WorkItem work) {
      super(work);
    }

    @Override
    public boolean advance() throws IOException {
      if (context.workIsFailed()) {
        return false;
      }
      return super.advance();
    }

    @Override
    protected WindowedValue<T> decodeMessage(Windmill.Message message) throws IOException {
      Instant timestampMillis =
          WindmillTimeUtils.windmillToHarnessTimestamp(message.getTimestamp());
      InputStream data = message.getData().newInput();
      InputStream metadata = message.getMetadata().newInput();
      Collection<? extends BoundedWindow> windows =
          WindmillSink.decodeMetadataWindows(windowsCoder, message.getMetadata());
      PaneInfo pane = WindmillSink.decodeMetadataPane(message.getMetadata());
      if (valueCoder instanceof KvCoder) {
        KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) valueCoder;
        InputStream key = context.getSerializedKey().newInput();
        notifyElementRead(key.available() + data.available() + metadata.available());

        @SuppressWarnings("unchecked")
        T result =
            (T) KV.of(decode(kvCoder.getKeyCoder(), key), decode(kvCoder.getValueCoder(), data));
        return WindowedValue.of(result, timestampMillis, windows, pane);
      } else {
        notifyElementRead(data.available() + metadata.available());
        return WindowedValue.of(decode(valueCoder, data), timestampMillis, windows, pane);
      }
    }

    private <X> X decode(Coder<X> coder, InputStream input) throws IOException {
      return coder.decode(input, Coder.Context.OUTER);
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
