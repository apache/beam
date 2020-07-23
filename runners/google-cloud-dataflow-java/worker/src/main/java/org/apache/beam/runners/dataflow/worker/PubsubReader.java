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

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Pubsub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A Reader that receives elements from Pubsub, via a Windmill server. */
class PubsubReader<T> extends NativeReader<WindowedValue<T>> {
  private final Coder<T> coder;
  private final StreamingModeExecutionContext context;
  // Function used to parse Windmill data.
  // If non-null, data from Windmill is expected to be a PubsubMessage protobuf.
  private final SimpleFunction<PubsubMessage, T> parseFn;

  PubsubReader(
      Coder<WindowedValue<T>> coder,
      StreamingModeExecutionContext context,
      SimpleFunction<PubsubMessage, T> parseFn) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    WindowedValueCoder<T> windowedCoder = (WindowedValueCoder) coder;
    this.coder = windowedCoder.getValueCoder();
    this.context = context;
    this.parseFn = parseFn;
  }

  /** A {@link ReaderFactory.Registrar} for pubsub sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      Factory factory = new Factory();
      return ImmutableMap.of(
          "PubsubReader", factory, "org.apache.beam.runners.dataflow.worker.PubsubSource", factory);
    }
  }

  static class Factory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject cloudSourceSpec,
        Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      coder = checkArgumentNotNull(coder);
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<Object>> typedCoder = (Coder<WindowedValue<Object>>) coder;
      SimpleFunction<PubsubMessage, Object> parseFn = null;
      byte[] attributesFnBytes =
          getBytes(cloudSourceSpec, PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, null);
      // If attributesFnBytes is set, Pubsub data will be in PubsubMessage protobuf format. The
      // array should contain a serialized Java function that accepts a PubsubMessage object. The
      // special case of a zero-length array allows pass-through of the raw protobuf.
      if (attributesFnBytes != null && attributesFnBytes.length > 0) {
        parseFn =
            (SimpleFunction<PubsubMessage, Object>)
                SerializableUtils.deserializeFromByteArray(attributesFnBytes, "serialized fn info");
      }
      return new PubsubReader<>(
          typedCoder, (StreamingModeExecutionContext) executionContext, parseFn);
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
      T value;
      InputStream data = message.getData().newInput();
      notifyElementRead(data.available());
      if (parseFn != null) {
        Pubsub.PubsubMessage pubsubMessage = Pubsub.PubsubMessage.parseFrom(data);
        value =
            parseFn.apply(
                new PubsubMessage(
                    pubsubMessage.getData().toByteArray(),
                    pubsubMessage.getAttributesMap(),
                    pubsubMessage.getMessageId()));
      } else {
        value = coder.decode(data, Coder.Context.OUTER);
      }
      return WindowedValue.timestampedValueInGlobalWindow(
          value, WindmillTimeUtils.windmillToHarnessTimestamp(message.getTimestamp()));
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
