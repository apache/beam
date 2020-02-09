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

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** A Reader that receives elements from Pubsub, via a Windmill server. */
class PubsubReader extends NativeReader<WindowedValue<PubsubMessage>> {
  // The parsing mode. Whether the response is a serialized PubsubMessage or a raw byte string for
  // the data field.
  enum Mode {
    PUBSUB_MESSAGE, RAW_BYTES
  }
  private final StreamingModeExecutionContext context;
  private final Mode mode;

  PubsubReader(
      StreamingModeExecutionContext context,
      Mode mode) {
    this.context = context;
    this.mode = mode;
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
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      byte[] attributesFnBytes =
          getBytes(cloudSourceSpec, PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, null);
      Mode mode = Mode.RAW_BYTES;
      if (attributesFnBytes != null && attributesFnBytes.length > 0) {
        mode = Mode.PUBSUB_MESSAGE ;
      }
      return new PubsubReader((StreamingModeExecutionContext) executionContext, mode);
    }
  }

  @Override
  public NativeReaderIterator<WindowedValue<PubsubMessage>> iterator() throws IOException {
    return new PubsubReaderIterator(context.getWork());
  }

  class PubsubReaderIterator extends WindmillReaderIteratorBase<PubsubMessage> {
    protected PubsubReaderIterator(Windmill.WorkItem work) {
      super(work);
    }

    @Override
    protected WindowedValue<PubsubMessage> decodeMessage(Windmill.Message message) throws IOException {
      PubsubMessage value;
      InputStream data = message.getData().newInput();
      notifyElementRead(data.available());
      switch (mode) {
        case RAW_BYTES:
          value = PubsubMessage.newBuilder().setData(ByteString.readFrom(data)).build();
          break;
        case PUBSUB_MESSAGE:
          value = PubsubMessage.parseFrom(data);
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + mode);
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
