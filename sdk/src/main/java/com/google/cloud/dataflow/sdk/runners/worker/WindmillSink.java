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

import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class WindmillSink<T> extends Sink<WindowedValue<T>> {
  private WindmillStreamWriter writer;
  private final Coder<T> valueCoder;
  private final Coder windowsCoder;
  private StreamingModeExecutionContext context;

  WindmillSink(String destinationName,
               Coder<WindowedValue<T>> coder,
               StreamingModeExecutionContext context) {
    this.writer = new WindmillStreamWriter(destinationName);
    FullWindowedValueCoder<T> inputCoder = (FullWindowedValueCoder<T>) coder;
    this.valueCoder = inputCoder.getValueCoder();
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.context = context;
  }

  public static <T> WindmillSink<T> create(PipelineOptions options,
                                           CloudObject spec,
                                           Coder<WindowedValue<T>> coder,
                                           ExecutionContext context) throws Exception {
    return new WindmillSink<>(getString(spec, "stream_id"), coder,
        (StreamingModeExecutionContext) context);
  }

  @Override
  public SinkWriter<WindowedValue<T>> writer() {
    return writer;
  }

  class WindmillStreamWriter implements SinkWriter<WindowedValue<T>> {
    private Map<ByteString, Windmill.KeyedMessageBundle.Builder> productionMap;
    private final String destinationName;

    private WindmillStreamWriter(String destinationName) {
      this.destinationName = destinationName;
      productionMap = new HashMap<ByteString, Windmill.KeyedMessageBundle.Builder>();
    }

    private <S> ByteString encode(Coder<S> coder, S object) throws IOException {
      ByteString.Output stream = ByteString.newOutput();
      coder.encode(object, stream, Coder.Context.OUTER);
      return stream.toByteString();
    }

    @Override
    public long add(WindowedValue<T> data) throws IOException {
      ByteString key, value;
      ByteString windows = encode(windowsCoder, data.getWindows());
      if (valueCoder instanceof KvCoder) {
        KvCoder kvCoder = (KvCoder) valueCoder;
        KV kv = (KV) data.getValue();
        key = encode(kvCoder.getKeyCoder(), kv.getKey());
        value = encode(kvCoder.getValueCoder(), kv.getValue());
      } else {
        key = context.getSerializedKey();
        value = encode(valueCoder, data.getValue());
      }
      Windmill.KeyedMessageBundle.Builder keyedOutput = productionMap.get(key);
      if (keyedOutput == null) {
        keyedOutput = Windmill.KeyedMessageBundle.newBuilder().setKey(key);
        productionMap.put(key, keyedOutput);
      }

      long timestampMicros = TimeUnit.MILLISECONDS.toMicros(data.getTimestamp().getMillis());
      Windmill.Message.Builder builder = Windmill.Message.newBuilder()
          .setTimestamp(timestampMicros)
          .setData(value)
          .setMetadata(windows);
      keyedOutput.addMessages(builder.build());
      return key.size() + value.size() + windows.size();
    }

    @Override
    public void close() throws IOException {
      Windmill.OutputMessageBundle.Builder outputBuilder =
          Windmill.OutputMessageBundle.newBuilder().setDestinationStreamId(destinationName);

      for (Windmill.KeyedMessageBundle.Builder keyedOutput : productionMap.values()) {
        outputBuilder.addBundles(keyedOutput.build());
      }
      if (outputBuilder.getBundlesCount() > 0) {
        context.getOutputBuilder().addOutputMessages(outputBuilder.build());
      }
      productionMap.clear();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
