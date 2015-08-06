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

import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A sink that writes to Pubsub, via a Windmill server.
 *
 * @param <T> the type of the elements written to the sink
 */
class PubsubSink<T> extends Sink<WindowedValue<T>> {
  private final String topic;
  private final String timestampLabel;
  private final String idLabel;
  private final Coder<WindowedValue<T>> coder;
  private final StreamingModeExecutionContext context;

  PubsubSink(
      String topic,
      String timestampLabel,
      String idLabel,
      Coder<WindowedValue<T>> coder,
      StreamingModeExecutionContext context) {
    this.topic = topic;
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.coder = coder;
    this.context = context;
  }

  @SuppressWarnings("unused")
  public static <T> PubsubSink<T> create(PipelineOptions options,
                                         CloudObject spec,
                                         Coder<WindowedValue<T>> coder,
                                         ExecutionContext context,
                                         CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    String topic = getString(spec, "pubsub_topic");
    String timestampLabel = getString(spec, "pubsub_timestamp_label", "");
    String idLabel = getString(spec, "pubsub_id_label", "");
    return new PubsubSink<>(
        topic, timestampLabel, idLabel, coder, (StreamingModeExecutionContext) context);
  }

  @Override
  public SinkWriter<WindowedValue<T>> writer() {
    return new PubsubWriter(topic);
  }

  /** The SinkWriter for a PubsubSink. */
  class PubsubWriter implements SinkWriter<WindowedValue<T>> {
    private Windmill.PubSubMessageBundle.Builder outputBuilder;

    private PubsubWriter(String topic) {
      outputBuilder =
          Windmill.PubSubMessageBundle.newBuilder()
              .setTopic(topic)
              .setTimestampLabel(timestampLabel)
              .setIdLabel(idLabel);
    }

    private <T> ByteString encode(Coder<T> coder, T object) throws IOException {
      ByteString.Output stream = ByteString.newOutput();
      coder.encode(object, stream, Coder.Context.OUTER);
      return stream.toByteString();
    }

    @Override
    public long add(WindowedValue<T> data) throws IOException {
      ByteString byteString = encode(coder, data);

      long timestampMicros = TimeUnit.MILLISECONDS.toMicros(data.getTimestamp().getMillis());
      outputBuilder.addMessages(
          Windmill.Message.newBuilder()
          .setData(byteString)
          .setTimestamp(timestampMicros)
          .build());

      return byteString.size();
    }

    @Override
    public void close() throws IOException {
      Windmill.PubSubMessageBundle pubsubMessages = outputBuilder.build();
      if (pubsubMessages.getMessagesCount() > 0) {
        context.getOutputBuilder().addPubsubMessages(pubsubMessages);
      }
      outputBuilder.clear();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
