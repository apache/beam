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

import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.windmill.Pubsub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class PubsubDynamicSink extends Sink<WindowedValue<PubsubMessage>> {
  private final String timestampLabel;
  private final String idLabel;
  private final StreamingModeExecutionContext context;

  PubsubDynamicSink(String timestampLabel, String idLabel, StreamingModeExecutionContext context) {
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.context = context;
  }

  /** A {@link SinkFactory.Registrar} for pubsub sinks. */
  @AutoService(SinkFactory.Registrar.class)
  public static class Registrar implements SinkFactory.Registrar {

    @Override
    public Map<String, SinkFactory> factories() {
      PubsubDynamicSink.Factory factory = new Factory();
      return ImmutableMap.of(
          "PubsubDynamicSink",
          factory,
          "org.apache.beam.runners.dataflow.worker.PubsubDynamicSink",
          factory);
    }
  }

  static class Factory implements SinkFactory {
    @Override
    public PubsubDynamicSink create(
        CloudObject spec,
        Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      String timestampLabel = getString(spec, PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "");
      String idLabel = getString(spec, PropertyNames.PUBSUB_ID_ATTRIBUTE, "");

      return new PubsubDynamicSink(
          timestampLabel,
          idLabel,
          (StreamingModeExecutionContext) checkArgumentNotNull(executionContext));
    }
  }

  @Override
  public Sink.SinkWriter<WindowedValue<PubsubMessage>> writer() {
    return new PubsubDynamicSink.PubsubWriter();
  }

  class PubsubWriter implements Sink.SinkWriter<WindowedValue<PubsubMessage>> {
    private final Map<String, Windmill.PubSubMessageBundle.Builder> outputBuilders;
    private final ByteStringOutputStream stream; // Kept across adds for buffer reuse.

    PubsubWriter() {
      outputBuilders = Maps.newHashMap();
      stream = new ByteStringOutputStream();
    }

    public ByteString getDataFromMessage(PubsubMessage formatted, ByteStringOutputStream stream)
        throws IOException {
      Pubsub.PubsubMessage.Builder pubsubMessageBuilder =
          Pubsub.PubsubMessage.newBuilder().setData(ByteString.copyFrom(formatted.getPayload()));
      Map<String, String> attributeMap = formatted.getAttributeMap();
      if (attributeMap != null) {
        pubsubMessageBuilder.putAllAttributes(attributeMap);
      }
      pubsubMessageBuilder.build().writeTo(stream);
      return stream.toByteStringAndReset();
    }

    public void close(Windmill.PubSubMessageBundle.Builder outputBuilder) throws IOException {
      context.getOutputBuilder().addPubsubMessages(outputBuilder);
      outputBuilder.clear();
    }

    @Override
    public long add(WindowedValue<PubsubMessage> data) throws IOException {
      String dataTopic =
          checkArgumentNotNull(
              data.getValue().getTopic(), "No topic set for message when using dynamic topics.");
      Preconditions.checkArgument(
          !dataTopic.isEmpty(), "No topic set for message when using dynamic topics.");
      ByteString byteString = getDataFromMessage(data.getValue(), stream);
      Windmill.PubSubMessageBundle.Builder builder =
          outputBuilders.computeIfAbsent(
              dataTopic,
              topic ->
                  context
                      .getOutputBuilder()
                      .addPubsubMessagesBuilder()
                      .setTopic(topic)
                      .setTimestampLabel(timestampLabel)
                      .setIdLabel(idLabel)
                      .setWithAttributes(true));
      builder.addMessages(
          Windmill.Message.newBuilder()
              .setData(byteString)
              .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(data.getTimestamp()))
              .build());
      return byteString.size();
    }

    @Override
    public void close() throws IOException {
      outputBuilders.clear();
    }

    @Override
    public void abort() throws IOException {
      close();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
