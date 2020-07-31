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
import static org.apache.beam.runners.dataflow.util.Structs.getString;

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
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A sink that writes to Pubsub, via a Windmill server.
 *
 * @param <T> the type of the elements written to the sink
 */
class PubsubSink<T> extends Sink<WindowedValue<T>> {
  private final String topic;
  private final String timestampLabel;
  private final String idLabel;
  private final Coder<T> coder;
  private final StreamingModeExecutionContext context;
  // Function used to convert PCollection elements to PubsubMessage objects.
  private final SimpleFunction<T, PubsubMessage> formatFn;
  private final boolean withAttributes;

  PubsubSink(
      String topic,
      String timestampLabel,
      String idLabel,
      Coder<WindowedValue<T>> coder,
      SimpleFunction<T, PubsubMessage> formatFn,
      boolean withAttributes,
      StreamingModeExecutionContext context) {
    this.topic = topic;
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    @SuppressWarnings({"unchecked", "rawtypes"})
    WindowedValueCoder<T> windowedCoder = (WindowedValueCoder) coder;
    this.coder = windowedCoder.getValueCoder();
    this.withAttributes = withAttributes;
    this.formatFn = formatFn;
    this.context = context;
  }

  /** A {@link SinkFactory.Registrar} for pubsub sinks. */
  @AutoService(SinkFactory.Registrar.class)
  public static class Registrar implements SinkFactory.Registrar {

    @Override
    public Map<String, SinkFactory> factories() {
      Factory factory = new Factory();
      return ImmutableMap.of(
          "PubsubSink", factory, "org.apache.beam.runners.dataflow.worker.PubsubSink", factory);
    }
  }

  public static class Factory implements SinkFactory {
    @Override
    public PubsubSink<?> create(
        CloudObject spec,
        Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      String topic = getString(spec, PropertyNames.PUBSUB_TOPIC);
      String timestampLabel = getString(spec, PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "");
      String idLabel = getString(spec, PropertyNames.PUBSUB_ID_ATTRIBUTE, "");

      @SuppressWarnings("unchecked")
      Coder<WindowedValue<Object>> typedCoder = (Coder<WindowedValue<Object>>) coder;
      SimpleFunction<Object, PubsubMessage> formatFn = null;
      byte[] attributesFnBytes =
          getBytes(spec, PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, null);
      // If attributesFnBytes is set, the array should contain a serialized Java function that
      // outputs a PubsubMessage Java object. The special case of a zero-length array allows
      // passing PCollections of raw PubsubMessage protobufs directly to Windmill.
      boolean withAttributes = false;
      if (attributesFnBytes != null) {
        withAttributes = true;
        if (attributesFnBytes.length > 0) {
          formatFn =
              (SimpleFunction<Object, PubsubMessage>)
                  SerializableUtils.deserializeFromByteArray(
                      attributesFnBytes, "serialized fn info");
        }
      }

      return new PubsubSink<>(
          topic,
          timestampLabel,
          idLabel,
          typedCoder,
          formatFn,
          withAttributes,
          (StreamingModeExecutionContext) executionContext);
    }
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
              .setIdLabel(idLabel)
              .setWithAttributes(withAttributes);
    }

    @Override
    public long add(WindowedValue<T> data) throws IOException {
      ByteString byteString = null;
      if (formatFn != null) {
        PubsubMessage formatted = formatFn.apply(data.getValue());
        Pubsub.PubsubMessage.Builder pubsubMessageBuilder =
            Pubsub.PubsubMessage.newBuilder().setData(ByteString.copyFrom(formatted.getPayload()));
        if (formatted.getAttributeMap() != null) {
          pubsubMessageBuilder.putAllAttributes(formatted.getAttributeMap());
        }
        ByteString.Output output = ByteString.newOutput();
        pubsubMessageBuilder.build().writeTo(output);
        byteString = output.toByteString();
      } else {
        ByteString.Output stream = ByteString.newOutput();
        coder.encode(data.getValue(), stream, Coder.Context.OUTER);
        byteString = stream.toByteString();
      }

      outputBuilder.addMessages(
          Windmill.Message.newBuilder()
              .setData(byteString)
              .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(data.getTimestamp()))
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
