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
package org.apache.beam.sdk.io.pulsar;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Class for reading and writing from Apache Pulsar. Support is currently incomplete, and there may
 * be bugs; see https://github.com/apache/beam/issues/31078 for more info, and comment in that issue
 * if you run into issues with this IO.
 */
@SuppressWarnings({"rawtypes", "nullness"})
public class PulsarIO {

  /** Static class, prevent instantiation. */
  private PulsarIO() {}

  /**
   * Read from Apache Pulsar. Support is currently incomplete, and there may be bugs; see
   * https://github.com/apache/beam/issues/31078 for more info, and comment in that issue if you run
   * into issues with this IO.
   */
  public static Read read() {
    return new AutoValue_PulsarIO_Read.Builder()
        .setPulsarClient(PulsarIOUtils.PULSAR_CLIENT_SERIALIZABLE_FUNCTION)
        .build();
  }

  @AutoValue
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read extends PTransform<PBegin, PCollection<PulsarMessage>> {

    abstract @Nullable String getClientUrl();

    abstract @Nullable String getAdminUrl();

    abstract @Nullable String getTopic();

    abstract @Nullable Long getStartTimestamp();

    abstract @Nullable Long getEndTimestamp();

    abstract @Nullable MessageId getEndMessageId();

    abstract @Nullable SerializableFunction<Message<byte[]>, Instant> getExtractOutputTimestampFn();

    abstract SerializableFunction<String, PulsarClient> getPulsarClient();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setClientUrl(String url);

      abstract Builder setAdminUrl(String url);

      abstract Builder setTopic(String topic);

      abstract Builder setStartTimestamp(Long timestamp);

      abstract Builder setEndTimestamp(Long timestamp);

      abstract Builder setEndMessageId(MessageId msgId);

      abstract Builder setExtractOutputTimestampFn(
          SerializableFunction<Message<byte[]>, Instant> fn);

      abstract Builder setPulsarClient(SerializableFunction<String, PulsarClient> fn);

      abstract Read build();
    }

    public Read withAdminUrl(String url) {
      return builder().setAdminUrl(url).build();
    }

    public Read withClientUrl(String url) {
      return builder().setClientUrl(url).build();
    }

    public Read withTopic(String topic) {
      return builder().setTopic(topic).build();
    }

    public Read withStartTimestamp(Long timestamp) {
      return builder().setStartTimestamp(timestamp).build();
    }

    public Read withEndTimestamp(Long timestamp) {
      return builder().setEndTimestamp(timestamp).build();
    }

    public Read withEndMessageId(MessageId msgId) {
      return builder().setEndMessageId(msgId).build();
    }

    public Read withExtractOutputTimestampFn(SerializableFunction<Message<byte[]>, Instant> fn) {
      return builder().setExtractOutputTimestampFn(fn).build();
    }

    public Read withPublishTime() {
      return withExtractOutputTimestampFn(ExtractOutputTimestampFn.usePublishTime());
    }

    public Read withProcessingTime() {
      return withExtractOutputTimestampFn(ExtractOutputTimestampFn.useProcessingTime());
    }

    public Read withPulsarClient(SerializableFunction<String, PulsarClient> pulsarClientFn) {
      return builder().setPulsarClient(pulsarClientFn).build();
    }

    @Override
    public PCollection<PulsarMessage> expand(PBegin input) {
      return input
          .apply(
              Create.of(
                  PulsarSourceDescriptor.of(
                      getTopic(),
                      getStartTimestamp(),
                      getEndTimestamp(),
                      getEndMessageId(),
                      getClientUrl(),
                      getAdminUrl())))
          .apply(ParDo.of(new ReadFromPulsarDoFn(this)))
          .setCoder(PulsarMessageCoder.of());
    }
  }

  /**
   * Write to Apache Pulsar. Support is currently incomplete, and there may be bugs; see
   * https://github.com/apache/beam/issues/31078 for more info, and comment in that issue if you run
   * into issues with this IO.
   */
  public static Write write() {
    return new AutoValue_PulsarIO_Write.Builder().build();
  }

  @AutoValue
  @SuppressWarnings({"rawtypes"})
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    abstract @Nullable String getTopic();

    abstract String getClientUrl();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTopic(String topic);

      abstract Builder setClientUrl(String clientUrl);

      abstract Write build();
    }

    public Write withTopic(String topic) {
      return builder().setTopic(topic).build();
    }

    public Write withClientUrl(String clientUrl) {
      return builder().setClientUrl(clientUrl).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteToPulsarDoFn(this)));
      return PDone.in(input.getPipeline());
    }
  }

  static class ExtractOutputTimestampFn {
    public static SerializableFunction<Message<byte[]>, Instant> useProcessingTime() {
      return record -> Instant.now();
    }

    public static SerializableFunction<Message<byte[]>, Instant> usePublishTime() {
      return record -> new Instant(record.getPublishTime());
    }
  }
}
