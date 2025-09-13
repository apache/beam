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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * IO connector for reading and writing from Apache Pulsar. Support is currently experimental, and
 * there may be bugs or performance issues; see https://github.com/apache/beam/issues/31078 for more
 * info, and comment in that issue if you run into issues with this IO.
 */
@SuppressWarnings({"rawtypes", "nullness"})
public class PulsarIO {

  /** Static class, prevent instantiation. */
  private PulsarIO() {}

  /**
   * Read from Apache Pulsar.
   *
   * <p>Support is currently experimental, and there may be bugs or performance issues; see
   * https://github.com/apache/beam/issues/31078 for more info, and comment in that issue if you run
   * into issues with this IO.
   *
   * @param fn a mapping function converting {@link Message} that returned by Pulsar client to a
   *     custom type understood by Beam.
   */
  public static <T> Read<T> read(SerializableFunction<Message, T> fn) {
    return new AutoValue_PulsarIO_Read.Builder()
        .setOutputFn(fn)
        .setConsumerPollingTimeout(PulsarIOUtils.DEFAULT_CONSUMER_POLLING_TIMEOUT)
        .setTimestampType(ReadTimestampType.PUBLISH_TIME)
        .build();
  }

  /**
   * The same as {@link PulsarIO#read(SerializableFunction)}, but returns {@link
   * PCollection<PulsarMessage>}.
   */
  public static Read<PulsarMessage> read() {
    return new AutoValue_PulsarIO_Read.Builder()
        .setOutputFn(PULSAR_MESSAGE_SERIALIZABLE_FUNCTION)
        .setConsumerPollingTimeout(PulsarIOUtils.DEFAULT_CONSUMER_POLLING_TIMEOUT)
        .setTimestampType(ReadTimestampType.PUBLISH_TIME)
        .build();
  }

  private static final SerializableFunction<Message<byte[]>, PulsarMessage>
      PULSAR_MESSAGE_SERIALIZABLE_FUNCTION = PulsarMessage::create;

  @AutoValue
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable String getClientUrl();

    abstract @Nullable String getAdminUrl();

    abstract @Nullable String getTopic();

    abstract @Nullable Long getStartTimestamp();

    abstract @Nullable Long getEndTimestamp();

    abstract @Nullable MessageId getEndMessageId();

    abstract ReadTimestampType getTimestampType();

    abstract long getConsumerPollingTimeout();

    abstract @Nullable SerializableFunction<String, PulsarClient> getPulsarClient();

    abstract @Nullable SerializableFunction<String, PulsarAdmin> getPulsarAdmin();

    abstract SerializableFunction<Message<?>, T> getOutputFn();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setClientUrl(String url);

      abstract Builder<T> setAdminUrl(String url);

      abstract Builder<T> setTopic(String topic);

      abstract Builder<T> setStartTimestamp(Long timestamp);

      abstract Builder<T> setEndTimestamp(Long timestamp);

      abstract Builder<T> setEndMessageId(MessageId msgId);

      abstract Builder<T> setTimestampType(ReadTimestampType timestampType);

      abstract Builder<T> setConsumerPollingTimeout(long timeOutMs);

      abstract Builder<T> setPulsarClient(SerializableFunction<String, PulsarClient> fn);

      abstract Builder<T> setPulsarAdmin(SerializableFunction<String, PulsarAdmin> fn);

      @SuppressWarnings("getvsset") // outputFn determines generic type
      abstract Builder<T> setOutputFn(SerializableFunction<Message<?>, T> fn);

      abstract Read<T> build();
    }

    /**
     * Configure Pulsar admin url.
     *
     * <p>Admin client is used to approximate backlogs. This setting is optional.
     *
     * @param url admin url. For example, {@code "http://localhost:8080"}.
     */
    public Read<T> withAdminUrl(String url) {
      return builder().setAdminUrl(url).build();
    }

    /**
     * Configure Pulsar client url. {@code "pulsar://localhost:6650"}.
     *
     * @param url client url. For example,
     */
    public Read<T> withClientUrl(String url) {
      return builder().setClientUrl(url).build();
    }

    public Read<T> withTopic(String topic) {
      return builder().setTopic(topic).build();
    }

    public Read<T> withStartTimestamp(Long timestamp) {
      return builder().setStartTimestamp(timestamp).build();
    }

    public Read<T> withEndTimestamp(Long timestamp) {
      return builder().setEndTimestamp(timestamp).build();
    }

    public Read<T> withEndMessageId(MessageId msgId) {
      return builder().setEndMessageId(msgId).build();
    }

    /** Set elements timestamped by {@link Message#getPublishTime()}. It is the default. */
    public Read<T> withPublishTime() {
      return builder().setTimestampType(ReadTimestampType.PUBLISH_TIME).build();
    }

    /** Set elements timestamped to the moment it get processed. */
    public Read<T> withProcessingTime() {
      return builder().setTimestampType(ReadTimestampType.PROCESSING_TIME).build();
    }

    /**
     * Sets the timeout time in seconds for Pulsar consumer polling request. A lower timeout
     * optimizes for latency. Increase the timeout if the consumer is not fetching any records. The
     * default is 2 seconds.
     */
    public Read<T> withConsumerPollingTimeout(long duration) {
      checkState(duration > 0, "Consumer polling timeout must be greater than 0.");
      return builder().setConsumerPollingTimeout(duration).build();
    }

    public Read<T> withPulsarClient(SerializableFunction<String, PulsarClient> pulsarClientFn) {
      return builder().setPulsarClient(pulsarClientFn).build();
    }

    public Read<T> withPulsarAdmin(SerializableFunction<String, PulsarAdmin> pulsarAdminFn) {
      return builder().setPulsarAdmin(pulsarAdminFn).build();
    }

    @SuppressWarnings("unchecked") // for PulsarMessage
    @Override
    public PCollection<T> expand(PBegin input) {
      PCollection<T> pcoll =
          input
              .apply(
                  Create.of(
                      PulsarSourceDescriptor.of(
                          getTopic(), getStartTimestamp(), getEndTimestamp(), getEndMessageId())))
              .apply(ParDo.of(new NaiveReadFromPulsarDoFn<>(this)));
      if (getOutputFn().equals(PULSAR_MESSAGE_SERIALIZABLE_FUNCTION)) {
        // register coder for default implementation of read
        return pcoll.setTypeDescriptor((TypeDescriptor<T>) TypeDescriptor.of(PulsarMessage.class));
      }
      return pcoll;
    }
  }

  enum ReadTimestampType {
    PROCESSING_TIME,
    PUBLISH_TIME,
  }

  /**
   * Write to Apache Pulsar. Support is currently experimental, and there may be bugs or performance
   * issues; see https://github.com/apache/beam/issues/31078 for more info, and comment in that
   * issue if you run into issues with this IO.
   */
  public static Write write() {
    return new AutoValue_PulsarIO_Write.Builder()
        .setPulsarClient(PulsarIOUtils.PULSAR_CLIENT_SERIALIZABLE_FUNCTION)
        .build();
  }

  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    abstract @Nullable String getTopic();

    abstract @Nullable String getClientUrl();

    abstract SerializableFunction<String, PulsarClient> getPulsarClient();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTopic(String topic);

      abstract Builder setClientUrl(String clientUrl);

      abstract Builder setPulsarClient(SerializableFunction<String, PulsarClient> fn);

      abstract Write build();
    }

    public Write withTopic(String topic) {
      return builder().setTopic(topic).build();
    }

    public Write withClientUrl(String clientUrl) {
      return builder().setClientUrl(clientUrl).build();
    }

    public Write withPulsarClient(SerializableFunction<String, PulsarClient> pulsarClientFn) {
      return builder().setPulsarClient(pulsarClientFn).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteToPulsarDoFn(this)));
      return PDone.in(input.getPipeline());
    }
  }
}
