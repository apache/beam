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
package org.apache.beam.sdk.io.rabbitmq;

import com.google.auto.value.AutoValue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Serializable wrapper around rabbitmq's {@link GetResponse}.
 *
 * <p>The main purpose of this class is to provide a serializable public API for
 * AMQP.BasicProperties, which lacks one of its own. Secondarily, introducing the builder pattern
 * with AutoValue makes this a bit easier to work with.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class RabbitMqMessage implements Serializable {
  public static final int DELIVERY_MODE_NON_DURABLE = 1;
  public static final int DELIVERY_MODE_DURABLE = 2;

  public static final int PRIORITY_LOWEST = 1;

  /**
   * Defaults to the empty string. When publishing messages to an exchange type other than fanout,
   * this should be specified.
   */
  public abstract String routingKey();

  @SuppressWarnings("mutable")
  public abstract byte[] body();

  @Nullable
  public abstract String contentType();

  @Nullable
  public abstract String contentEncoding();

  public abstract Map<String, Object> headers();

  /**
   * Defaults to "non-durable".
   *
   * @see #DELIVERY_MODE_DURABLE
   * @see #DELIVERY_MODE_NON_DURABLE
   */
  @Nullable
  public abstract Integer deliveryMode();

  /**
   * Defaults to "lowest priority".
   *
   * @see #PRIORITY_LOWEST
   */
  @Nullable
  public abstract Integer priority();

  @Nullable
  public abstract String correlationId();

  @Nullable
  public abstract String replyTo();

  @Nullable
  public abstract String expiration();

  @Nullable
  public abstract String messageId();

  @Nullable
  public abstract Date timestamp();

  @Nullable
  public abstract String type();

  @Nullable
  public abstract String userId();

  @Nullable
  public abstract String appId();

  @Nullable
  public abstract String clusterId();

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object other);

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_RabbitMqMessage.Builder()
        .setDeliveryMode(DELIVERY_MODE_NON_DURABLE)
        .setMessageId(UUID.randomUUID().toString())
        .setPriority(PRIORITY_LOWEST)
        .setRoutingKey("")
        .setTimestamp(new Date())
        .setHeaders(Collections.emptyMap());
  }

  public static RabbitMqMessage fromGetResponse(GetResponse delivery) {
    delivery = serializableDeliveryOf(delivery);
    BasicProperties props = delivery.getProps();
    Map<String, Object> headers = Collections.emptyMap();
    Map<String, Object> incomingHeaders = props.getHeaders();
    if (null != incomingHeaders) {
      headers = Collections.unmodifiableMap(new HashMap<>(incomingHeaders));
    }

    return builder()
        .setRoutingKey(
            Optional.ofNullable(delivery.getEnvelope()).map(Envelope::getRoutingKey).orElse(""))
        .setBody(delivery.getBody())
        .setContentType(props.getContentType())
        .setContentEncoding(props.getContentEncoding())
        .setHeaders(headers)
        .setDeliveryMode(props.getDeliveryMode())
        .setPriority(props.getPriority())
        .setCorrelationId(props.getCorrelationId())
        .setReplyTo(props.getReplyTo())
        .setExpiration(props.getExpiration())
        .setMessageId(props.getMessageId())
        .setTimestamp(props.getTimestamp())
        .setType(props.getType())
        .setUserId(props.getUserId())
        .setAppId(props.getAppId())
        .setClusterId(props.getClusterId())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRoutingKey(String routingKey);

    public abstract Builder setBody(byte[] body);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setContentEncoding(String contentEncoding);

    public abstract Builder setHeaders(Map<String, Object> headers);

    public abstract Builder setDeliveryMode(Integer deliveryMode);

    public abstract Builder setPriority(Integer priority);

    public abstract Builder setCorrelationId(String correlationId);

    public abstract Builder setReplyTo(String replyTo);

    public abstract Builder setExpiration(String expiration);

    public abstract Builder setMessageId(String messageId);

    abstract Date timestamp();

    public abstract Builder setTimestamp(Date timestamp);

    public abstract Builder setType(String type);

    public abstract Builder setUserId(String userId);

    public abstract Builder setAppId(String appId);

    abstract Builder setClusterId(String clusterId);

    abstract RabbitMqMessage autoBuild();

    public RabbitMqMessage build() {
      Date timestamp = timestamp();
      if (timestamp != null) {
        // amqp timestamp property is unix timestamp ('seconds since epoch'), so
        // truncating to latest second
        setTimestamp(
            new Date(timestamp.toInstant().truncatedTo(ChronoUnit.SECONDS).toEpochMilli()));
      }
      return autoBuild();
    }
  }

  /**
   * Make delivery serializable by cloning all non-serializable values into serializable ones. If it
   * is not possible, initial delivery is returned and error message is logged.
   *
   * @param processed a direct response from amqp
   * @return an instance of GetResponse where headers have been 'cast' or otherwise normalized to
   *     esrializable values
   * @throws UnsupportedOperationException if some values could not be made serializable
   */
  private static GetResponse serializableDeliveryOf(GetResponse processed) {
    // All content of envelope is serializable, so no problem there
    Envelope envelope = processed.getEnvelope();
    // in basicproperties, there may be LongString, which are *not* serializable
    BasicProperties properties = processed.getProps();
    BasicProperties nextProperties =
        new BasicProperties.Builder()
            .appId(properties.getAppId())
            .clusterId(properties.getClusterId())
            .contentEncoding(properties.getContentEncoding())
            .contentType(properties.getContentType())
            .correlationId(properties.getCorrelationId())
            .deliveryMode(properties.getDeliveryMode())
            .expiration(properties.getExpiration())
            .headers(serializableHeaders(properties.getHeaders()))
            .messageId(properties.getMessageId())
            .priority(properties.getPriority())
            .replyTo(properties.getReplyTo())
            .timestamp(properties.getTimestamp())
            .type(properties.getType())
            .userId(properties.getUserId())
            .build();
    return new GetResponse(
        envelope, nextProperties, processed.getBody(), processed.getMessageCount());
  }

  private static Map<String, Object> serializableHeaders(Map<String, Object> headers) {
    Map<String, Object> returned = new HashMap<>();
    if (headers != null) {
      for (Map.Entry<String, Object> h : headers.entrySet()) {
        Object value = h.getValue();
        if (!(value instanceof Serializable)) {
          try {
            if (value instanceof LongString) {
              LongString longString = (LongString) value;
              byte[] bytes = longString.getBytes();
              value = new String(bytes, StandardCharsets.UTF_8);
            } else {
              throw new RuntimeException(String.format("no transformation defined for %s", value));
            }
          } catch (Throwable t) {
            throw new UnsupportedOperationException(
                String.format(
                    "can't make unserializable value %s a serializable value (which is mandatory for Apache Beam dataflow implementation)",
                    value),
                t);
          }
        }
        returned.put(h.getKey(), value);
      }
    }
    return returned;
  }

  public AMQP.BasicProperties createProperties() {
    return new AMQP.BasicProperties()
        .builder()
        .contentType(contentType())
        .contentEncoding(contentEncoding())
        .headers(headers())
        .deliveryMode(deliveryMode())
        .priority(priority())
        .correlationId(correlationId())
        .replyTo(replyTo())
        .expiration(expiration())
        .messageId(messageId())
        .timestamp(timestamp())
        .type(type())
        .userId(userId())
        .appId(appId())
        .clusterId(clusterId())
        .build();
  }
}
