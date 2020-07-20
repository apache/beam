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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * It contains the message payload, and additional metadata like routing key or attributes. The main
 * reason of this class is that AMQP.BasicProperties doesn't provide a serializable public API.
 */
public class RabbitMqMessage implements Serializable {

  /**
   * Make delivery serializable by cloning all non-serializable values into serializable ones. If it
   * is not possible, initial delivery is returned and error message is logged
   *
   * @param processed
   * @return
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
              String s = new String(bytes, StandardCharsets.UTF_8);
              value = s;
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

  private final @Nullable String routingKey;
  private final byte[] body;
  private final String contentType;
  private final String contentEncoding;
  private final Map<String, Object> headers;
  private final Integer deliveryMode;
  private final Integer priority;
  private final @Nullable String correlationId;
  private final @Nullable String replyTo;
  private final String expiration;
  private final String messageId;
  private final Date timestamp;
  private final @Nullable String type;
  private final @Nullable String userId;
  private final @Nullable String appId;
  private final @Nullable String clusterId;

  public RabbitMqMessage(byte[] body) {
    this.body = body;
    routingKey = "";
    contentType = null;
    contentEncoding = null;
    headers = new HashMap<>();
    deliveryMode = 1;
    priority = 1;
    correlationId = null;
    replyTo = null;
    expiration = null;
    messageId = null;
    timestamp = new Date();
    type = null;
    userId = null;
    appId = null;
    clusterId = null;
  }

  public RabbitMqMessage(String routingKey, GetResponse delivery) {
    this.routingKey = routingKey;
    delivery = serializableDeliveryOf(delivery);
    body = delivery.getBody();
    contentType = delivery.getProps().getContentType();
    contentEncoding = delivery.getProps().getContentEncoding();
    headers = delivery.getProps().getHeaders();
    deliveryMode = delivery.getProps().getDeliveryMode();
    priority = delivery.getProps().getPriority();
    correlationId = delivery.getProps().getCorrelationId();
    replyTo = delivery.getProps().getReplyTo();
    expiration = delivery.getProps().getExpiration();
    messageId = delivery.getProps().getMessageId();
    timestamp = delivery.getProps().getTimestamp();
    type = delivery.getProps().getType();
    userId = delivery.getProps().getUserId();
    appId = delivery.getProps().getAppId();
    clusterId = delivery.getProps().getClusterId();
  }

  public RabbitMqMessage(
      String routingKey,
      byte[] body,
      String contentType,
      String contentEncoding,
      Map<String, Object> headers,
      Integer deliveryMode,
      Integer priority,
      String correlationId,
      String replyTo,
      String expiration,
      String messageId,
      Date timestamp,
      String type,
      String userId,
      String appId,
      String clusterId) {
    this.routingKey = routingKey;
    this.body = body;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.headers = headers;
    this.deliveryMode = deliveryMode;
    this.priority = priority;
    this.correlationId = correlationId;
    this.replyTo = replyTo;
    this.expiration = expiration;
    this.messageId = messageId;
    this.timestamp = timestamp;
    this.type = type;
    this.userId = userId;
    this.appId = appId;
    this.clusterId = clusterId;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public byte[] getBody() {
    return body;
  }

  public String getContentType() {
    return contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }

  public Integer getDeliveryMode() {
    return deliveryMode;
  }

  public Integer getPriority() {
    return priority;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getReplyTo() {
    return replyTo;
  }

  public String getExpiration() {
    return expiration;
  }

  public String getMessageId() {
    return messageId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getType() {
    return type;
  }

  public String getUserId() {
    return userId;
  }

  public String getAppId() {
    return appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public AMQP.BasicProperties createProperties() {
    return new AMQP.BasicProperties()
        .builder()
        .contentType(contentType)
        .contentEncoding(contentEncoding)
        .headers(headers)
        .deliveryMode(deliveryMode)
        .priority(priority)
        .correlationId(correlationId)
        .replyTo(replyTo)
        .expiration(expiration)
        .messageId(messageId)
        .timestamp(timestamp)
        .type(type)
        .userId(userId)
        .appId(appId)
        .clusterId(clusterId)
        .build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        routingKey,
        Arrays.hashCode(body),
        contentType,
        contentEncoding,
        deliveryMode,
        priority,
        correlationId,
        replyTo,
        expiration,
        messageId,
        timestamp,
        type,
        userId,
        appId,
        clusterId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RabbitMqMessage) {
      RabbitMqMessage other = (RabbitMqMessage) obj;
      return Objects.equals(routingKey, other.routingKey)
          && Arrays.equals(body, other.body)
          && Objects.equals(contentType, other.contentType)
          && Objects.equals(contentEncoding, other.contentEncoding)
          && Objects.equals(deliveryMode, other.deliveryMode)
          && Objects.equals(priority, other.priority)
          && Objects.equals(correlationId, other.correlationId)
          && Objects.equals(replyTo, other.replyTo)
          && Objects.equals(expiration, other.expiration)
          && Objects.equals(messageId, other.messageId)
          && Objects.equals(timestamp, other.timestamp)
          && Objects.equals(type, other.type)
          && Objects.equals(userId, other.userId)
          && Objects.equals(appId, other.appId)
          && Objects.equals(clusterId, other.clusterId);
    }
    return false;
  }
}
