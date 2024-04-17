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
package org.apache.beam.sdk.io.solace.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.api.client.http.HttpRequestFactory;
import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.JCSMPFactory;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class BasicAuthSempClient implements SempClient {
  private static final Logger LOG = LoggerFactory.getLogger(BasicAuthSempClient.class);
  private final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final SempBasicAuthClientExecutor sempBasicAuthClientExecutor;

  public BasicAuthSempClient(
      String host,
      String username,
      String password,
      String vpnName,
      SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier) {
    sempBasicAuthClientExecutor =
        new SempBasicAuthClientExecutor(
            host, username, password, vpnName, httpRequestFactorySupplier.get());
  }

  @Override
  public boolean isQueueNonExclusive(String queueName) throws IOException {
    LOG.info("SolaceIO.Read: SempOperations: query SEMP if queue {} is nonExclusive", queueName);
    BrokerResponse response = sempBasicAuthClientExecutor.getQueueResponse(queueName);
    if (response.content == null) {
      throw new IOException("SolaceIO: response from SEMP is empty!");
    }
    Queue q = mapJsonToClass(response.content, Queue.class);
    return q.data().accessType().equals("non-exclusive");
  }

  @Override
  public com.solacesystems.jcsmp.Queue createQueueForTopic(String queueName, String topicName)
      throws IOException {
    createQueue(queueName);
    createSubscription(queueName, topicName);
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  @Override
  public long getBacklogBytes(String queueName) throws IOException {
    BrokerResponse response = sempBasicAuthClientExecutor.getQueueResponse(queueName);
    if (response.content == null) {
      throw new IOException("SolaceIO: response from SEMP is empty!");
    }
    Queue q = mapJsonToClass(response.content, Queue.class);
    return q.data().msgSpoolUsage();
  }

  private void createQueue(String queueName) throws IOException {
    LOG.info("SolaceIO.Read: Creating new queue {}.", queueName);
    sempBasicAuthClientExecutor.createQueueResponse(queueName);
  }

  private void createSubscription(String queueName, String topicName) throws IOException {
    LOG.info("SolaceIO.Read: Creating new subscription {} for topic {}.", queueName, topicName);
    sempBasicAuthClientExecutor.createSubscriptionResponse(queueName, topicName);
  }

  private <T> T mapJsonToClass(String content, Class<T> mapSuccessToClass)
      throws JsonProcessingException {
    return objectMapper.readValue(content, mapSuccessToClass);
  }

  @AutoValue
  @JsonSerialize(as = Queue.class)
  @JsonDeserialize(builder = AutoValue_BasicAuthSempClient_Queue.Builder.class)
  abstract static class Queue {

    public abstract QueueData data();

    public static Builder builder() {
      return new AutoValue_BasicAuthSempClient_Queue.Builder();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    abstract static class Builder {

      public abstract Builder setData(QueueData queueData);

      public abstract Queue build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_BasicAuthSempClient_QueueData.Builder.class)
  abstract static class QueueData {
    public abstract String accessType();

    public abstract long msgSpoolUsage();

    public static Builder builder() {
      return new AutoValue_BasicAuthSempClient_QueueData.Builder();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    abstract static class Builder {

      public abstract Builder setAccessType(String accessType);

      public abstract Builder setMsgSpoolUsage(long msgSpoolUsage);

      public abstract QueueData build();
    }
  }

  @AutoValue
  @JsonSerialize(as = ErrorMessage.class)
  @JsonDeserialize(builder = AutoValue_BasicAuthSempClient_ErrorMessage.Builder.class)
  abstract static class ErrorMessage {

    public abstract ErrorMessageMeta meta();

    public static Builder builder() {
      return new AutoValue_BasicAuthSempClient_ErrorMessage.Builder();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    abstract static class Builder {

      public abstract Builder setMeta(ErrorMessageMeta errorMessageMeta);

      public abstract ErrorMessage build();
    }

    @AutoValue
    @JsonSerialize(as = ErrorMessageMeta.class)
    @JsonDeserialize(
        builder = AutoValue_BasicAuthSempClient_ErrorMessage_ErrorMessageMeta.Builder.class)
    abstract static class ErrorMessageMeta {
      public abstract ErrorMessageDetails error();

      public static Builder builder() {
        return new AutoValue_BasicAuthSempClient_ErrorMessage_ErrorMessageMeta.Builder();
      }

      public abstract Builder toBuilder();

      @AutoValue.Builder
      @JsonPOJOBuilder(withPrefix = "set")
      abstract static class Builder {

        public abstract Builder setError(ErrorMessageDetails errorMessageDetails);

        public abstract ErrorMessageMeta build();
      }
    }

    @AutoValue
    @JsonSerialize(as = ErrorMessageDetails.class)
    @JsonDeserialize(
        builder = AutoValue_BasicAuthSempClient_ErrorMessage_ErrorMessageDetails.Builder.class)
    abstract static class ErrorMessageDetails {
      public abstract Integer code();

      public abstract String description();

      public abstract String status();

      public static Builder builder() {
        return new AutoValue_BasicAuthSempClient_ErrorMessage_ErrorMessageDetails.Builder();
      }

      public abstract Builder toBuilder();

      @AutoValue.Builder
      @JsonPOJOBuilder(withPrefix = "set")
      abstract static class Builder {

        public abstract Builder setCode(Integer code);

        public abstract Builder setDescription(String description);

        public abstract Builder setStatus(String status);

        public abstract ErrorMessageDetails build();
      }
    }
  }
}
