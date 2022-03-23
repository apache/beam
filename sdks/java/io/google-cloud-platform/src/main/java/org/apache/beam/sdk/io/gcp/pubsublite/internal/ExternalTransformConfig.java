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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.UuidDeduplicationOptions;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

class ExternalTransformConfig {
  private ExternalTransformConfig() {}

  public static class WriteConfig {
    private final PublisherOptions.Builder builder = PublisherOptions.newBuilder();
    private boolean addUuids = false;

    public void setTopicPath(String path) {
      builder.setTopicPath(TopicPath.parse(path));
    }

    public void setAddUuids(Boolean addUuids) {
      this.addUuids = addUuids;
    }
  }

  public static class WriteExternalBuilder
      implements ExternalTransformBuilder<WriteConfig, PCollection<byte[]>, PDone> {
    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(WriteConfig configuration) {
      PublisherOptions options = configuration.builder.build();
      boolean addUuids = configuration.addUuids;
      return new PTransform<PCollection<byte[]>, PDone>() {
        @Override
        public PDone expand(PCollection<byte[]> input) {
          PCollection<PubSubMessage> messages =
              input.apply(new ProtoFromBytes<>(PubSubMessage::parseFrom));
          if (addUuids) {
            messages = messages.apply(PubsubLiteIO.addUuids());
          }
          return messages.apply(PubsubLiteIO.write(options));
        }
      };
    }
  }

  public static class ReadConfig {
    private final SubscriberOptions.Builder builder = SubscriberOptions.newBuilder();
    private boolean deduplicate = false;

    public void setSubscriptionPath(String path) {
      builder.setSubscriptionPath(SubscriptionPath.parse(path));
    }

    public void setDeduplicate(Boolean deduplicate) {
      this.deduplicate = deduplicate;
    }
  }

  public static class ReadExternalBuilder
      implements ExternalTransformBuilder<ReadConfig, PBegin, PCollection<byte[]>> {
    @Override
    public PTransform<PBegin, PCollection<byte[]>> buildExternal(ReadConfig configuration) {
      SubscriberOptions options = configuration.builder.build();
      boolean deduplicate = configuration.deduplicate;
      return new PTransform<PBegin, PCollection<byte[]>>() {
        @Override
        public PCollection<byte[]> expand(PBegin input) {
          PCollection<SequencedMessage> messages = input.apply(PubsubLiteIO.read(options));
          if (deduplicate) {
            messages =
                messages.apply(
                    PubsubLiteIO.deduplicate(UuidDeduplicationOptions.newBuilder().build()));
          }
          return messages.apply(new ProtoToBytes<>());
        }
      };
    }
  }
}
