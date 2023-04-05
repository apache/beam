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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static com.google.cloud.pubsublite.cloudpubsub.MessageTransforms.fromCpsPublishTransformer;
import static com.google.cloud.pubsublite.cloudpubsub.MessageTransforms.toCpsPublishTransformer;
import static com.google.cloud.pubsublite.cloudpubsub.MessageTransforms.toCpsSubscribeTransformer;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.cloudpubsub.KeyExtractor;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessages;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A class providing transforms between Cloud Pub/Sub and Pub/Sub Lite message types. */
public final class CloudPubsubTransforms {
  private CloudPubsubTransforms() {}
  /**
   * Ensure that all messages that pass through can be converted to Cloud Pub/Sub messages using the
   * standard transformation methods in the client library.
   *
   * <p>Will fail the pipeline if a message has multiple attributes per key.
   */
  public static PTransform<PCollection<PubSubMessage>, PCollection<PubSubMessage>>
      ensureUsableAsCloudPubsub() {
    return new PTransform<PCollection<PubSubMessage>, PCollection<PubSubMessage>>() {
      @Override
      public PCollection<PubSubMessage> expand(PCollection<PubSubMessage> input) {
        return input.apply(
            MapElements.into(TypeDescriptor.of(PubSubMessage.class))
                .via(
                    message -> {
                      Object unused =
                          toCpsPublishTransformer().transform(Message.fromProto(message));
                      return message;
                    }));
      }
    };
  }

  /**
   * Transform messages read from Pub/Sub Lite to their equivalent Cloud Pub/Sub Message that would
   * have been read from PubsubIO.
   *
   * <p>Will fail the pipeline if a message has multiple attributes per map key.
   */
  public static PTransform<PCollection<SequencedMessage>, PCollection<PubsubMessage>>
      toCloudPubsubMessages() {
    return new PTransform<PCollection<SequencedMessage>, PCollection<PubsubMessage>>() {
      @Override
      public PCollection<PubsubMessage> expand(PCollection<SequencedMessage> input) {
        return input.apply(
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(
                    message ->
                        PubsubMessages.fromProto(
                            toCpsSubscribeTransformer()
                                .transform(
                                    com.google.cloud.pubsublite.SequencedMessage.fromProto(
                                        message)))));
      }
    };
  }

  /**
   * Transform messages publishable using PubsubIO to their equivalent Pub/Sub Lite publishable
   * message.
   */
  public static PTransform<PCollection<PubsubMessage>, PCollection<PubSubMessage>>
      fromCloudPubsubMessages() {
    return new PTransform<PCollection<PubsubMessage>, PCollection<PubSubMessage>>() {
      @Override
      public PCollection<PubSubMessage> expand(PCollection<PubsubMessage> input) {
        return input.apply(
            MapElements.into(TypeDescriptor.of(PubSubMessage.class))
                .via(
                    message ->
                        fromCpsPublishTransformer(KeyExtractor.DEFAULT)
                            .transform(PubsubMessages.toProto(message))
                            .toProto()));
      }
    };
  }
}
