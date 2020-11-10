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

import static com.google.cloud.pubsublite.cloudpubsub.MessageTransforms.toCpsPublishTransformer;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import io.grpc.StatusException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A class providing a conversion validity check between Cloud Pub/Sub and Pub/Sub Lite message
 * types.
 */
public final class CloudPubsubChecks {
  private CloudPubsubChecks() {}

  /**
   * Ensure that all messages that pass through can be converted to Cloud Pub/Sub messages using the
   * standard transformation methods in the client library.
   *
   * <p>Will fail the pipeline if a message has multiple attributes per key.
   */
  public static PTransform<PCollection<? extends PubSubMessage>, PCollection<PubSubMessage>>
      ensureUsableAsCloudPubsub() {
    return ParDo.of(
        new DoFn<PubSubMessage, PubSubMessage>() {
          @ProcessElement
          public void processElement(
              @Element PubSubMessage message, OutputReceiver<PubSubMessage> output)
              throws StatusException {
            Object unused = toCpsPublishTransformer().transform(Message.fromProto(message));
            output.output(message);
          }
        });
  }
}
