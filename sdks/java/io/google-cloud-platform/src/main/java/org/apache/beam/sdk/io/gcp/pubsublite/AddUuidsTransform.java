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

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A transform to add UUIDs to each message to be written to Pub/Sub Lite. */
class AddUuidsTransform extends PTransform<PCollection<PubSubMessage>, PCollection<PubSubMessage>> {
  private static PubSubMessage addUuid(PubSubMessage message) {
    PubSubMessage.Builder builder = message.toBuilder();
    builder.putAttributes(
        Uuid.DEFAULT_ATTRIBUTE,
        AttributeValues.newBuilder().addValues(Uuid.random().value()).build());
    return builder.build();
  }

  @Override
  public PCollection<PubSubMessage> expand(PCollection<PubSubMessage> input) {
    PCollection<PubSubMessage> withUuids =
        input.apply(
            "AddUuids",
            MapElements.into(new TypeDescriptor<PubSubMessage>() {})
                .via(AddUuidsTransform::addUuid));
    // Reshuffle into 1000 buckets to avoid having unit-sized bundles under high throughput.
    return withUuids.apply(
        "ShuffleToPersist", Reshuffle.<PubSubMessage>viaRandomKey().withNumBuckets(1000));
  }
}
