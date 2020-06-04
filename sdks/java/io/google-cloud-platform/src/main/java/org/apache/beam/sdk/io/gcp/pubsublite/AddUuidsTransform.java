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

import com.google.cloud.pubsublite.Message;
import com.google.common.collect.ImmutableListMultimap;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A transform to add UUIDs to each message to be written to Pub/Sub Lite. */
class AddUuidsTransform extends PTransform<PCollection<Message>, PCollection<Message>> {
  private static Message addUuid(Message message) {
    ImmutableListMultimap.Builder<String, ByteString> attributesBuilder =
        ImmutableListMultimap.builder();
    message.attributes().entries().stream()
        .filter(entry -> !entry.getKey().equals(Uuid.DEFAULT_ATTRIBUTE))
        .forEach(attributesBuilder::put);
    attributesBuilder.put(Uuid.DEFAULT_ATTRIBUTE, Uuid.random().value());
    return message.toBuilder().setAttributes(attributesBuilder.build()).build();
  }

  @Override
  public PCollection<Message> expand(PCollection<Message> input) {
    PCollection<Message> withUuids =
        input.apply(
            "AddUuids",
            MapElements.into(new TypeDescriptor<Message>() {}).via(AddUuidsTransform::addUuid));
    // Reshuffle into 1000 buckets to avoid having unit-sized bundles under high throughput.
    return withUuids.apply(
        "ShuffleToPersist", Reshuffle.<Message>viaRandomKey().withNumBuckets(1000));
  }
}
