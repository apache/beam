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

import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A transform for deduplicating Pub/Sub Lite messages based on the UUID they were published with.
 */
class UuidDeduplicationTransform
    extends PTransform<PCollection<SequencedMessage>, PCollection<SequencedMessage>> {
  private final UuidDeduplicationOptions options;

  UuidDeduplicationTransform(UuidDeduplicationOptions options) {
    this.options = options;
  }

  @Override
  public PCollection<SequencedMessage> expand(PCollection<SequencedMessage> input) {
    ProcessFunction<SequencedMessage, KV<Uuid, SequencedMessage>> mapWithKeys =
        message -> KV.of(options.uuidExtractor().apply(message), message);
    PCollection<KV<Uuid, SequencedMessage>> uuidMapped =
        input.apply(
            "MapUuids",
            MapElements.into(new TypeDescriptor<KV<Uuid, SequencedMessage>>() {}).via(mapWithKeys));
    PCollection<KV<Uuid, SequencedMessage>> unique =
        uuidMapped.apply("Deduplicate", options.deduplicate());
    return unique.apply("StripUuids", Values.create());
  }
}
