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

package org.apache.beam.sdk.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.avro.AvroEventsSource;
import org.apache.beam.sdk.nexmark.sources.pubsub.PubsubEventsSource;
import org.apache.beam.sdk.nexmark.sources.synthetic.SyntheticEventsSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Creates an event source for {@link org.apache.beam.sdk.nexmark.NexmarkLauncher}.
 */
public class EventSourceFactory {

  /**
   * Return source of events for this run.
   * @throws IllegalArgumentException for unsupported source type or Pubsub mode.
   * @throws IllegalStateException if called for PUBLISH_ONLY.
   */
  public static PTransform<PBegin, PCollection<Event>> createSource(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    switch (configuration.sourceType) {
      case DIRECT:
        return SyntheticEventsSource.create(configuration, options, queryName);
      case AVRO:
        return AvroEventsSource.createSource(options, queryName);
      case PUBSUB:
        return PubsubEventsSource.create(configuration, options, queryName, now);
      default:
        throw new IllegalArgumentException("Unkown source type " + configuration.sourceType.name());
    }
  }
}
