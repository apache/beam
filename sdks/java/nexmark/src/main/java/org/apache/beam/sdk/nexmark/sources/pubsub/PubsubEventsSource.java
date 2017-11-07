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

package org.apache.beam.sdk.nexmark.sources.pubsub;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkLauncher;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.utils.PubsubUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.LoggerFactory;

/**
 * Creates a PCollection of Events which are read from Pubsub.
 *
 * <p>Events are expected to be published to the Pubsub topic either by an extrenal party, or by the
 * {@link PubsubEventsGenerator} depending on the value of pubSubMode configuration option.
 */
public class PubsubEventsSource extends PTransform<PBegin, PCollection<Event>> {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkLauncher.class);

  /**
   * Return source of events from Pubsub.
   * Returns Optional.absent() if the mode is PUBLISH_ONLY.
   */
  public static PTransform<PBegin, PCollection<Event>> create(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    switch (configuration.pubSubMode) {
      case PUBLISH_ONLY:
        throw new IllegalStateException(
            "Creating event source is not supported in PUBLISH_ONLY mode");
      case SUBSCRIBE_ONLY:
      case COMBINED:
        return readEventsFromPubsub(configuration, options, queryName, now);
      default:
        throw new IllegalArgumentException("Unknown Pubsub mode: "
            + configuration.pubSubMode.name());
    }
  }

  private static PTransform<PBegin, PCollection<Event>> readEventsFromPubsub(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    String shortSubscriptionName =
        PubsubUtils.shortSubscription(options, queryName, now);
    NexmarkUtils.console("Reading events from Pubsub %s", shortSubscriptionName);
    LOG.debug("Reading events from Pubsub {}", shortSubscriptionName);

    return new PubsubEventsSource(
            queryName,
            shortSubscriptionName,
            configuration.usePubsubPublishTime);
  }

  private String queryName;
  private String shortSubscriptionName;
  private boolean usePubsubPublishTime;

  public PubsubEventsSource(
      String queryName,
      String shortSubscriptionName,
      boolean usePubsubPublishTime) {

    this.queryName = queryName;
    this.shortSubscriptionName = shortSubscriptionName;
    this.usePubsubPublishTime = usePubsubPublishTime;
  }

  @Override
  public PCollection<Event> expand(PBegin input) {
    PubsubIO.Read<PubsubMessage> pubsubInput =
        PubsubIO.readMessagesWithAttributes()
            .fromSubscription(shortSubscriptionName)
            .withIdAttribute(NexmarkUtils.PUBSUB_ID);

    if (!usePubsubPublishTime) {
      pubsubInput = pubsubInput.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }

    return input
        .apply(queryName + ".ReadPubsubEvents", pubsubInput)
        .apply(queryName + ".PubsubMessageToEvent", MessageTransforms.messageToEvent());
  }
}
