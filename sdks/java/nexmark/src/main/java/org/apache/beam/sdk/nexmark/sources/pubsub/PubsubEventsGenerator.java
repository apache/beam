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

import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkLauncher;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.synthetic.SyntheticEventsSource;
import org.apache.beam.sdk.nexmark.utils.PubsubUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import org.slf4j.LoggerFactory;

/**
 * Generates and publishes Events to a Pubsub topic.
 * These can be later consumed by the {@link PubsubEventsSource}.
 */
public class PubsubEventsGenerator extends PTransform<PBegin, PDone> {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkLauncher.class);

  /**
   * Publish only, do not execute Nexmark queries.
   */
  public static boolean isPublishOnly(NexmarkConfiguration configuration) {
    return configuration.sourceType == NexmarkUtils.SourceType.PUBSUB
        && configuration.pubSubMode == NexmarkUtils.PubSubMode.PUBLISH_ONLY;
  }

  public static boolean needPublisher(NexmarkConfiguration configuration) {
    return configuration.sourceType == NexmarkUtils.SourceType.PUBSUB
        && configuration.pubSubMode == NexmarkUtils.PubSubMode.COMBINED;
  }

  public static PTransform<PBegin, PDone> create(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      long now) {

    String shortTopicName = PubsubUtils.shortTopic(options, queryName, now);
    NexmarkUtils.console("Writing events to Pubsub %s", shortTopicName);
    LOG.debug("Writing events to Pubsub %s", shortTopicName);

    return new PubsubEventsGenerator(
        configuration,
        options,
        queryName,
        shortTopicName,
        monitorName(configuration, queryName),
        monitorInstance(configuration, queryName));
  }

  private static String monitorName(NexmarkConfiguration configuration, String queryName) {
    return queryName + (isPublishOnly(configuration) ? ".Snoop" : ".Monitor");
  }

  private static PTransform<PCollection<? extends Event>, PCollection<Event>> monitorInstance(
      NexmarkConfiguration configuration,
      String queryName) {

    return isPublishOnly(configuration)
        ? NexmarkUtils.countEvents(queryName)
        : new Monitor<Event>(queryName, "publisher").getTransform();
  }

  private NexmarkConfiguration configuration;
  private NexmarkOptions options;
  private String queryName;
  private String shortTopicName;
  private String monitorName;
  private PTransform<PCollection<? extends Event>, PCollection<Event>> monitor;

  private PubsubEventsGenerator(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName,
      String shortTopicName,
      String monitorName,
      PTransform<PCollection<? extends Event>, PCollection<Event>> monitor) {

    this.configuration = configuration;
    this.options = options;
    this.queryName = queryName;
    this.shortTopicName = shortTopicName;
    this.monitorName = monitorName;
    this.monitor = monitor;
  }

  @Override
  public PDone expand(PBegin input) {
    PTransform<PBegin, PCollection<Event>> generateEvents =
        SyntheticEventsSource
            .create(configuration, options, queryName);

    PTransform<PCollection<Event>, PDone> pubsubSink =
        new PubsubSink(queryName, shortTopicName, configuration.usePubsubPublishTime);

    return input
        .apply(generateEvents)
        .apply(monitorName, monitor)
        .apply(pubsubSink);
  }
}

