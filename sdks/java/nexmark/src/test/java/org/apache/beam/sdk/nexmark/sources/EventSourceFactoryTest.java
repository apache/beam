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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.avro.AvroEventsSource;
import org.apache.beam.sdk.nexmark.sources.pubsub.PubsubEventsSource;
import org.apache.beam.sdk.nexmark.sources.synthetic.SyntheticEventsSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link EventSourceFactoryTest}.
 */
public class EventSourceFactoryTest {

  private static final String QUERY_NAME = "queryName";
  private static final String INPUT_PATH = "inputPath";
  private static final String SUBSCRIPTION_NAME = "pubsubSubscriptionName";
  private static final long NOW = 12345L;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void testCreatesSyntheticBatchSource() throws Exception {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SourceType.DIRECT);
    NexmarkOptions options = mock(NexmarkOptions.class);
    doReturn(false).when(options).isStreaming();

    PTransform<PBegin, PCollection<Event>> eventSource =
        EventSourceFactory.createSource(configuration, options, QUERY_NAME, NOW);

    assertNotNull(eventSource);
    assertTrue(eventSource instanceof SyntheticEventsSource);
  }

  @Test public void testCreatesSyntheticStreamingSource() throws Exception {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SourceType.DIRECT);
    NexmarkOptions options = mock(NexmarkOptions.class);
    doReturn(true).when(options).isStreaming();

    PTransform<PBegin, PCollection<Event>> eventSource =
        EventSourceFactory.createSource(configuration, options, QUERY_NAME, NOW);

    assertNotNull(eventSource);
    assertTrue(eventSource instanceof SyntheticEventsSource);
  }

  @Test public void testCreatesAvroSource() throws Exception {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SourceType.AVRO);
    NexmarkOptions options = mock(NexmarkOptions.class);
    doReturn(INPUT_PATH).when(options).getInputPath();

    PTransform<PBegin, PCollection<Event>> eventSource =
        EventSourceFactory.createSource(configuration, options, QUERY_NAME, NOW);

    assertNotNull(eventSource);
    assertTrue(eventSource instanceof AvroEventsSource);
  }

  @Test public void testCreatesPubsubSource() throws Exception {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SourceType.PUBSUB);
    configuration.pubSubMode = NexmarkUtils.PubSubMode.COMBINED;

    NexmarkOptions options = mock(NexmarkOptions.class);
    doReturn(SUBSCRIPTION_NAME).when(options).getPubsubSubscription();
    doReturn(NexmarkUtils.ResourceNameMode.VERBATIM).when(options).getResourceNameMode();

    PTransform<PBegin, PCollection<Event>> eventSource =
        EventSourceFactory.createSource(configuration, options, QUERY_NAME, NOW);

    assertNotNull(eventSource);
    assertTrue(eventSource instanceof PubsubEventsSource);
  }

  private NexmarkConfiguration newConfig(NexmarkUtils.SourceType sourceType) {
    NexmarkConfiguration configuration = new NexmarkConfiguration();
    configuration.sourceType = sourceType;
    return configuration;
  }
}
