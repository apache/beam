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

package org.apache.beam.sdk.nexmark.sinks;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.sinks.bigquery.BigQueryResultsSink;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import org.junit.Test;

/**
 * Unit tests for {@link QueryResultsSinkFactory}.
 */
public class QueryResultsSinkFactoryTest {

  private static final String QUERY_NAME = "queryName";
  private static final long NOW = 1239812L;

  @Test public void testCreatesPubsubSink() {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SinkType.PUBSUB);
    NexmarkOptions options = newOptions();
    options.setPubsubTopic("pubsubstopic");
    options.setResourceNameMode(NexmarkUtils.ResourceNameMode.VERBATIM);

    PTransform<PCollection<String>, ? extends POutput> sink =
      QueryResultsSinkFactory.createSink(configuration, options, QUERY_NAME, NOW);

    assertTrue(sink instanceof PubsubIO.Write);
  }

  @Test public void testCreatesTextSink() {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SinkType.TEXT);
    NexmarkOptions options = newOptions();
    options.setOutputPath("outputpath");
    options.setResourceNameMode(NexmarkUtils.ResourceNameMode.VERBATIM);

    PTransform<PCollection<String>, ? extends POutput> sink =
        QueryResultsSinkFactory.createSink(configuration, options, QUERY_NAME, NOW);

    assertTrue(sink instanceof TextIO.Write);
  }

  @Test public void testCreatesBigQuerySink() {
    NexmarkConfiguration configuration = newConfig(NexmarkUtils.SinkType.BIGQUERY);
    NexmarkOptions options = newOptions();

    PTransform<PCollection<String>, ? extends POutput> sink =
        QueryResultsSinkFactory.createSink(configuration, options, QUERY_NAME, NOW);

    assertTrue(sink instanceof BigQueryResultsSink);
  }

  private NexmarkConfiguration newConfig(NexmarkUtils.SinkType sinkType) {
    NexmarkConfiguration configuration = new NexmarkConfiguration();
    configuration.sinkType = sinkType;
    return configuration;
  }

  private NexmarkOptions newOptions() {
    return PipelineOptionsFactory.create().as(NexmarkOptions.class);
  }
}
