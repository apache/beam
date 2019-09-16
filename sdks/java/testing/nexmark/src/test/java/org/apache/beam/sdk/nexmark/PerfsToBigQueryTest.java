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
package org.apache.beam.sdk.nexmark;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testutils.TestResult;
import org.apache.beam.sdk.testutils.fakes.FakeBigQueryResultsPublisher;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/** Test class for BigQuery sinks. */
public class PerfsToBigQueryTest {

  private static final NexmarkQueryName QUERY = NexmarkQueryName.CURRENCY_CONVERSION;

  private NexmarkOptions options;

  private FakeBigQueryResultsPublisher publisher;

  @Before
  public void before() {
    options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setBigQueryTable("nexmark");
    options.setBigQueryDataset("nexmark");
    options.setRunner(DirectRunner.class);
    options.setStreaming(true);
    options.setProject("nexmark-test");
    options.setResourceNameMode(NexmarkUtils.ResourceNameMode.QUERY_RUNNER_AND_MODE);

    publisher = new FakeBigQueryResultsPublisher();
  }

  @Test
  public void testSavePerfsToBigQuery() {
    NexmarkConfiguration nexmarkConfiguration1 = new NexmarkConfiguration();
    nexmarkConfiguration1.query = QUERY;
    // just for the 2 configurations to be different to have different keys
    nexmarkConfiguration1.cpuDelayMs = 100L;
    NexmarkPerf nexmarkPerf1 = new NexmarkPerf();
    nexmarkPerf1.numResults = 1000L;
    nexmarkPerf1.eventsPerSec = 0.5F;
    nexmarkPerf1.runtimeSec = 0.325F;

    NexmarkConfiguration nexmarkConfiguration2 = new NexmarkConfiguration();
    nexmarkConfiguration2.query = QUERY;
    // just for the 2 configurations to be different to have different keys
    nexmarkConfiguration1.cpuDelayMs = 200L;
    NexmarkPerf nexmarkPerf2 = new NexmarkPerf();
    nexmarkPerf2.numResults = 1001L;
    nexmarkPerf2.eventsPerSec = 1.5F;
    nexmarkPerf2.runtimeSec = 1.325F;

    // simulate 2 runs of the same query just to check that rows are appended correctly.
    HashMap<NexmarkConfiguration, NexmarkPerf> perfs = new HashMap<>(2);
    perfs.put(nexmarkConfiguration1, nexmarkPerf1);
    perfs.put(nexmarkConfiguration2, nexmarkPerf2);

    long startTimestampMilliseconds = 1454284800000L;
    Main.savePerfsToBigQuery(publisher, options, perfs, new Instant(startTimestampMilliseconds));

    String tableName = NexmarkUtils.tableName(options, QUERY.getNumberOrName(), 0L, null);
    List<TestResult> rows = publisher.getRecords(tableName);

    assertThat(rows, hasItems(nexmarkPerf1, nexmarkPerf2));
  }
}
