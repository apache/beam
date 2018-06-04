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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test class for BigQuery sinks. */
public class PerfsToBigQueryTest {

  private static final int QUERY = 1;
  private NexmarkOptions options;
  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);
  @Rule
  public transient TemporaryFolder testFolder = new TemporaryFolder();


  @Before
  public void before() {
    options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setBigQueryTable("nexmark");
    options.setBigQueryDataset("nexmark");
    options.setRunner(DirectRunner.class);
    options.setStreaming(true);
    options.setProject("nexmark-test");
    options.setTempLocation(testFolder.getRoot().getAbsolutePath());

  }

  @Test
  public void testSavePerfsToBigQuery() throws IOException, InterruptedException {
    NexmarkConfiguration nexmarkConfiguration1 = new NexmarkConfiguration();
    nexmarkConfiguration1.query = QUERY;
    NexmarkPerf nexmarkPerf1 = new NexmarkPerf();
    nexmarkPerf1.numResults = 1000L;
    nexmarkPerf1.eventsPerSec = 0.5F;
    nexmarkPerf1.runtimeSec = 0.325F;

    NexmarkConfiguration nexmarkConfiguration2 = new NexmarkConfiguration();
    nexmarkConfiguration1.query = QUERY;
    NexmarkPerf nexmarkPerf2 = new NexmarkPerf();
    nexmarkPerf2.numResults = 1001L;
    nexmarkPerf2.eventsPerSec = 1.5F;
    nexmarkPerf2.runtimeSec = 1.325F;

    HashMap<NexmarkConfiguration, NexmarkPerf> perfs = new HashMap<>(2);
    perfs.put(nexmarkConfiguration1, nexmarkPerf1);
    perfs.put(nexmarkConfiguration2, nexmarkPerf2);
    Main.savePerfsToBigQuery(options, perfs, fakeBqServices);

    String tableSpec = NexmarkUtils.tableSpec(options, String.valueOf(QUERY), 0L, null);
    List<TableRow> actualRows =
        fakeDatasetService.getAllRows(
            options.getProject(),
            options.getBigQueryDataset(),
            BigQueryHelpers.parseTableSpec(tableSpec).getTableId());
    assertEquals("Wrong number of rows inserted", 2, actualRows.size());
    assertThat(
        actualRows,
        containsInAnyOrder(
            new TableRow()
                .set("Runtime(sec)", nexmarkPerf1.runtimeSec)
                .set("Events(/sec)", nexmarkPerf1.eventsPerSec)
                .set("Size of the result collection", nexmarkPerf1.numResults),
            new TableRow()
                .set("Runtime(sec)", nexmarkPerf2.runtimeSec)
                .set("Events(/sec)", nexmarkPerf2.eventsPerSec)
                .set("Size of the result collection", nexmarkPerf2.numResults)));
  }
}
