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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;
import java.util.Date;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of BigtableRead. */
@RunWith(JUnit4.class)
public class BigtableReadIT {
  private static final String COLUMN_FAMILY_NAME = "cf";

  private String project;

  private BigtableTestOptions options;
  private String tableId = String.format("BigtableReadIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());

  private BigtableDataClient client;
  private BigtableTableAdminClient tableAdminClient;

  @Before
  public void setup() throws IOException {
    PipelineOptionsFactory.register(BigtableTestOptions.class);

    options = TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);

    project = options.getBigtableProject();

    if (project.equals("")) {
      project = options.as(GcpOptions.class).getProject();
    }

    BigtableDataSettings veneerSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(project)
            .setInstanceId(options.getInstanceId())
            .build();

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(project)
            .setInstanceId(options.getInstanceId())
            .build();

    client = BigtableDataClient.create(veneerSettings);
    tableAdminClient = BigtableTableAdminClient.create(adminSettings);
  }

  @After
  public void tearDown() {
    if (tableAdminClient != null) {
      try {
        tableAdminClient.deleteTable(tableId);
      } catch (Exception e) {
        // ignore exceptions
      }
      tableAdminClient.close();
    }

    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testE2EBigtableRead() {
    BigtableOptions.Builder bigtableOptionsBuilder =
        new BigtableOptions.Builder().setProjectId(project).setInstanceId(options.getInstanceId());

    final String tableId = "BigtableReadTest";
    final long numRows = 1000L;

    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(BigtableIO.read().withBigtableOptions(bigtableOptionsBuilder).withTableId(tableId))
            .apply(Count.globally());
    PAssert.thatSingleton(count).isEqualTo(numRows);
    PipelineResult r = p.run();
    checkLineageSourceMetric(r, tableId);
  }

  @Test
  public void testE2EBigtableSegmentRead() {
    tableAdminClient.createTable(CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY_NAME));

    final long numRows = 20L;
    String value = StringUtils.repeat("v", 100 * 1000 * 1000);
    // populate a table with large rows, each row is 100 MB. This will make the pipeline reach
    // segment reader memory limit with 8 rows when running on ec2-standard.
    for (int i = 0; i < numRows; i++) {
      client.mutateRow(
          RowMutation.create(tableId, "key-" + i).setCell(COLUMN_FAMILY_NAME, "q", value));
    }

    BigtableOptions.Builder bigtableOptionsBuilder =
        new BigtableOptions.Builder().setProjectId(project).setInstanceId(options.getInstanceId());

    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(
                BigtableIO.read()
                    .withBigtableOptions(bigtableOptionsBuilder)
                    .withTableId(tableId)
                    .withMaxBufferElementCount(10))
            .apply(Count.globally());
    PAssert.thatSingleton(count).isEqualTo(numRows);
    PipelineResult r = p.run();
    checkLineageSourceMetric(r, tableId);
  }

  private void checkLineageSourceMetric(PipelineResult r, String tableId) {
    // TODO(https://github.com/apache/beam/issues/32071) test malformed,
    //   when pipeline.run() is non-blocking, the metrics are not available by the time of query
    if (options.getRunner().getName().contains("DirectRunner")) {
      assertThat(
          Lineage.query(r.metrics(), Lineage.Type.SOURCE),
          hasItem(
              Lineage.getFqName(
                  "bigtable", ImmutableList.of(project, options.getInstanceId(), tableId))));
    }
  }
}
