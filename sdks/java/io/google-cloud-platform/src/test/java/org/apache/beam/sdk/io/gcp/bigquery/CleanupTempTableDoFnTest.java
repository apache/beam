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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.CleanupInfo;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.CleanupOperationMessage;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.CleanupTempTableDoFn;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CleanupTempTableDoFnTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Before
  public void setUp() {
    FakeDatasetService.setUp();
  }

  @Test
  public void testCleanupTempTableDoFn() throws Exception {
    FakeDatasetService fakeDatasetService = new FakeDatasetService();
    FakeBigQueryServices fakeBqServices =
        new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    TableReference tableRef =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");

    fakeDatasetService.createDataset(
        tableRef.getProjectId(), tableRef.getDatasetId(), "", "", null);
    fakeDatasetService.createTable(
        new com.google.api.services.bigquery.model.Table().setTableReference(tableRef));

    assertTrue(
        fakeDatasetService.getDataset(tableRef.getProjectId(), tableRef.getDatasetId()) != null);
    assertTrue(fakeDatasetService.getTable(tableRef) != null);

    CleanupInfo cleanupInfo = new CleanupInfo(tableRef, true, 2);

    PCollection<KV<String, CleanupOperationMessage>> input =
        p.apply(
            Create.of(
                KV.of("job1", CleanupOperationMessage.initialize(cleanupInfo)),
                KV.of("job1", CleanupOperationMessage.streamComplete()),
                KV.of("job1", CleanupOperationMessage.streamComplete())));

    input.apply(ParDo.of(new CleanupTempTableDoFn(fakeBqServices)));

    p.run().waitUntilFinish();

    // The dataset is deleted, so getDataset and getTable should throw a 404 Exception or return
    // null
    try {
      fakeDatasetService.getDataset(tableRef.getProjectId(), tableRef.getDatasetId());
      fail("Dataset should have been deleted");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("Tried to get a dataset")
              || e.getMessage().contains("Not Found"));
    }

    try {
      TableReference deletedRef =
          new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
      Object table = fakeDatasetService.getTable(deletedRef);
      assertTrue(table == null);
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("Tried to get a dataset")
              || e.getMessage().contains("Not Found"));
    }
  }
}
