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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PlanExpireSnapshotsDoFnTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testPlanExpiresSnapshotsWithCleanupLevelNone() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "plan_test_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertEquals(3, snapshots.size());
    long cutoff = snapshots.get(2).timestampMillis();

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setExpireOlderThan(cutoff).setRetainLast(1).build();

    PCollectionTuple planned =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(
                ParDo.of(new PlanExpireSnapshotsDoFn(getCatalogConfig(), config))
                    .withOutputTags(
                        PlanExpireSnapshotsDoFn.PLAN_SUMMARY,
                        TupleTagList.of(PlanExpireSnapshotsDoFn.MANIFESTS)
                            .and(PlanExpireSnapshotsDoFn.DIRECT_FILES)));

    PAssert.that(planned.get(PlanExpireSnapshotsDoFn.PLAN_SUMMARY))
        .containsInAnyOrder(ExpireSnapshotsResult.builder().setExpiredSnapshotsCount(2L).build());

    pipeline.run();

    table.refresh();
    List<Snapshot> remainingSnapshots = Lists.newArrayList(table.snapshots());
    assertEquals(1, remainingSnapshots.size());
    assertEquals(snapshots.get(2).snapshotId(), remainingSnapshots.get(0).snapshotId());
  }

  @Test
  public void testPlanThrowsWhenGcDisabled() {
    TableIdentifier tableId = TableIdentifier.of("default", "gc_disabled_" + System.nanoTime());
    warehouse.createTable(
        tableId, TestFixtures.SCHEMA, null, ImmutableMap.of(TableProperties.GC_ENABLED, "false"));

    ExpireSnapshots.Configuration config = ExpireSnapshots.Configuration.builder().build();

    pipeline
        .apply(Create.of(tableId.toString()))
        .apply(
            ParDo.of(new PlanExpireSnapshotsDoFn(getCatalogConfig(), config))
                .withOutputTags(
                    PlanExpireSnapshotsDoFn.PLAN_SUMMARY,
                    TupleTagList.of(PlanExpireSnapshotsDoFn.MANIFESTS)
                        .and(PlanExpireSnapshotsDoFn.DIRECT_FILES)));

    assertThrows(
        Exception.class,
        () -> {
          pipeline.run();
        });
  }

  @Test
  public void testPlanZeroSnapshotsExpiredIsNoOp() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "noop_test_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);
    table.refresh();

    // Cutoff far in the past
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setExpireOlderThan(1L).build();

    PCollectionTuple planned =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(
                ParDo.of(new PlanExpireSnapshotsDoFn(getCatalogConfig(), config))
                    .withOutputTags(
                        PlanExpireSnapshotsDoFn.PLAN_SUMMARY,
                        TupleTagList.of(PlanExpireSnapshotsDoFn.MANIFESTS)
                            .and(PlanExpireSnapshotsDoFn.DIRECT_FILES)));

    PAssert.that(planned.get(PlanExpireSnapshotsDoFn.PLAN_SUMMARY))
        .containsInAnyOrder(ExpireSnapshotsResult.zeros());

    pipeline.run();

    table.refresh();
    assertEquals(3, Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void testPlanHonorsRetainLast() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "retain_last_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);
    table.refresh();

    // Cutoff in the future so all 3 are older than cutoff, but retainLast = 2
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(System.currentTimeMillis() + 100_000L)
            .setRetainLast(2)
            .build();

    PCollectionTuple planned =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(
                ParDo.of(new PlanExpireSnapshotsDoFn(getCatalogConfig(), config))
                    .withOutputTags(
                        PlanExpireSnapshotsDoFn.PLAN_SUMMARY,
                        TupleTagList.of(PlanExpireSnapshotsDoFn.MANIFESTS)
                            .and(PlanExpireSnapshotsDoFn.DIRECT_FILES)));

    PAssert.that(planned.get(PlanExpireSnapshotsDoFn.PLAN_SUMMARY))
        .containsInAnyOrder(ExpireSnapshotsResult.builder().setExpiredSnapshotsCount(1L).build());

    pipeline.run();

    table.refresh();
    assertEquals(2, Lists.newArrayList(table.snapshots()).size());
  }
}
