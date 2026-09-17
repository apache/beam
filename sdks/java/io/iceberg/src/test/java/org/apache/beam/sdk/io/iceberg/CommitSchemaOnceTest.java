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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CommitSchemaOnceTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestName testName = new TestName();
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final Schema TABLE =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));

  @Test
  public void testCommitsTheWindowsSchemasAndEmitsTheSchemaId() {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TABLE);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "email", Types.StringType.get()));
    List<CollectDistinctSchemas.SchemaGroup> schemas =
        Arrays.asList(
            CollectDistinctSchemas.SchemaGroup.of(
                SchemaParser.toJson(FileSchemas.canonical(file)), 3L, Arrays.asList()));

    PCollection<Long> schemaIds =
        pipeline
            .apply(
                Create.of(Arrays.asList(schemas)).withCoder(CollectDistinctSchemas.outputCoder()))
            .apply(
                ParDo.of(
                    new CommitSchemaOnce(
                        catalogConfig,
                        "default." + testName.getMethodName(),
                        SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION),
                        IncompatibleSchemaHandling.FAIL_PIPELINE,
                        new CommitSchemaUnion.TableCreation(null, null, null))));
    PAssert.that(schemaIds).containsInAnyOrder(1L);
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, commitsCounted(result));

    Table table = warehouse.loadTable(tableId);
    assertNotNull(table.schema().findField("email"));
  }

  /** A window the table already covers commits nothing and does not count as a commit. */
  @Test
  public void testNoOpWindowDoesNotCountACommit() {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TABLE);
    Table table = warehouse.loadTable(tableId);
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING, NameMappingUtils.regenerate(table.schema(), null))
        .commit();
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
    Schema covered = new Schema(required(1, "id", Types.LongType.get()));
    List<CollectDistinctSchemas.SchemaGroup> schemas =
        Arrays.asList(
            CollectDistinctSchemas.SchemaGroup.of(
                SchemaParser.toJson(FileSchemas.canonical(covered)), 2L, Arrays.asList()));

    pipeline
        .apply(Create.of(Arrays.asList(schemas)).withCoder(CollectDistinctSchemas.outputCoder()))
        .apply(
            ParDo.of(
                new CommitSchemaOnce(
                    catalogConfig,
                    "default." + testName.getMethodName(),
                    SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION),
                    IncompatibleSchemaHandling.FAIL_PIPELINE,
                    new CommitSchemaUnion.TableCreation(null, null, null))));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(0, commitsCounted(result));
  }

  private static long commitsCounted(PipelineResult result) {
    long total = 0;
    for (MetricResult<Long> counter :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            CommitSchemaOnce.class, CommitSchemaOnce.COMMITS_COUNTER))
                    .build())
            .getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }
}
