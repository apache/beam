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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Parameterized test for ParquetTable's filter and projection pushdown capabilities. */
@RunWith(Parameterized.class)
@Category(NeedsRunner.class)
public class ParquetTableProviderTest implements Serializable {

  @ClassRule public static final TemporaryFolder tempFolder = new TemporaryFolder();
  @ClassRule public static final TestPipeline writePipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  private static BeamSqlEnv env;
  private static final Schema FULL_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addNullableField("product_name", Schema.FieldType.STRING)
          .addBooleanField("is_stocked")
          .addDoubleField("price")
          .addInt32Field("category_id")
          .build();

  private static final Schema PROJECTED_ID_PRICE_SCHEMA =
      Schema.builder().addInt32Field("id").addDoubleField("price").build();

  private static final Schema PROJECTED_NAME_CAT_SCHEMA =
      Schema.builder()
          .addNullableField("product_name", Schema.FieldType.STRING)
          .addInt32Field("category_id")
          .build();

  @Parameter(0)
  public String testCaseName;

  @Parameter(1)
  public String query;

  @Parameter(2)
  public List<Object> params;

  @Parameter(3)
  public long expectedReadCount;

  @Parameter(4)
  public List<Row> expectedRows;

  @Parameter(5)
  public Schema expectedSchema;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            "Filter: PriceGreaterThan",
            "SELECT * FROM ProductInfo WHERE price > 200.0",
            Collections.emptyList(),
            3L,
            Arrays.asList(
                Row.withSchema(FULL_SCHEMA).addValues(1, "Laptop", true, 1200.50, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(4, "Monitor", true, 300.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(6, "Dock", true, 250.0, 103).build()),
            FULL_SCHEMA
          },
          {
            "Filter: StockedAndCategory",
            "SELECT * FROM ProductInfo WHERE is_stocked = TRUE AND category_id = 101",
            Collections.emptyList(),
            2L,
            Arrays.asList(
                Row.withSchema(FULL_SCHEMA).addValues(1, "Laptop", true, 1200.50, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(2, "Mouse", true, 25.0, 101).build()),
            FULL_SCHEMA
          },
          {
            "Filter: IsNotNull",
            "SELECT * FROM ProductInfo WHERE product_name IS NOT NULL",
            Collections.emptyList(),
            6L,
            Arrays.asList(
                Row.withSchema(FULL_SCHEMA).addValues(1, "Laptop", true, 1200.50, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(2, "Mouse", true, 25.0, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(3, "Keyboard", false, 75.25, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(4, "Monitor", true, 300.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(5, "Webcam", false, 150.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(6, "Dock", true, 250.0, 103).build()),
            FULL_SCHEMA
          },
          {
            "Filter: Parameterized (No Pushdown)",
            "SELECT * FROM ProductInfo WHERE price > ? AND is_stocked = ?",
            Arrays.asList(100.0, true),
            7L,
            Arrays.asList(
                Row.withSchema(FULL_SCHEMA).addValues(1, "Laptop", true, 1200.50, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(2, "Mouse", true, 25.0, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(3, "Keyboard", false, 75.25, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(4, "Monitor", true, 300.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(5, "Webcam", false, 150.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(6, "Dock", true, 250.0, 103).build(),
                Row.withSchema(FULL_SCHEMA).addValues(7, null, false, 45.0, 103).build()),
            FULL_SCHEMA
          },
          {
            "Projection: Simple",
            "SELECT id, price FROM ProductInfo",
            Collections.emptyList(),
            7L,
            Arrays.asList(
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(1, 1200.50).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(2, 25.0).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(3, 75.25).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(4, 300.0).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(5, 150.0).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(6, 250.0).build(),
                Row.withSchema(PROJECTED_ID_PRICE_SCHEMA).addValues(7, 45.0).build()),
            PROJECTED_ID_PRICE_SCHEMA
          },
          {
            "Projection with Filter",
            "SELECT product_name, category_id FROM ProductInfo WHERE price < 100.0",
            Collections.emptyList(),
            3L,
            Arrays.asList(
                Row.withSchema(PROJECTED_NAME_CAT_SCHEMA).addValues("Mouse", 101).build(),
                Row.withSchema(PROJECTED_NAME_CAT_SCHEMA).addValues("Keyboard", 101).build(),
                Row.withSchema(PROJECTED_NAME_CAT_SCHEMA).addValues(null, 103).build()),
            PROJECTED_NAME_CAT_SCHEMA
          },
          {
            "No Filter: No Pushdown",
            "SELECT * FROM ProductInfo",
            Collections.emptyList(),
            7L,
            Arrays.asList(
                Row.withSchema(FULL_SCHEMA).addValues(1, "Laptop", true, 1200.50, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(2, "Mouse", true, 25.0, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(3, "Keyboard", false, 75.25, 101).build(),
                Row.withSchema(FULL_SCHEMA).addValues(4, "Monitor", true, 300.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(5, "Webcam", false, 150.0, 102).build(),
                Row.withSchema(FULL_SCHEMA).addValues(6, "Dock", true, 250.0, 103).build(),
                Row.withSchema(FULL_SCHEMA).addValues(7, null, false, 45.0, 103).build()),
            FULL_SCHEMA
          },
        });
  }

  @BeforeClass
  public static void setupAll() {
    File complexDestFile = new File(tempFolder.getRoot(), "product-info/");
    env = BeamSqlEnv.inMemory(new ParquetTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE ProductInfo %s TYPE parquet LOCATION '%s'",
            "(id INT, product_name VARCHAR NULL, is_stocked BOOLEAN, price DOUBLE, category_id INT)",
            complexDestFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO ProductInfo VALUES "
                + "(1, 'Laptop', TRUE, 1200.50, 101), "
                + "(2, 'Mouse', TRUE, 25.0, 101), "
                + "(3, 'Keyboard', FALSE, 75.25, 101), "
                + "(4, 'Monitor', TRUE, 300.0, 102), "
                + "(5, 'Webcam', FALSE, 150.0, 102), "
                + "(6, 'Dock', TRUE, 250.0, 103), "
                + "(7, NULL, FALSE, 45.0, 103)"));
    writePipeline.run().waitUntilFinish();
  }

  @Test
  public void testPushdown() {
    BeamRelNode relNode = env.parseQuery(query);

    PCollection<Row> rows = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    PCollection<Row> countedRows =
        rows.apply("CountRecords", ParDo.of(new CounterFn())).setRowSchema(expectedSchema);

    PAssert.that(countedRows).containsInAnyOrder(expectedRows);

    PipelineResult result = readPipeline.run();
    result.waitUntilFinish();

    MetricsFilter filter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(CounterFn.class, "elements_processed"))
            .build();
    MetricQueryResults metrics = result.metrics().queryMetrics(filter);

    long finalCount = 0;
    Iterable<MetricResult<Long>> counters = metrics.getCounters();
    if (counters.iterator().hasNext()) {
      finalCount = counters.iterator().next().getCommitted();
    }

    assertEquals(
        "Pushdown failed for case '" + testCaseName + "'. Incorrect number of rows processed.",
        expectedReadCount,
        finalCount);
  }

  private static class CounterFn extends DoFn<Row, Row> {
    private final Counter elementsProcessed =
        Metrics.counter(CounterFn.class, "elements_processed");

    @ProcessElement
    public void processElement(ProcessContext c) {
      elementsProcessed.inc();
      c.output(c.element());
    }
  }
}
