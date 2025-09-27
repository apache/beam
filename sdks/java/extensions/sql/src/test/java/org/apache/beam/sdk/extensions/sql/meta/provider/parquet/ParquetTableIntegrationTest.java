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
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for ParquetTable with filter and projection pushdown. */
@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class ParquetTableIntegrationTest implements Serializable {

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  @ClassRule public static final TestPipeline WRITE_PIPELINE = TestPipeline.create();
  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  private static BeamSqlEnv env;
  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addStringField("name")
          .addBooleanField("active")
          .addDoubleField("score")
          .addInt64Field("timestamp")
          .build();

  @BeforeClass
  public static void setupAll() throws Exception {
    File testDataDir = new File(TEMP_FOLDER.getRoot(), "test-data");
    env = BeamSqlEnv.inMemory(new ParquetTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE TestTable %s TYPE parquet LOCATION '%s'",
            "(id INT, name VARCHAR, active BOOLEAN, score DOUBLE, timestamp BIGINT)",
            testDataDir.getAbsolutePath() + File.separator));

    // Insert test data
    BeamSqlRelUtils.toPCollection(
        WRITE_PIPELINE,
        env.parseQuery(
            "INSERT INTO TestTable VALUES "
                + "(1, 'Alice', TRUE, 95.5, 1000), "
                + "(2, 'Bob', FALSE, 87.2, 2000), "
                + "(3, 'Charlie', TRUE, 92.8, 3000), "
                + "(4, 'David', TRUE, 78.9, 4000), "
                + "(5, 'Eve', FALSE, 88.1, 5000), "
                + "(6, 'Frank', TRUE, 91.3, 6000), "
                + "(7, 'Grace', FALSE, 85.7, 7000), "
                + "(8, 'Henry', TRUE, 94.2, 8000)"));
    WRITE_PIPELINE.run().waitUntilFinish();
  }

  @Test
  public void testSimpleFilter() {
    String query = "SELECT * FROM TestTable WHERE active = TRUE";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(1, "Alice", true, 95.5, 1000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(3, "Charlie", true, 92.8, 3000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(4, "David", true, 78.9, 4000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(6, "Frank", true, 91.3, 6000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(8, "Henry", true, 94.2, 8000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testRangeFilter() {
    String query = "SELECT * FROM TestTable WHERE score >= 90.0 AND score <= 95.0";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(1, "Alice", true, 95.5, 1000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(3, "Charlie", true, 92.8, 3000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(6, "Frank", true, 91.3, 6000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(8, "Henry", true, 94.2, 8000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testInFilter() {
    String query = "SELECT * FROM TestTable WHERE id IN (1, 3, 5, 7)";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(1, "Alice", true, 95.5, 1000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(3, "Charlie", true, 92.8, 3000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(5, "Eve", false, 88.1, 5000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(7, "Grace", false, 85.7, 7000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testIsNullFilter() {
    // First insert a row with null name
    BeamSqlRelUtils.toPCollection(
        readPipeline, env.parseQuery("INSERT INTO TestTable VALUES (9, NULL, TRUE, 90.0, 9000)"));
    readPipeline.run().waitUntilFinish();

    String query = "SELECT * FROM TestTable WHERE name IS NULL";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(Row.withSchema(TEST_SCHEMA).addValues(9, null, true, 90.0, 9000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testProjectionOnly() {
    String query = "SELECT id, name FROM TestTable";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema projectedSchema = Schema.builder().addInt32Field("id").addStringField("name").build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(projectedSchema).addValues(1, "Alice").build(),
            Row.withSchema(projectedSchema).addValues(2, "Bob").build(),
            Row.withSchema(projectedSchema).addValues(3, "Charlie").build(),
            Row.withSchema(projectedSchema).addValues(4, "David").build(),
            Row.withSchema(projectedSchema).addValues(5, "Eve").build(),
            Row.withSchema(projectedSchema).addValues(6, "Frank").build(),
            Row.withSchema(projectedSchema).addValues(7, "Grace").build(),
            Row.withSchema(projectedSchema).addValues(8, "Henry").build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testProjectionWithFilter() {
    String query = "SELECT name, score FROM TestTable WHERE active = TRUE AND score > 90.0";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema projectedSchema =
        Schema.builder().addStringField("name").addDoubleField("score").build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(projectedSchema).addValues("Alice", 95.5).build(),
            Row.withSchema(projectedSchema).addValues("Charlie", 92.8).build(),
            Row.withSchema(projectedSchema).addValues("Frank", 91.3).build(),
            Row.withSchema(projectedSchema).addValues("Henry", 94.2).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testComplexFilter() {
    String query =
        "SELECT * FROM TestTable WHERE (active = TRUE AND score > 90.0) OR (active = FALSE AND score < 90.0)";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(1, "Alice", true, 95.5, 1000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(2, "Bob", false, 87.2, 2000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(3, "Charlie", true, 92.8, 3000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(5, "Eve", false, 88.1, 5000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(6, "Frank", true, 91.3, 6000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(7, "Grace", false, 85.7, 7000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(8, "Henry", true, 94.2, 8000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testNotInFilter() {
    String query = "SELECT * FROM TestTable WHERE id NOT IN (1, 3, 5, 7)";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(2, "Bob", false, 87.2, 2000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(4, "David", true, 78.9, 4000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(6, "Frank", true, 91.3, 6000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(8, "Henry", true, 94.2, 8000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testTimestampFilter() {
    String query = "SELECT * FROM TestTable WHERE timestamp > 3000";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(TEST_SCHEMA).addValues(4, "David", true, 78.9, 4000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(5, "Eve", false, 88.1, 5000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(6, "Frank", true, 91.3, 6000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(7, "Grace", false, 85.7, 7000L).build(),
            Row.withSchema(TEST_SCHEMA).addValues(8, "Henry", true, 94.2, 8000L).build());

    PAssert.that(result).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testNoFilterNoProjection() {
    String query = "SELECT * FROM TestTable";
    BeamRelNode relNode = env.parseQuery(query);
    PCollection<Row> result = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    // Should return all rows with full schema
    assertThat(result.getSchema(), Matchers.equalTo(TEST_SCHEMA));

    PipelineResult pipelineResult = readPipeline.run();
    pipelineResult.waitUntilFinish();

    // Verify that the pipeline completed successfully
    assertEquals(PipelineResult.State.DONE, pipelineResult.getState());
  }
}
