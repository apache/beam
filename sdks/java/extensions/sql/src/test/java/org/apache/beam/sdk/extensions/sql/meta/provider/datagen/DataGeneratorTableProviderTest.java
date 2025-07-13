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
package org.apache.beam.sdk.extensions.sql.meta.provider.datagen;

import java.math.BigDecimal;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for the {@link DataGeneratorTableProvider}. */
public class DataGeneratorTableProviderTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBoundedGeneration() {
    String createDdl =
        "CREATE EXTERNAL TABLE bounded_table (\n"
            + "  id BIGINT,\n"
            + "  name VARCHAR\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"100\"\n"
            + "}'";

    PCollection<Row> result =
        pipeline.apply(
            "testBoundedGeneration",
            SqlTransform.query("SELECT * FROM bounded_table").withDdlString(createDdl));

    Assert.assertEquals(IsBounded.BOUNDED, result.isBounded());
    PAssert.that(result.apply(Count.globally())).containsInAnyOrder(100L);

    pipeline.run().waitUntilFinish();
  }

  private static class ValidateFieldsFn extends DoFn<Row, Void> {
    private final long startId;
    private final long endId;
    private final int nameLength;
    private final double minScore;
    private final double maxScore;

    ValidateFieldsFn(long startId, long endId, int nameLength, double minScore, double maxScore) {
      this.startId = startId;
      this.endId = endId;
      this.nameLength = nameLength;
      this.minScore = minScore;
      this.maxScore = maxScore;
    }

    @ProcessElement
    public void processElement(@Element Row row) {
      Long eventId = row.getInt64("event_id");
      Assert.assertTrue("Event ID should be within range", eventId >= startId && eventId <= endId);

      String eventName = row.getString("event_name");
      Assert.assertEquals("Event name should have correct length", nameLength, eventName.length());

      Double score = row.getDouble("score");
      Assert.assertTrue("Score should be within range", score >= minScore && score <= maxScore);
    }
  }

  @Test
  public void testFieldGenerators() {
    String createDdl =
        "CREATE EXTERNAL TABLE complex_table (\n"
            + "  event_id BIGINT,\n"
            + "  event_name VARCHAR,\n"
            + "  score DOUBLE\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"10\",\n"
            + "  \"fields.event_id.kind\": \"sequence\",\n"
            + "  \"fields.event_id.start\": \"100\",\n"
            + "  \"fields.event_id.end\": \"109\",\n"
            + "  \"fields.event_name.kind\": \"random\",\n"
            + "  \"fields.event_name.length\": \"15\",\n"
            + "  \"fields.score.kind\": \"random\",\n"
            + "  \"fields.score.min\": \"50.0\",\n"
            + "  \"fields.score.max\": \"100.0\"\n"
            + "}'";

    PCollection<Row> result =
        pipeline.apply(
            "testFieldGenerators",
            SqlTransform.query("SELECT * FROM complex_table").withDdlString(createDdl));

    result.apply("ValidateFields", ParDo.of(new ValidateFieldsFn(100, 109, 15, 50.0, 100.0)));
    pipeline.run().waitUntilFinish();
  }

  private static class ValidateNullsFn extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      Assert.assertNull("Field should be null", row.getValue("nullable_field"));
      Assert.assertNotNull("Field should not be null", row.getInt64("non_nullable_field"));
    }
  }

  @Test
  public void testNullRate() {
    String createDdl =
        "CREATE EXTERNAL TABLE null_rate_table (\n"
            + "  nullable_field VARCHAR,\n"
            + "  non_nullable_field BIGINT\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"50\",\n"
            + "  \"fields.nullable_field.null-rate\": \"1.0\",\n"
            + "  \"fields.non_nullable_field.kind\": \"sequence\"\n"
            + "}'";

    PCollection<Row> result =
        pipeline.apply(
            "testNullRate",
            SqlTransform.query("SELECT * FROM null_rate_table").withDdlString(createDdl));

    result.apply("ValidateNulls", ParDo.of(new ValidateNullsFn()));

    pipeline.run().waitUntilFinish();
  }

  private static class ValidateAllTypesFn extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      Assert.assertNotNull(row.getBoolean("is_active"));

      BigDecimal cost = row.getDecimal("cost");
      Assert.assertTrue("Cost should be >= 10.50", cost.compareTo(BigDecimal.valueOf(10.50)) >= 0);
      Assert.assertTrue("Cost should be <= 99.99", cost.compareTo(BigDecimal.valueOf(99.99)) <= 0);

      Instant now = Instant.now();
      Instant pastTimestamp = row.getDateTime("past_timestamp").toInstant();
      Instant nowTimestamp = row.getDateTime("now_timestamp").toInstant();

      Assert.assertTrue("past_timestamp should be in the past", pastTimestamp.isBefore(now));
      Assert.assertTrue(
          "now_timestamp should be very recent",
          now.plus(Duration.millis(100)).isAfter(nowTimestamp));
    }
  }

  @Test
  public void testAllDataTypes() {
    String createDdl =
        "CREATE EXTERNAL TABLE all_types_table (\n"
            + "  is_active BOOLEAN,\n"
            + "  cost DECIMAL,\n"
            + "  past_timestamp TIMESTAMP,\n"
            + "  now_timestamp TIMESTAMP\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"10\",\n"
            + "  \"fields.cost.min\": \"10.50\",\n"
            + "  \"fields.cost.max\": \"99.99\",\n"
            + "  \"fields.past_timestamp.max-past\": \"3600000\"\n"
            + "}'";

    pipeline
        .apply(
            "testAllDataTypes",
            SqlTransform.query("SELECT * FROM all_types_table").withDdlString(createDdl))
        .apply("ValidateAllTypes", ParDo.of(new ValidateAllTypesFn()));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingRequiredPropertyThrowsException() {
    String createDdl = "CREATE EXTERNAL TABLE bad_table (id INT) TYPE 'datagen' TBLPROPERTIES '{}'";

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "A 'datagen' table requires either 'rows-per-second' (for unbounded) or 'number-of-rows' (for bounded) in TBLPROPERTIES.");

    pipeline.apply(
        "testMissingRequiredProperty",
        SqlTransform.query("SELECT * FROM bad_table").withDdlString(createDdl));
    pipeline.run().waitUntilFinish();
  }

  /**
   * Tests the default processing-time behavior to ensure it still works correctly and that
   * event-time configuration is not required.
   */
  @Test
  public void testProcessingTimeBehavior() {
    String createDdl =
        "CREATE EXTERNAL TABLE processing_time_table (\n"
            + "  id BIGINT\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"20\"\n"
            + "}'";

    PCollection<Row> result =
        pipeline.apply(
            "testProcessingTimeBehavior",
            SqlTransform.query("SELECT * FROM processing_time_table").withDdlString(createDdl));

    PAssert.that(result.apply("CountRows", Count.globally())).containsInAnyOrder(20L);

    pipeline.run().waitUntilFinish();
  }

  private static class ValidateMultiTimestampFn extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      Instant mainEventTime = row.getDateTime("main_event_time").toInstant();
      Instant secondaryTime = row.getDateTime("secondary_time").toInstant();

      Assert.assertTrue(
          "Secondary timestamp should be recent",
          Instant.now().plus(Duration.millis(200)).isAfter(secondaryTime));

      Assert.assertNotEquals(mainEventTime, secondaryTime);
    }
  }

  /**
   * Verifies that a table with multiple timestamp columns works correctly, with one column driving
   * the watermark and the other being populated independently.
   */
  @Test
  public void testMultipleTimestampColumns() {
    String createDdl =
        "CREATE EXTERNAL TABLE multi_ts_table (\n"
            + "  main_event_time TIMESTAMP,\n"
            + "  secondary_time TIMESTAMP\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"10\",\n"
            + "  \"timestamp.behavior\": \"event_time\",\n"
            + "  \"event_time.timestamp_column\": \"main_event_time\",\n"
            + "  \"fields.secondary_time.kind\": \"datetime\",\n"
            + "  \"fields.secondary_time.now\": \"true\"\n"
            + "}'";

    PCollection<Row> result =
        pipeline.apply(
            "testMultiTimestamp",
            SqlTransform.query("SELECT * FROM multi_ts_table").withDdlString(createDdl));

    result.apply("ValidateTimestamps", ParDo.of(new ValidateMultiTimestampFn()));

    pipeline.run().waitUntilFinish();
  }

  /**
   * Ensures that a misconfiguration (specifying event_time behavior without the required column)
   * throws a descriptive error.
   */
  @Test
  public void testEventTimeMissingColumnThrowsException() {
    String createDdl =
        "CREATE EXTERNAL TABLE bad_event_time_table (\n"
            + "  ts TIMESTAMP\n"
            + ") TYPE 'datagen' TBLPROPERTIES '{\n"
            + "  \"number-of-rows\": \"10\",\n"
            + "  \"timestamp.behavior\": \"event_time\"\n"
            + "}'";

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "For 'event_time' behavior, 'event_time.timestamp_column' must be specified.");

    pipeline.apply(
        "testMissingEventTimeColumn",
        SqlTransform.query("SELECT * FROM bad_event_time_table").withDdlString(createDdl));
    pipeline.run().waitUntilFinish();
  }
}
