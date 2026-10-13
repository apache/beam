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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.junit.Assert.assertThrows;

import java.time.LocalDateTime;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ApplyWatermarkColumn}. */
@RunWith(JUnit4.class)
public class ApplyWatermarkColumnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void stampsRowsFromSupportedWatermarkTypes() {
    assertWatermarkTimestamp(
        "jodaDateTime",
        Schema.builder().addStringField("id").addDateTimeField("wm").build(),
        Row.withSchema(Schema.builder().addStringField("id").addDateTimeField("wm").build())
            .addValues("joda", new Instant(1_234L))
            .build(),
        new Instant(1_234L));

    assertWatermarkTimestamp(
        "longMicros",
        Schema.builder().addStringField("id").addInt64Field("wm").build(),
        Row.withSchema(Schema.builder().addStringField("id").addInt64Field("wm").build())
            .addValues("long", 1_234_567L)
            .build(),
        new Instant(1_234L));

    assertWatermarkTimestamp(
        "localDateTime",
        Schema.builder().addStringField("id").addLogicalTypeField("wm", SqlTypes.DATETIME).build(),
        Row.withSchema(
                Schema.builder()
                    .addStringField("id")
                    .addLogicalTypeField("wm", SqlTypes.DATETIME)
                    .build())
            .addValues("ldt", LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_000_000))
            .build(),
        new Instant(-877L));

    assertWatermarkTimestamp(
        "javaInstant",
        Schema.builder().addStringField("id").addLogicalTypeField("wm", Timestamp.MICROS).build(),
        Row.withSchema(
                Schema.builder()
                    .addStringField("id")
                    .addLogicalTypeField("wm", Timestamp.MICROS)
                    .build())
            .addValues("instant", java.time.Instant.parse("1969-12-31T23:59:59.123Z"))
            .build(),
        new Instant(-877L));

    pipeline.run();
  }

  @Test
  public void nullWatermarkValuePreservesInputTimestamp() {
    Schema schema =
        Schema.of(
            Schema.Field.of("id", Schema.FieldType.STRING),
            Schema.Field.nullable("wm", Schema.FieldType.DATETIME));
    Row row = Row.withSchema(schema).addValues("null", null).build();
    Instant inputTimestamp = new Instant(99L);

    PCollection<Row> output =
        pipeline
            .apply(
                Create.timestamped(TimestampedValue.of(row, inputTimestamp))
                    .withCoder(RowCoder.of(schema)))
            .apply(ParDo.of(new ApplyWatermarkColumn("wm", "microseconds")));
    output.setCoder(RowCoder.of(schema));

    PAssert.that(output.apply(Reify.timestamps()))
        .containsInAnyOrder(TimestampedValue.of(row, inputTimestamp));

    pipeline.run();
  }

  @Test
  public void unsupportedWatermarkTypeThrows() {
    Schema schema = Schema.builder().addStringField("wm").build();
    Row row = Row.withSchema(schema).addValue("2026-05-24T00:00:00Z").build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          try (DoFnTester<Row, Row> tester =
              DoFnTester.of(new ApplyWatermarkColumn("wm", "microseconds"))) {
            tester.processElement(row);
          }
        });
  }

  @Test
  public void missingWatermarkColumnThrows() {
    Schema schema = Schema.builder().addStringField("other").build();
    Row row = Row.withSchema(schema).addValue("value").build();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          try (DoFnTester<Row, Row> tester =
              DoFnTester.of(new ApplyWatermarkColumn("wm", "microseconds"))) {
            tester.processElement(row);
          }
        });
  }

  private void assertWatermarkTimestamp(
      String name, Schema schema, Row row, Instant expectedTimestamp) {
    PCollection<Row> output =
        pipeline
            .apply(name + "Create", Create.of(row).withCoder(RowCoder.of(schema)))
            .apply(
                name + "ApplyWatermark", ParDo.of(new ApplyWatermarkColumn("wm", "microseconds")));
    output.setCoder(RowCoder.of(schema));

    PCollection<TimestampedValue<Row>> timestamps =
        output.apply(name + "ReifyTimestamp", Reify.timestamps());
    PAssert.that(timestamps).containsInAnyOrder(TimestampedValue.of(row, expectedTimestamp));
  }
}
