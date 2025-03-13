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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.SpannerChangestreamsReadSchemaTransformProvider.DataChangeRecordToRow;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerChangestreamsReadDlqTest {
  private static final TupleTag<Row> OUTPUTTAG =
      SpannerChangestreamsReadSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERRORTAG =
      SpannerChangestreamsReadSchemaTransformProvider.ERROR_TAG;

  private static final String TABLENAME = "test_table";

  private static final Schema tableSchema =
      Schema.builder().addNullableInt32Field("id").addNullableStringField("name").build();
  private static final Schema tableChangeRecordSchema =
      Schema.builder()
          .addStringField("operation")
          .addStringField("commitTimestamp")
          .addInt64Field("recordSequence")
          .addRowField("rowValues", tableSchema)
          .build();

  private static final Schema ERRORSCHEMA =
      SpannerChangestreamsReadSchemaTransformProvider.ERROR_SCHEMA;

  private static final Row record =
      Row.withSchema(tableSchema).withFieldValue("id", 0).withFieldValue("name", "test").build();

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(tableChangeRecordSchema)
              .withFieldValue("operation", "INSERT")
              .withFieldValue("commitTimestamp", "2023-06-01T18:14:22.078159000Z")
              .withFieldValue("recordSequence", 1L)
              .withFieldValue("rowValues", record)
              .build());

  private static final Mod MOD =
      new Mod("{\r\n\"id\": \"0\"\r\n}", null, "{\r\n\"id\": \"0\",\r\n\"name\": \"test\" \r\n}");

  private static final Mod INCORRECTMOD =
      new Mod(
          "{\r\n\"id\": \"0\"\r\n}",
          null,
          "{\r\n\"id\": \"0\",\r\n\"name\": \"test\" \r\n,\r\n\"lastName\": \"test\" \r\n}");

  private static final List<DataChangeRecord> message =
      Arrays.asList(
          new DataChangeRecord(
              "token",
              Timestamp.parseTimestamp("2023-06-01T18:14:22.078159000Z"),
              "id",
              false,
              "1",
              TABLENAME,
              Collections.emptyList(),
              Collections.singletonList(MOD),
              ModType.INSERT,
              ValueCaptureType.NEW_VALUES,
              1L,
              0L,
              "tag",
              false,
              null));

  private static final List<DataChangeRecord> incorrectmessage =
      Arrays.asList(
          new DataChangeRecord(
              "token",
              Timestamp.parseTimestamp("2023-06-01T18:14:22.078159000Z"),
              "id",
              false,
              "1",
              TABLENAME,
              Collections.emptyList(),
              Collections.singletonList(INCORRECTMOD),
              ModType.INSERT,
              ValueCaptureType.NEW_VALUES,
              1L,
              0L,
              "tag",
              false,
              null));

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testSpannerChangestreamsReadDataChangeRecordToRowSuccess() throws Exception {
    PCollection<DataChangeRecord> input = p.apply(Create.of(message));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new DataChangeRecordToRow(
                        TABLENAME,
                        tableChangeRecordSchema,
                        "SpannerChangestreams-read-error-counter"))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(tableChangeRecordSchema);
    output.get(ERRORTAG).setRowSchema(ERRORSCHEMA);

    PAssert.that(output.get(OUTPUTTAG)).containsInAnyOrder(ROWS);
    p.run().waitUntilFinish();
  }

  @Test
  public void testSpannerChangestreamsReadDataChangeRecordToRowFailure() throws Exception {
    PCollection<DataChangeRecord> input = p.apply(Create.of(incorrectmessage));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new DataChangeRecordToRow(
                        TABLENAME,
                        tableChangeRecordSchema,
                        "SpannerChangestreams-read-error-counter"))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(tableChangeRecordSchema);
    output.get(ERRORTAG).setRowSchema(ERRORSCHEMA);

    PCollection<Long> count = output.get(ERRORTAG).apply(Count.globally());

    PAssert.that(count).containsInAnyOrder(Collections.singletonList(1L));

    p.run().waitUntilFinish();
  }
}
