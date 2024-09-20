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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.bigquery.storage.v1.StreamStats.Progress;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransform;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransformConfiguration;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryDirectReadSchemaTransformProviderTest {

  private static PipelineOptions testOptions = TestPipeline.testingPipelineOptions();

  private final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private final FakeJobService fakeJobService = new FakeJobService();
  private final FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withJobService(fakeJobService)
          .withDatasetService(fakeDatasetService);

  private static final String TABLE_SPEC = "my-project:dataset.table";

  private static final String AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"RowRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"str\", \"type\": \"string\"},\n"
          + "     {\"name\": \"num\", \"type\": \"long\"},\n"
          + "     {\"name\": \"dt\", \"type\": \"string\", \"logicalType\": \"datetime\"}\n"
          + " ]\n"
          + "}";

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_STRING);

  private static final org.apache.beam.sdk.schemas.Schema SCHEMA =
      org.apache.beam.sdk.schemas.Schema.of(
          Field.of("str", FieldType.STRING),
          Field.of("num", FieldType.INT64),
          Field.of("dt", FieldType.logicalType(SqlTypes.DATETIME)));
  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA)
              .withFieldValue("str", "a")
              .withFieldValue("num", 1L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-01T00:00:00"))
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("str", "b")
              .withFieldValue("num", 2L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-02T00:00:00"))
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("str", "c")
              .withFieldValue("num", 3L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-03T00:00:00"))
              .build());

  private static final String TRIMMED_AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"RowRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"str\", \"type\": \"string\"}\n"
          + " ]\n"
          + "}";

  private static final Schema TRIMMED_AVRO_SCHEMA =
      new Schema.Parser().parse(TRIMMED_AVRO_SCHEMA_STRING);

  private static final org.apache.beam.sdk.schemas.Schema TRIMMED_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.of(Field.of("str", FieldType.STRING));
  private static final List<Row> TRIMMED_ROWS =
      Arrays.asList(
          Row.withSchema(TRIMMED_SCHEMA).withFieldValue("str", "b").build(),
          Row.withSchema(TRIMMED_SCHEMA).withFieldValue("str", "c").build());

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();

    TableSchema tableSchema = BigQueryUtils.toTableSchema(SCHEMA);
    TableReference ref = BigQueryHelpers.parseTableSpec(TABLE_SPEC);
    Table table = new Table().setTableReference(ref).setNumBytes(10L).setSchema(tableSchema);
    fakeDatasetService.createDataset("my-project", "dataset", "", "test_dataset", null);
    fakeDatasetService.createTable(table);

    testOptions.as(BigQueryOptions.class).setProject("parent-project");
  }

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(testOptions);

  private static GenericRecord createRecord(String str, long num, String dt, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("str", str);
    genericRecord.put("num", num);
    genericRecord.put("dt", dt);
    return genericRecord;
  }

  private static GenericRecord createRecord(String str, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("str", str);
    return genericRecord;
  }

  private static ReadRowsResponse createResponse(
      Schema schema,
      Collection<GenericRecord> genericRecords,
      double progressAtResponseStart,
      double progressAtResponseEnd)
      throws Exception {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    for (GenericRecord genericRecord : genericRecords) {
      writer.write(genericRecord, binaryEncoder);
    }

    binaryEncoder.flush();

    return ReadRowsResponse.newBuilder()
        .setAvroRows(
            AvroRows.newBuilder()
                .setSerializedBinaryRows(ByteString.copyFrom(outputStream.toByteArray()))
                .setRowCount(genericRecords.size()))
        .setRowCount(genericRecords.size())
        .setStats(
            StreamStats.newBuilder()
                .setProgress(
                    Progress.newBuilder()
                        .setAtResponseStart(progressAtResponseStart)
                        .setAtResponseEnd(progressAtResponseEnd)))
        .build();
  }

  @Test
  public void testValidateConfig() {
    List<BigQueryDirectReadSchemaTransformConfiguration> invalidConfigs =
        Arrays.asList(
            BigQueryDirectReadSchemaTransformConfiguration.builder()
                .setQuery("SELECT * FROM project:dataset.table")
                .setTableSpec("project:dataset.table")
                .build(),
            BigQueryDirectReadSchemaTransformConfiguration.builder()
                .setQuery("SELECT * FROM project:dataset.table")
                .setRowRestriction("num > 10")
                .build(),
            BigQueryDirectReadSchemaTransformConfiguration.builder()
                .setTableSpec("not a table spec")
                .build());

    for (BigQueryDirectReadSchemaTransformConfiguration config : invalidConfigs) {
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            config.validate();
          });
    }
  }

  @Test
  public void testDirectRead() throws Exception {
    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/parent-project")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/my-project/datasets/dataset/tables/table")
                    .setDataFormat(DataFormat.AVRO)
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder().build()))
            .setMaxStreamCount(10)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .addStreams(ReadStream.newBuilder().setName("streamName"))
            .setDataFormat(DataFormat.AVRO)
            .build();

    ReadRowsRequest expectedReadRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream("streamName").build();

    List<GenericRecord> records =
        Arrays.asList(
            createRecord("a", 1L, "2000-01-01T00:00:00", AVRO_SCHEMA),
            createRecord("b", 2L, "2000-01-02T00:00:00", AVRO_SCHEMA),
            createRecord("c", 3L, "2000-01-03T00:00:00", AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Arrays.asList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 3), 0.50, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    BigQueryDirectReadSchemaTransformConfiguration config =
        BigQueryDirectReadSchemaTransformConfiguration.builder().setTableSpec(TABLE_SPEC).build();
    BigQueryDirectReadSchemaTransformProvider provider =
        new BigQueryDirectReadSchemaTransformProvider();
    BigQueryDirectReadSchemaTransform readTransform =
        (BigQueryDirectReadSchemaTransform) provider.from(config);
    PCollectionRowTuple input = PCollectionRowTuple.empty(p);
    String tag = provider.outputCollectionNames().get(0);

    readTransform.setBigQueryServices(fakeBigQueryServices.withStorageClient(fakeStorageClient));
    PCollectionRowTuple output = input.apply(readTransform);
    assertTrue(output.has(tag));
    PCollection<Row> rows = output.get(tag);
    PAssert.that(rows).containsInAnyOrder(ROWS);

    p.run().waitUntilFinish();
  }

  @Test
  public void testDirectReadWithSelectedFieldsAndRowRestriction() throws Exception {
    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/parent-project")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/my-project/datasets/dataset/tables/table")
                    .setReadOptions(
                        ReadSession.TableReadOptions.newBuilder()
                            .addSelectedFields("str")
                            .setRowRestriction("num > 1"))
                    .setDataFormat(DataFormat.AVRO))
            .setMaxStreamCount(10)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .addStreams(ReadStream.newBuilder().setName("streamName"))
            .setDataFormat(DataFormat.AVRO)
            .build();

    ReadRowsRequest expectedReadRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream("streamName").build();

    List<GenericRecord> records =
        Arrays.asList(
            createRecord("b", TRIMMED_AVRO_SCHEMA), createRecord("c", TRIMMED_AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Arrays.asList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(0, 1), 0.0, 0.50),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(1, 2), 0.50, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    BigQueryDirectReadSchemaTransformConfiguration config =
        BigQueryDirectReadSchemaTransformConfiguration.builder()
            .setTableSpec(TABLE_SPEC)
            .setSelectedFields(Arrays.asList("str"))
            .setRowRestriction("num > 1")
            .build();
    BigQueryDirectReadSchemaTransformProvider provider =
        new BigQueryDirectReadSchemaTransformProvider();
    BigQueryDirectReadSchemaTransform readTransform =
        (BigQueryDirectReadSchemaTransform) provider.from(config);
    PCollectionRowTuple input = PCollectionRowTuple.empty(p);
    String tag = provider.outputCollectionNames().get(0);

    readTransform.setBigQueryServices(fakeBigQueryServices.withStorageClient(fakeStorageClient));
    PCollectionRowTuple output = input.apply(readTransform);
    assertTrue(output.has(tag));
    PCollection<Row> rows = output.get(tag);
    PAssert.that(rows).containsInAnyOrder(TRIMMED_ROWS);

    p.run().waitUntilFinish();
  }
}
