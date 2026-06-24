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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
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
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/**
 * Tests for {@link BigQueryIO#readDynamically(SerializableFunction, Coder)} limited to direct read.
 */
@RunWith(JUnit4.class)
public class BigQueryIODynamicReadTest {

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private static final String AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"RowRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"number\", \"type\": \"long\"}\n"
          + " ]\n"
          + "}";
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
  private static final String TRIMMED_AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + "\"type\": \"record\",\n"
          + "\"name\": \"RowRecord\",\n"
          + "\"fields\": [\n"
          + "    {\"name\": \"name\", \"type\": \"string\"}\n"
          + " ]\n"
          + "}";
  private static final Schema TRIMMED_AVRO_SCHEMA =
      new Schema.Parser().parse(TRIMMED_AVRO_SCHEMA_STRING);
  private static final TableSchema TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED")));
  private static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA =
      new org.apache.arrow.vector.types.pojo.Schema(
          asList(
              field("name", new ArrowType.Utf8()), field("number", new ArrowType.Int(64, true))));
  private final transient TemporaryFolder testFolder = new TemporaryFolder();
  private final FakeDatasetService fakeDatasetService = new FakeDatasetService();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient GcpOptions options;
  private transient TestPipeline p;

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(Statement base, Description description) {
          // We need to set up the temporary folder, and then set up the TestPipeline based on the
          // chosen folder. Unfortunately, since rule evaluation order is unspecified and unrelated
          // to field order, and is separate from construction, that requires manually creating this
          // TestRule.
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
                  options.as(BigQueryOptions.class).setProject("project-id");
                  if (description.getAnnotations().stream()
                      .anyMatch(a -> a.annotationType().equals(ProjectOverride.class))) {
                    options.as(BigQueryOptions.class).setBigQueryProject("bigquery-project-id");
                  }
                  options
                      .as(BigQueryOptions.class)
                      .setTempLocation(testFolder.getRoot().getAbsolutePath());
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  private BufferAllocator allocator;

  private static ByteString serializeArrowSchema(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(
          new WriteChannel(Channels.newChannel(byteOutputStream)), arrowSchema);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to serialize arrow schema.", ex);
    }
    return ByteString.copyFrom(byteOutputStream.toByteArray());
  }

  private static ReadRowsResponse createResponse(
      Schema schema,
      Collection<GenericRecord> genericRecords,
      double progressAtResponseStart,
      double progressAtResponseEnd)
      throws Exception {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder binaryEncoder = ENCODER_FACTORY.binaryEncoder(outputStream, null);
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

  private static GenericRecord createRecord(String name, Schema schema) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("name", name);
    return genericRecord;
  }

  private static GenericRecord createRecord(String name, long number, Schema schema) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("name", name);
    genericRecord.put("number", number);
    return genericRecord;
  }

  private static org.apache.arrow.vector.types.pojo.Field field(
      String name,
      boolean nullable,
      ArrowType type,
      org.apache.arrow.vector.types.pojo.Field... children) {
    return new org.apache.arrow.vector.types.pojo.Field(
        name,
        new org.apache.arrow.vector.types.pojo.FieldType(nullable, type, null, null),
        asList(children));
  }

  static org.apache.arrow.vector.types.pojo.Field field(
      String name, ArrowType type, org.apache.arrow.vector.types.pojo.Field... children) {
    return field(name, false, type, children);
  }

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testCreateWithQuery() {
    String query = "SELECT * FROM dataset.table";
    Boolean flattenResults = true;
    Boolean legacySql = false;

    BigQueryDynamicReadDescriptor descriptor =
        BigQueryDynamicReadDescriptor.create(query, null, flattenResults, legacySql, null, null);

    assertNotNull(descriptor);
  }

  @Test
  public void testCreateWithTable() {
    String table = "dataset.table";

    BigQueryDynamicReadDescriptor descriptor =
        BigQueryDynamicReadDescriptor.create(null, table, null, null, null, null);

    assertNotNull(descriptor);
  }

  @Test
  public void testCreateWithTableAndSelectedFieldsAndRowRestriction() {
    String table = "dataset.table";
    List<String> selectedFields = Arrays.asList("field1", "field2");
    String rowRestriction = "field1 > 10";

    BigQueryDynamicReadDescriptor descriptor =
        BigQueryDynamicReadDescriptor.create(
            null, table, null, null, selectedFields, rowRestriction);

    assertNotNull(descriptor);
  }

  @Test
  public void testCreateWithNullQueryAndTableShouldThrowException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(null, null, null, null, null, null));
  }

  @Test
  public void testCreateWithBothQueryAndTableShouldThrowException() {
    String query = "SELECT * FROM dataset.table";
    String table = "dataset.table";
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(query, table, null, null, null, null));
  }

  @Test
  public void testCreateWithTableAndFlattenResultsShouldThrowException() {
    String table = "dataset.table";
    Boolean flattenResults = true;
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(null, table, flattenResults, null, null, null));
  }

  @Test
  public void testCreateWithTableAndLegacySqlShouldThrowException() {
    String table = "dataset.table";
    Boolean legacySql = true;
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(null, table, null, legacySql, null, null));
  }

  @Test
  public void testCreateWithQueryAndSelectedFieldsShouldThrowException() {
    String query = "SELECT * FROM dataset.table";
    Boolean flattenResults = true;
    Boolean legacySql = false;
    List<String> selectedFields = Arrays.asList("field1", "field2");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            BigQueryDynamicReadDescriptor.create(
                query, null, flattenResults, legacySql, selectedFields, null));
  }

  @Test
  public void testCreateWithQueryAndRowRestrictionShouldThrowException() {
    String query = "SELECT * FROM dataset.table";
    Boolean flattenResults = true;
    Boolean legacySql = false;
    String rowRestriction = "field1 > 10";

    assertThrows(
        IllegalArgumentException.class,
        () ->
            BigQueryDynamicReadDescriptor.create(
                query, null, flattenResults, legacySql, null, rowRestriction));
  }

  @Test
  public void testCreateWithQueryAndNullFlattenResultsShouldThrowException() {
    String query = "SELECT * FROM dataset.table";
    Boolean legacySql = false;

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(query, null, null, legacySql, null, null));
  }

  @Test
  public void testCreateWithQueryAndNullLegacySqlShouldThrowException() {
    String query = "SELECT * FROM dataset.table";
    Boolean flattenResults = true;

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryDynamicReadDescriptor.create(query, null, flattenResults, null, null, null));
  }

  @Test
  public void testCoderInference() {
    // Lambdas erase too much type information -- use an anonymous class here.
    SerializableFunction<SchemaAndRecord, KV<ByteString, ReadSession>> parseFn =
        new SerializableFunction<SchemaAndRecord, KV<ByteString, ReadSession>>() {
          @Override
          public KV<ByteString, ReadSession> apply(SchemaAndRecord input) {
            return null;
          }
        };

    assertEquals(
        KvCoder.of(ByteStringCoder.of(), ProtoCoder.of(ReadSession.class)),
        BigQueryIO.read(parseFn).inferCoder(CoderRegistry.createDefault()));
  }

  private ReadRowsResponse createResponseArrow(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema,
      List<String> name,
      List<Long> number,
      double progressAtResponseStart,
      double progressAtResponseEnd) {
    ArrowRecordBatch serializedRecord;
    try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)) {
      schemaRoot.allocateNew();
      schemaRoot.setRowCount(name.size());
      VarCharVector strVector = (VarCharVector) schemaRoot.getFieldVectors().get(0);
      BigIntVector bigIntVector = (BigIntVector) schemaRoot.getFieldVectors().get(1);
      for (int i = 0; i < name.size(); i++) {
        bigIntVector.set(i, number.get(i));
        strVector.set(i, new Text(name.get(i)));
      }

      VectorUnloader unLoader = new VectorUnloader(schemaRoot);
      try (org.apache.arrow.vector.ipc.message.ArrowRecordBatch records =
          unLoader.getRecordBatch()) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
          MessageSerializer.serialize(new WriteChannel(Channels.newChannel(os)), records);
          serializedRecord =
              ArrowRecordBatch.newBuilder()
                  .setRowCount(records.getLength())
                  .setSerializedRecordBatch(ByteString.copyFrom(os.toByteArray()))
                  .build();
        } catch (IOException e) {
          throw new RuntimeException("Error writing to byte array output stream", e);
        }
      }
    }

    return ReadRowsResponse.newBuilder()
        .setArrowRecordBatch(serializedRecord)
        .setRowCount(name.size())
        .setStats(
            StreamStats.newBuilder()
                .setProgress(
                    Progress.newBuilder()
                        .setAtResponseStart(progressAtResponseStart)
                        .setAtResponseEnd(progressAtResponseEnd)))
        .build();
  }

  private static final class ParseKeyValue
      implements SerializableFunction<SchemaAndRecord, KV<String, Long>> {

    @Override
    public KV<String, Long> apply(SchemaAndRecord input) {
      return KV.of(
          input.getRecord().get("name").toString(), (Long) input.getRecord().get("number"));
    }
  }

  @Test
  public void testReadFromBigQueryIO() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setDataFormat(DataFormat.AVRO)
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder()))
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
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    PCollection<KV<String, Long>> output =
        p.apply(
                Create.of(
                    BigQueryDynamicReadDescriptor.create(
                        null, "foo.com:project:dataset.table", null, null, null, null)))
            .apply(
                BigQueryIO.readDynamically(
                        new ParseKeyValue(), KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                    .withFormat(DataFormat.AVRO)
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(fakeDatasetService)
                            .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of("A", 1L), KV.of("B", 2L), KV.of("C", 3L), KV.of("D", 4L)));

    p.run();
  }

  @Test
  public void testReadFromBigQueryIOWithTrimmedSchema() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setReadOptions(
                        ReadSession.TableReadOptions.newBuilder().addSelectedFields("name"))
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
        Lists.newArrayList(
            createRecord("A", TRIMMED_AVRO_SCHEMA),
            createRecord("B", TRIMMED_AVRO_SCHEMA),
            createRecord("C", TRIMMED_AVRO_SCHEMA),
            createRecord("D", TRIMMED_AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(2, 4), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    PCollection<TableRow> output =
        p.apply(
                Create.of(
                    BigQueryDynamicReadDescriptor.create(
                        null,
                        "foo.com:project:dataset.table",
                        null,
                        null,
                        Lists.newArrayList("name"),
                        null)))
            .apply(
                BigQueryIO.readDynamicallyTableRows()
                    .withFormat(DataFormat.AVRO)
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(fakeDatasetService)
                            .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                new TableRow().set("name", "A"),
                new TableRow().set("name", "B"),
                new TableRow().set("name", "C"),
                new TableRow().set("name", "D")));

    p.run();
  }

  @Test
  public void testReadFromBigQueryIOArrow() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setDataFormat(DataFormat.ARROW)
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder()))
            .setMaxStreamCount(10)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setArrowSchema(
                ArrowSchema.newBuilder()
                    .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                    .build())
            .addStreams(ReadStream.newBuilder().setName("streamName"))
            .setDataFormat(DataFormat.ARROW)
            .build();

    ReadRowsRequest expectedReadRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream("streamName").build();

    List<String> names = Arrays.asList("A", "B", "C", "D");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L);
    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.50),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    PCollection<KV<String, Long>> output =
        p.apply(
                Create.of(
                    BigQueryDynamicReadDescriptor.create(
                        null, "foo.com:project:dataset.table", null, null, null, null)))
            .apply(
                BigQueryIO.readDynamically(
                        new ParseKeyValue(), KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                    .withFormat(DataFormat.ARROW)
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(fakeDatasetService)
                            .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of("A", 1L), KV.of("B", 2L), KV.of("C", 3L), KV.of("D", 4L)));

    p.run();
  }

  private FakeJobService fakeJobService = new FakeJobService();

  public PCollection<KV<String, Long>> configureDynamicRead(
      Pipeline p,
      SerializableFunction<SchemaAndRecord, KV<String, Long>> parseFn,
      ErrorHandler<BadRecord, PCollection<Long>> errorHandler)
      throws Exception {
    TableReference sourceTableRef = BigQueryHelpers.parseTableSpec("project:dataset.table");

    fakeDatasetService.createDataset(
        sourceTableRef.getProjectId(),
        sourceTableRef.getDatasetId(),
        "asia-northeast1",
        "Fake plastic tree^H^H^H^Htables",
        null);

    fakeDatasetService.createTable(
        new Table().setTableReference(sourceTableRef).setLocation("asia-northeast1"));

    Table queryResultTable = new Table().setSchema(TABLE_SCHEMA).setNumBytes(0L);

    String encodedQuery = FakeBigQueryServices.encodeQueryResult(queryResultTable);

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        encodedQuery,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of(sourceTableRef))));

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
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.500),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.5, 0.875));

    //
    // Note that since the temporary table name is generated by the pipeline, we can't match the
    // expected create read session request exactly. For now, match against any appropriately typed
    // proto object.
    //

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(any())).thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    BigQueryIO.DynamicRead<KV<String, Long>> t =
        BigQueryIO.readDynamically(parseFn, KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .withTestServices(
                new FakeBigQueryServices()
                    .withDatasetService(fakeDatasetService)
                    .withJobService(fakeJobService)
                    .withStorageClient(fakeStorageClient));
    if (errorHandler != null) {
      t = t.withBadRecordErrorHandler(errorHandler);
    }
    return p.apply(
            Create.of(
                BigQueryDynamicReadDescriptor.create(encodedQuery, null, false, false, null, null)))
        .apply("read", t);
  }

  @Test
  public void testReadQueryFromBigQueryIO() throws Exception {
    PCollection<KV<String, Long>> output = configureDynamicRead(p, new ParseKeyValue(), null);

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of("A", 1L), KV.of("B", 2L), KV.of("C", 3L), KV.of("D", 4L)));

    p.run();
  }

  private static final class FailingParseKeyValue
      implements SerializableFunction<SchemaAndRecord, KV<String, Long>> {
    @Override
    public KV<String, Long> apply(SchemaAndRecord input) {
      if (input.getRecord().get("name").toString().equals("B")) {
        throw new RuntimeException("ExpectedException");
      }
      return KV.of(
          input.getRecord().get("name").toString(), (Long) input.getRecord().get("number"));
    }
  }

  @Test
  public void testReadFromBigQueryWithExceptionHandling() throws Exception {
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorHandlingTestUtils.ErrorSinkTransform());
    PCollection<KV<String, Long>> output =
        configureDynamicRead(p, new FailingParseKeyValue(), errorHandler);

    errorHandler.close();

    PAssert.that(output)
        .containsInAnyOrder(ImmutableList.of(KV.of("A", 1L), KV.of("C", 3L), KV.of("D", 4L)));

    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(1L);

    p.run();
  }
}
