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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.services.bigquery.model.Streamingbuffer;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroRows;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroSchema;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
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

/** Tests for {@link BigQueryIO#readTableRows() using {@link Method#DIRECT_READ}}. */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadTest {
  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
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
                  options = TestPipeline.testingPipelineOptions();
                  options.as(BigQueryOptions.class).setProject("project-id");
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

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
  }

  @Test
  public void testBuildTableBasedSource() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table");
    checkTypedReadTableObject(typedRead, "foo.com:project", "dataset", "table");
    assertTrue(typedRead.getValidate());
  }

  @Test
  public void testBuildTableBasedSourceWithoutValidation() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withoutValidation();
    checkTypedReadTableObject(typedRead, "foo.com:project", "dataset", "table");
    assertFalse(typedRead.getValidate());
  }

  @Test
  public void testBuildTableBasedSourceWithDefaultProject() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("myDataset.myTable");
    checkTypedReadTableObject(typedRead, null, "myDataset", "myTable");
  }

  @Test
  public void testBuildTableBasedSourceWithReadOptions() {
    TableReadOptions readOptions =
        TableReadOptions.newBuilder()
            .addSelectedFields("field1")
            .addSelectedFields("field2")
            .setRowRestriction("int_field > 5")
            .build();
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withReadOptions(readOptions);
    checkTypedReadTableObject(typedRead, "foo.com:project", "dataset", "table");
    assertEquals(typedRead.getReadOptions(), readOptions);
  }

  @Test
  public void testBuildTableBasedSourceWithTableReference() {
    TableReference tableReference =
        new TableReference()
            .setProjectId("foo.com:project")
            .setDatasetId("dataset")
            .setTableId("table");
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from(tableReference);
    checkTypedReadTableObject(typedRead, "foo.com:project", "dataset", "table");
  }

  private void checkTypedReadTableObject(
      TypedRead typedRead, String project, String dataset, String table) {
    assertEquals(project, typedRead.getTable().getProjectId());
    assertEquals(dataset, typedRead.getTable().getDatasetId());
    assertEquals(table, typedRead.getTable().getTableId());
    assertNull(typedRead.getQuery());
    assertEquals(Method.DIRECT_READ, typedRead.getMethod());
  }

  @Test
  public void testBuildSourceWithTableAndFlatten() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a result flattening preference,"
            + " which only applies to queries");
    p.apply(
        "ReadMyTable",
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withoutResultFlattening());
    p.run();
  }

  @Test
  public void testBuildSourceWithTableAndSqlDialect() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies a table with a SQL dialect preference,"
            + " which only applies to queries");
    p.apply(
        "ReadMyTable",
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .usingStandardSql());
    p.run();
  }

  @Test
  public void testDisplayData() {
    String tableSpec = "foo.com:project:dataset.table";
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from(tableSpec);
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("table", tableSpec));
  }

  @Test
  public void testEvaluatedDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table");
    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(typedRead);
    assertThat(displayData, hasItem(hasDisplayItem("table")));
  }

  @Test
  public void testName() {
    assertEquals(
        "BigQueryIO.TypedRead",
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .getName());
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

  @Test
  public void testTableSourceEstimatedSize() throws Exception {
    doTableSourceEstimatedSizeTest(false);
  }

  @Test
  public void testTableSourceEstimatedSize_IgnoresStreamingBuffer() throws Exception {
    doTableSourceEstimatedSizeTest(true);
  }

  private void doTableSourceEstimatedSizeTest(boolean useStreamingBuffer) throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(100L);
    if (useStreamingBuffer) {
      table.setStreamingBuffer(new Streamingbuffer().setEstimatedBytes(BigInteger.TEN));
    }

    fakeDatasetService.createTable(table);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    assertEquals(100, tableSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testTableSourceEstimatedSize_WithDefaultProject() throws Exception {
    fakeDatasetService.createDataset("project-id", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(100L);
    fakeDatasetService.createTable(table);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(BigQueryHelpers.parseTableSpec("dataset.table")),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    assertEquals(100, tableSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testTableSourceInitialSplit() throws Exception {
    doTableSourceInitialSplitTest(1024L, 1024);
  }

  @Test
  public void testTableSourceInitialSplit_MinSplitCount() throws Exception {
    doTableSourceInitialSplitTest(1024L * 1024L, 10);
  }

  @Test
  public void testTableSourceInitialSplit_MaxSplitCount() throws Exception {
    doTableSourceInitialSplitTest(10L, 10_000);
  }

  private void doTableSourceInitialSplitTest(long bundleSize, int streamCount) throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table()
            .setTableReference(tableRef)
            .setNumBytes(1024L * 1024L)
            .setSchema(new TableSchema());

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(streamCount)
            .build();

    ReadSession.Builder builder = ReadSession.newBuilder();
    for (int i = 0; i < streamCount; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(bundleSize, options);
    assertEquals(streamCount, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithTableReadOptions() throws Throwable {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table()
            .setTableReference(tableRef)
            .setNumBytes(100L)
            .setSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("name").setType("STRING"),
                            new TableFieldSchema().setName("number").setType("INTEGER"))));

    fakeDatasetService.createTable(table);

    TableReadOptions readOptions =
        TableReadOptions.newBuilder()
            .addSelectedFields("name")
            .addSelectedFields("number")
            .setRowRestriction("number > 5")
            .build();

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .setReadOptions(readOptions)
            .build();

    ReadSession.Builder builder = ReadSession.newBuilder();
    for (int i = 0; i < 10; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            readOptions,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(10L, options);
    assertEquals(10L, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithDefaultProject() throws Exception {
    fakeDatasetService.createDataset("project-id", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset.table");

    Table table =
        new Table()
            .setTableReference(tableRef)
            .setNumBytes(1024L * 1024L)
            .setSchema(new TableSchema());

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(1024)
            .build();

    ReadSession.Builder builder = ReadSession.newBuilder();
    for (int i = 0; i < 50; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(BigQueryHelpers.parseTableSpec("dataset.table")),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(1024L, options);
    assertEquals(50L, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_EmptyTable() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table()
            .setTableReference(tableRef)
            .setNumBytes(1024L * 1024L)
            .setSchema(new TableSchema());

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(1024)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(1024L, options);
    assertTrue(sources.isEmpty());
  }

  @Test
  public void testTableSourceCreateReader() throws Exception {
    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(
                BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table")),
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("BigQuery storage source must be split before reading");
    tableSource.createReader(options);
  }

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

  private static final TableSchema TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"),
                  new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED")));

  private static GenericRecord createRecord(String name, long number, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("name", name);
    genericRecord.put("number", number);
    return genericRecord;
  }

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  private static ReadRowsResponse createResponse(
      Schema schema, Collection<GenericRecord> genericRecords) throws Exception {
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
        .build();
  }

  @Test
  public void testStreamSourceEstimatedSizeBytes() throws Exception {

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.getDefaultInstance(),
            Stream.getDefaultInstance(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices());

    assertEquals(0, streamSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testStreamSourceSplit() throws Exception {

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.getDefaultInstance(),
            Stream.getDefaultInstance(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices());

    assertThat(streamSource.split(0, options), containsInAnyOrder(streamSource));
  }

  @Test
  public void testReadFromStreamSource() throws Exception {

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    Stream stream = Stream.newBuilder().setName("stream").build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder()
            .setReadPosition(StreamPosition.newBuilder().setStream(stream))
            .build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2)),
            createResponse(AVRO_SCHEMA, records.subList(2, 3)));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest)).thenReturn(responses);

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            stream,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    List<TableRow> rows = new ArrayList<>();
    BoundedReader<TableRow> reader = streamSource.createReader(options);
    for (boolean hasNext = reader.start(); hasNext; hasNext = reader.advance()) {
      rows.add(reader.getCurrent());
    }

    System.out.println("Rows: " + rows);

    assertEquals(3, rows.size());
  }

  @Test
  public void testStreamSourceSplitAtFraction() throws Exception {

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    Stream stream = Stream.newBuilder().setName("stream").build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder()
            .setReadPosition(StreamPosition.newBuilder().setStream(stream))
            .build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2)),
            createResponse(AVRO_SCHEMA, records.subList(2, 3)));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest)).thenReturn(responses);

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            stream,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<TableRow> reader = streamSource.createReader(options);
    reader.start();
    assertNull(reader.splitAtFraction(0.5));
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

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(new TableSchema());

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .addStreams(Stream.newBuilder().setName("streamName"))
            .build();

    ReadRowsRequest expectedReadRowsRequest =
        ReadRowsRequest.newBuilder()
            .setReadPosition(
                StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("streamName")))
            .build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2)),
            createResponse(AVRO_SCHEMA, records.subList(2, 4)));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest)).thenReturn(readRowsResponses);

    PCollection<KV<String, Long>> output =
        p.apply(
            BigQueryIO.read(new ParseKeyValue())
                .from("foo.com:project:dataset.table")
                .withMethod(Method.DIRECT_READ)
                .withTestServices(
                    new FakeBigQueryServices()
                        .withDatasetService(fakeDatasetService)
                        .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of("A", 1L), KV.of("B", 2L), KV.of("C", 3L), KV.of("D", 4L)));

    p.run();
  }
}
