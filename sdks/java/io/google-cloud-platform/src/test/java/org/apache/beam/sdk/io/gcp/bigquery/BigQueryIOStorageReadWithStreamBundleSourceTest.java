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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.util.ArrayList;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageStreamBundleSource.BigQueryStorageStreamBundleReader;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matchers;
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
 * Tests for {@link BigQueryIO#readTableRows() using {@link Method#DIRECT_READ}} AND {@link
 * BigQueryOptions#setEnableBundling(Boolean)} (Boolean)} set to True.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadWithStreamBundleSourceTest {

  private transient PipelineOptions options;
  private final transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline p;
  private BufferAllocator allocator;

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
                  if (description.getAnnotations().stream()
                      .anyMatch(a -> a.annotationType().equals(ProjectOverride.class))) {
                    options.as(BigQueryOptions.class).setBigQueryProject("bigquery-project-id");
                  }
                  options
                      .as(BigQueryOptions.class)
                      .setTempLocation(testFolder.getRoot().getAbsolutePath());
                  options.as(BigQueryOptions.class).setEnableBundling(true);
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private final FakeDatasetService fakeDatasetService = new FakeDatasetService();

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
      TypedRead<?> typedRead, String project, String dataset, String table) {
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
            .withSelectedFields(ImmutableList.of("foo", "bar"))
            .withProjectionPushdownApplied()
            .from(tableSpec);
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("table", tableSpec));
    assertThat(displayData, hasDisplayItem("selectedFields", "foo, bar"));
    assertThat(displayData, hasDisplayItem("projectionPushdownApplied", true));
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
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    assertEquals(100, tableSource.getEstimatedSizeBytes(options));
  }

  @Test
  @ProjectOverride
  public void testTableSourceEstimatedSize_WithBigQueryProject() throws Exception {
    fakeDatasetService.createDataset("bigquery-project-id", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("bigquery-project-id:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(100L);
    fakeDatasetService.createTable(table);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(BigQueryHelpers.parseTableSpec("dataset.table")),
            null,
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
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    assertEquals(100, tableSource.getEstimatedSizeBytes(options));
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

  private void doTableSourceInitialSplitTest(long bundleSize, long tableSize, int streamCount)
      throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(tableSize).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table"))
            .setMaxStreamCount(0)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO)
            .setEstimatedTotalBytesScanned(tableSize);
    for (int i = 0; i < streamCount; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(bundleSize, options);
    // Each StreamBundle is expected to contain a single stream.
    assertEquals(streamCount, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit() throws Exception {
    doTableSourceInitialSplitTest(1024L, 1024L * 1024L, 1024);
  }

  @Test
  public void testTableSourceInitialSplit_MinSplitCount() throws Exception {
    doTableSourceInitialSplitTest(1024L, 1024L * 1024L, 10);
  }

  @Test
  public void testTableSourceInitialSplit_MaxSplitCount() throws Exception {
    doTableSourceInitialSplitTest(10L, 1024L * 1024L, 10_000);
  }

  @Test
  public void testTableSourceInitialSplit_WithSelectedFieldsAndRowRestriction() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table = new Table().setTableReference(tableRef).setNumBytes(200L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setReadOptions(
                        ReadSession.TableReadOptions.newBuilder()
                            .addSelectedFields("name")
                            .setRowRestriction("number > 5")))
            .setMaxStreamCount(0)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO)
            .setEstimatedTotalBytesScanned(100L);
    for (int i = 0; i < 10; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            StaticValueProvider.of(Lists.newArrayList("name")),
            StaticValueProvider.of("number > 5"),
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(20L, options);
    assertEquals(5, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithDefaultProject() throws Exception {
    fakeDatasetService.createDataset("project-id", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset.table");

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(1024L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/project-id/datasets/dataset/tables/table"))
            .setMaxStreamCount(0)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO)
            .setEstimatedTotalBytesScanned(1024L);
    for (int i = 0; i < 50; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(BigQueryHelpers.parseTableSpec("dataset.table")),
            null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(4096L, options);
    // A single StreamBundle containing all the Streams.
    assertEquals(1, sources.size());
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
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table"))
            .setMaxStreamCount(0)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
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
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withDatasetService(fakeDatasetService));

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("BigQuery storage source must be split before reading");
    tableSource.createReader(options);
  }

  private static GenericRecord createRecord(String name, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("name", name);
    return genericRecord;
  }

  private static GenericRecord createRecord(String name, long number, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("name", name);
    genericRecord.put("number", number);
    return genericRecord;
  }

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

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

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

  @Test
  public void testStreamSourceEstimatedSizeBytes() throws Exception {
    List<ReadStream> streamBundle = Lists.newArrayList(ReadStream.getDefaultInstance());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.getDefaultInstance(),
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices(),
            1L);

    assertEquals(0, streamSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testStreamSourceSplit() throws Exception {
    List<ReadStream> streamBundle = Lists.newArrayList(ReadStream.getDefaultInstance());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.getDefaultInstance(),
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices(),
            1L);

    assertThat(streamSource.split(0, options), containsInAnyOrder(streamSource));
  }

  @Test
  public void testReadFromStreamSource() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("readStream1").setOffset(0).build();
    ReadRowsRequest expectedRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("readStream2").setOffset(0).build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA));

    List<ReadRowsResponse> responsesOne =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 3), 0.5, 0.75));
    List<ReadRowsResponse> responsesTwo =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(3, 5), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(5, 6), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesOne));
    when(fakeStorageClient.readRows(expectedRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesTwo));

    List<ReadStream> streamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    List<TableRow> rows = new ArrayList<>();
    BigQueryStorageStreamBundleReader<TableRow> reader = streamSource.createReader(options);
    for (boolean hasNext = reader.start(); hasNext; hasNext = reader.advance()) {
      rows.add(reader.getCurrent());
    }

    System.out.println("Rows: " + rows);

    assertEquals(6, rows.size());
  }

  private static final double DELTA = 1e-6;

  @Test
  public void testFractionConsumedWithOneStreamInBundle() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA),
            createRecord("G", 7, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.25),
            // Some responses may contain zero results, so we must ensure that we can are resilient
            // to such responses.
            createResponse(AVRO_SCHEMA, Lists.newArrayList(), 0.25, 0.25),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.3, 0.5),
            createResponse(AVRO_SCHEMA, records.subList(4, 7), 0.7, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    List<ReadStream> streamBundle =
        Lists.newArrayList(ReadStream.newBuilder().setName("readStream").build());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.25, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.4, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads D.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads E.
    assertEquals(0.8, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads F.
    assertEquals(0.9, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads G.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance()); // Reaches the end.

    // We are done with the stream, so we should report 100% consumption.
    assertEquals(Double.valueOf(1.0), reader.getFractionConsumed());
  }

  @Test
  public void testFractionConsumedWithMultipleStreamsInBundle() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("readStream1").build();
    ReadRowsRequest expectedRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("readStream2").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA),
            createRecord("G", 7, AVRO_SCHEMA));

    List<ReadRowsResponse> responsesOne =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.5),
            // Some responses may contain zero results, so we must ensure that we are resilient
            // to such responses.
            createResponse(AVRO_SCHEMA, Lists.newArrayList(), 0.5, 0.5),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.5, 1.0));

    List<ReadRowsResponse> responsesTwo =
        Lists.newArrayList(createResponse(AVRO_SCHEMA, records.subList(4, 7), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesOne));
    when(fakeStorageClient.readRows(expectedRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesTwo));

    List<ReadStream> streamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build());

    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.25, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.375, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads D.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads E.
    assertEquals(0.6666666666666666, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads F.
    assertEquals(0.8333333333333333, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads G.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance()); // Reaches the end.

    // We are done with the streams, so we should report 100% consumption.
    assertEquals(Double.valueOf(1.0), reader.getFractionConsumed());
  }

  @Test
  public void testStreamSourceSplitAtFractionNoOpWithOneStreamInBundle() throws Exception {
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.0,
                0.25),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.25, 0.50),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                0.50,
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    List<ReadStream> parentStreamBundle =
        Lists.newArrayList(ReadStream.newBuilder().setName("parentStream").build());
    BigQueryStorageStreamBundleSource<TableRow> streamBundleSource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            parentStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> primary = streamBundleSource.createReader(options);
    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));

    // Now split the stream. Since we do NOT split below the granularity of a single stream,
    // this will be a No-Op and the primary source should be read to completion.
    BoundedSource<TableRow> secondary = primary.splitAtFraction(0.5);
    assertNull(secondary);

    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("D", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("E", primary.getCurrent().get("name"));
    assertFalse(primary.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionWithMultipleStreamsInBundle() throws Exception {
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.0,
                0.6),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.6, 1.0),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA),
                    createRecord("E", 5, AVRO_SCHEMA),
                    createRecord("F", 6, AVRO_SCHEMA)),
                0.0,
                1.0),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("G", 7, AVRO_SCHEMA)), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream3").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(3, 4)));

    List<ReadStream> primaryStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build(),
            ReadStream.newBuilder().setName("readStream3").build());

    BigQueryStorageStreamBundleSource<TableRow> primarySource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            primaryStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the primary Source and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> primary = primarySource.createReader(options);

    assertTrue(primary.start());

    // Attempting to split at a sub-Stream level which is NOT supported by the
    // `BigQueryStorageStreamBundleSource`. IOTW, since there are exactly 3 Streams in the Source,
    // a split will only occur for fraction > 0.33.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.05);
    assertNull(secondarySource);

    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));

    // Now split the primary Source, and ensure that the returned source points to a non-null
    // StreamBundle containing Streams 2 & 3.
    secondarySource = primary.splitAtFraction(0.5);
    assertNotNull(secondarySource);
    BoundedReader<TableRow> secondary = secondarySource.createReader(options);

    // Since the last two streams were split out the Primary source has been exhausted.
    assertFalse(primary.advance());

    assertTrue(secondary.start());
    assertEquals("D", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());
    assertEquals("E", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());
    assertEquals("F", secondary.getCurrent().get("name"));
    assertTrue((secondary.advance()));

    // Since we have already started reading from the last Stream in the StreamBundle, splitting
    // is now a no-op.
    BoundedSource<TableRow> tertiarySource = secondary.splitAtFraction(0.55);
    assertNull(tertiarySource);

    assertEquals("G", secondary.getCurrent().get("name"));
    assertFalse((secondary.advance()));
  }

  @Test
  public void testStreamSourceSplitAtFractionRepeatedWithMultipleStreamInBundle() throws Exception {
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.0,
                0.6),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.6, 1.0),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA),
                    createRecord("E", 5, AVRO_SCHEMA),
                    createRecord("F", 6, AVRO_SCHEMA)),
                0.0,
                1.0),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("G", 7, AVRO_SCHEMA)), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream3").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(3, 4)));

    List<ReadStream> primaryStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build(),
            ReadStream.newBuilder().setName("readStream3").build());

    BigQueryStorageStreamBundleSource<TableRow> primarySource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            primaryStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the primary Source and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> primary = primarySource.createReader(options);

    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));

    // Now split the primary Source, and ensure that the returned source points to a non-null
    // StreamBundle containing ONLY Stream 3. Since there are exactly 3 Streams in the Source,
    // a split will only occur for fraction > 0.33.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.7);
    assertNotNull(secondarySource);
    BoundedReader<TableRow> secondary = secondarySource.createReader(options);
    assertTrue(secondary.start());
    assertEquals("G", secondary.getCurrent().get("name"));
    assertFalse((secondary.advance()));

    // A second splitAtFraction() call on the primary source. The resulting source should
    // contain a StreamBundle containing ONLY Stream 2. Since there are 2 Streams in the Source,
    // a split will only occur for fraction > 0.50.
    BoundedSource<TableRow> tertiarySource = primary.splitAtFraction(0.55);
    assertNotNull(tertiarySource);
    BoundedReader<TableRow> tertiary = tertiarySource.createReader(options);
    assertTrue(tertiary.start());
    assertEquals("D", tertiary.getCurrent().get("name"));
    assertTrue(tertiary.advance());
    assertEquals("E", tertiary.getCurrent().get("name"));
    assertTrue(tertiary.advance());
    assertEquals("F", tertiary.getCurrent().get("name"));
    assertFalse(tertiary.advance());

    // A third attempt to split the primary source. This will be ignored since the primary source
    // since the Source contains only a single stream now and `BigQueryStorageStreamBundleSource`
    // does NOT support sub-stream splitting.
    tertiarySource = primary.splitAtFraction(0.9);
    assertNull(tertiarySource);

    // All the rows in the primary Source have been read.
    assertFalse(primary.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenParentIsPastSplitPoint() throws Exception {
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.0,
                0.66),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.66, 1.0),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                0.0,
                1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));

    List<ReadStream> parentStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build());

    BigQueryStorageStreamBundleSource<TableRow> streamBundleSource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            parentStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the parent bundle and ensure the records are returned in
    // the prescribed order.
    BoundedReader<TableRow> primary = streamBundleSource.createReader(options);
    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("D", primary.getCurrent().get("name"));

    // We attempt to split the StreamBundle after starting to read the contents of the second
    // stream.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.5);
    assertNull(secondarySource);

    assertTrue(primary.advance());
    assertEquals("E", primary.getCurrent().get("name"));
    assertFalse(primary.advance());
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
                    .setDataFormat(DataFormat.AVRO))
            .setMaxStreamCount(0)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .addStreams(ReadStream.newBuilder().setName("streamName1"))
            .addStreams(ReadStream.newBuilder().setName("streamName2"))
            .setDataFormat(DataFormat.AVRO)
            .setEstimatedTotalBytesScanned(10L)
            .build();

    ReadRowsRequest expectedReadRowsRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("streamName1").build();
    ReadRowsRequest expectedReadRowsRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("streamName2").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA),
            createRecord("G", 7, AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponsesOne =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.5, 1.0));
    List<ReadRowsResponse> readRowsResponsesTwo =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(4, 5), 0.0, 0.33),
            createResponse(AVRO_SCHEMA, records.subList(5, 7), 0.33, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesOne));
    when(fakeStorageClient.readRows(expectedReadRowsRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesTwo));

    PCollection<KV<String, Long>> output =
        p.apply(
            BigQueryIO.read(new ParseKeyValue())
                .from("foo.com:project:dataset.table")
                .withMethod(Method.DIRECT_READ)
                .withFormat(DataFormat.AVRO)
                .withTestServices(
                    new FakeBigQueryServices()
                        .withDatasetService(fakeDatasetService)
                        .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of("A", 1L),
                KV.of("B", 2L),
                KV.of("C", 3L),
                KV.of("D", 4L),
                KV.of("E", 5L),
                KV.of("F", 6L),
                KV.of("G", 7L)));

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
            .setMaxStreamCount(0)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .addStreams(ReadStream.newBuilder().setName("streamName1"))
            .addStreams(ReadStream.newBuilder().setName("streamName2"))
            .setDataFormat(DataFormat.AVRO)
            .build();

    ReadRowsRequest expectedReadRowsRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("streamName1").build();
    ReadRowsRequest expectedReadRowsRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("streamName2").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", TRIMMED_AVRO_SCHEMA),
            createRecord("B", TRIMMED_AVRO_SCHEMA),
            createRecord("C", TRIMMED_AVRO_SCHEMA),
            createRecord("D", TRIMMED_AVRO_SCHEMA),
            createRecord("E", TRIMMED_AVRO_SCHEMA),
            createRecord("F", TRIMMED_AVRO_SCHEMA),
            createRecord("G", TRIMMED_AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponsesOne =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(2, 4), 0.5, 0.75));
    List<ReadRowsResponse> readRowsResponsesTwo =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(4, 5), 0.0, 0.33),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(5, 7), 0.33, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesOne));
    when(fakeStorageClient.readRows(expectedReadRowsRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesTwo));

    PCollection<TableRow> output =
        p.apply(
            BigQueryIO.readTableRows()
                .from("foo.com:project:dataset.table")
                .withMethod(Method.DIRECT_READ)
                .withSelectedFields(Lists.newArrayList("name"))
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
                new TableRow().set("name", "D"),
                new TableRow().set("name", "E"),
                new TableRow().set("name", "F"),
                new TableRow().set("name", "G")));

    p.run();
  }

  @Test
  public void testReadFromBigQueryIOWithBeamSchema() throws Exception {
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
            .setMaxStreamCount(0)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .addStreams(ReadStream.newBuilder().setName("streamName1"))
            .addStreams(ReadStream.newBuilder().setName("streamName2"))
            .setDataFormat(DataFormat.AVRO)
            .build();

    ReadRowsRequest expectedReadRowsRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("streamName1").build();
    ReadRowsRequest expectedReadRowsRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("streamName2").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", TRIMMED_AVRO_SCHEMA),
            createRecord("B", TRIMMED_AVRO_SCHEMA),
            createRecord("C", TRIMMED_AVRO_SCHEMA),
            createRecord("D", TRIMMED_AVRO_SCHEMA),
            createRecord("E", TRIMMED_AVRO_SCHEMA),
            createRecord("F", TRIMMED_AVRO_SCHEMA),
            createRecord("G", TRIMMED_AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponsesOne =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(2, 4), 0.5, 0.75));
    List<ReadRowsResponse> readRowsResponsesTwo =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(4, 5), 0.0, 0.33),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(5, 7), 0.33, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesOne));
    when(fakeStorageClient.readRows(expectedReadRowsRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesTwo));

    PCollection<Row> output =
        p.apply(
                BigQueryIO.readTableRowsWithSchema()
                    .from("foo.com:project:dataset.table")
                    .withMethod(Method.DIRECT_READ)
                    .withSelectedFields(Lists.newArrayList("name"))
                    .withFormat(DataFormat.AVRO)
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(fakeDatasetService)
                            .withStorageClient(fakeStorageClient)))
            .apply(Convert.toRows());

    org.apache.beam.sdk.schemas.Schema beamSchema =
        org.apache.beam.sdk.schemas.Schema.of(
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "name", org.apache.beam.sdk.schemas.Schema.FieldType.STRING));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                Row.withSchema(beamSchema).addValue("A").build(),
                Row.withSchema(beamSchema).addValue("B").build(),
                Row.withSchema(beamSchema).addValue("C").build(),
                Row.withSchema(beamSchema).addValue("D").build(),
                Row.withSchema(beamSchema).addValue("E").build(),
                Row.withSchema(beamSchema).addValue("F").build(),
                Row.withSchema(beamSchema).addValue("G").build()));

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
                    .setDataFormat(DataFormat.ARROW))
            .setMaxStreamCount(0)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setArrowSchema(
                ArrowSchema.newBuilder()
                    .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                    .build())
            .addStreams(ReadStream.newBuilder().setName("streamName1"))
            .addStreams(ReadStream.newBuilder().setName("streamName2"))
            .setDataFormat(DataFormat.ARROW)
            .build();

    ReadRowsRequest expectedReadRowsRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("streamName1").build();
    ReadRowsRequest expectedReadRowsRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("streamName2").build();

    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> readRowsResponsesOne =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.50),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.5, 0.75));
    List<ReadRowsResponse> readRowsResponsesTwo =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(4, 5), values.subList(4, 5), 0.0, 0.33),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(5, 6), values.subList(5, 6), 0.33, 0.66),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(6, 7), values.subList(6, 7), 0.66, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesOne));
    when(fakeStorageClient.readRows(expectedReadRowsRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponsesTwo));

    PCollection<KV<String, Long>> output =
        p.apply(
            BigQueryIO.read(new ParseKeyValue())
                .from("foo.com:project:dataset.table")
                .withMethod(Method.DIRECT_READ)
                .withFormat(DataFormat.ARROW)
                .withTestServices(
                    new FakeBigQueryServices()
                        .withDatasetService(fakeDatasetService)
                        .withStorageClient(fakeStorageClient)));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of("A", 1L),
                KV.of("B", 2L),
                KV.of("C", 3L),
                KV.of("D", 4L),
                KV.of("E", 5L),
                KV.of("F", 6L),
                KV.of("G", 7L)));

    p.run();
  }

  @Test
  public void testReadFromStreamSourceArrow() throws Exception {

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setArrowSchema(
                ArrowSchema.newBuilder()
                    .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                    .build())
            .setDataFormat(DataFormat.ARROW)
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();

    List<String> names = Arrays.asList("A", "B", "C");
    List<Long> values = Arrays.asList(1L, 2L, 3L);
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.50),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    List<ReadStream> streamBundle =
        Lists.newArrayList(ReadStream.newBuilder().setName("readStream").build());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    List<TableRow> rows = new ArrayList<>();
    BoundedReader<TableRow> reader = streamSource.createReader(options);
    for (boolean hasNext = reader.start(); hasNext; hasNext = reader.advance()) {
      rows.add(reader.getCurrent());
    }

    System.out.println("Rows: " + rows);

    assertEquals(3, rows.size());
  }

  @Test
  public void testFractionConsumedWithArrowAndOneStreamInBundle() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setArrowSchema(
                ArrowSchema.newBuilder()
                    .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                    .build())
            .setDataFormat(DataFormat.ARROW)
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();

    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(
                ARROW_SCHEMA, Lists.newArrayList(), Lists.newArrayList(), 0.25, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.3, 0.5),
            createResponseArrow(ARROW_SCHEMA, names.subList(4, 7), values.subList(4, 7), 0.7, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    List<ReadStream> streamBundle =
        Lists.newArrayList(ReadStream.newBuilder().setName("readStream").build());
    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.25, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.4, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads D.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads E.
    assertEquals(0.8, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads F.
    assertEquals(0.9, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads G.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance()); // Reaches the end.

    // We are done with the stream, so we should report 100% consumption.
    assertEquals(Double.valueOf(1.0), reader.getFractionConsumed());
  }

  @Test
  public void testFractionConsumedWithArrowAndMultipleStreamsInBundle() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setArrowSchema(
                ArrowSchema.newBuilder()
                    .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                    .build())
            .setDataFormat(DataFormat.ARROW)
            .build();

    ReadRowsRequest expectedRequestOne =
        ReadRowsRequest.newBuilder().setReadStream("readStream1").build();
    ReadRowsRequest expectedRequestTwo =
        ReadRowsRequest.newBuilder().setReadStream("readStream2").build();

    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> responsesOne =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.5),
            createResponseArrow(ARROW_SCHEMA, Lists.newArrayList(), Lists.newArrayList(), 0.5, 0.5),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.5, 1.0));

    List<ReadRowsResponse> responsesTwo =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(4, 7), values.subList(4, 7), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequestOne, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesOne));
    when(fakeStorageClient.readRows(expectedRequestTwo, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responsesTwo));

    List<ReadStream> streamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build());

    BigQueryStorageStreamBundleSource<TableRow> streamSource =
        BigQueryStorageStreamBundleSource.create(
            readSession,
            streamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.25, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.375, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads D.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads E.
    assertEquals(0.6666666666666666, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads F.
    assertEquals(0.8333333333333333, reader.getFractionConsumed(), DELTA);
    assertTrue(reader.advance()); // Reads G.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance()); // Reaches the end.

    // We are done with the streams, so we should report 100% consumption.
    assertEquals(Double.valueOf(1.0), reader.getFractionConsumed());
  }

  @Test
  public void testStreamSourceSplitAtFractionWithArrowAndMultipleStreamsInBundle()
      throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.6),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.6, 1.0),
            createResponseArrow(ARROW_SCHEMA, names.subList(3, 6), values.subList(3, 6), 0.0, 1.0),
            createResponseArrow(ARROW_SCHEMA, names.subList(6, 7), values.subList(6, 7), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream3").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(3, 4)));

    List<ReadStream> primaryStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build(),
            ReadStream.newBuilder().setName("readStream3").build());

    BigQueryStorageStreamBundleSource<TableRow> primarySource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            primaryStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the primary bundle and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> primary = primarySource.createReader(options);
    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());

    // Now split the StreamBundle, and ensure that the returned source points to a non-null
    // secondary StreamBundle.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.35);
    assertNotNull(secondarySource);
    BoundedReader<TableRow> secondary = secondarySource.createReader(options);

    assertEquals("C", primary.getCurrent().get("name"));
    assertFalse(primary.advance());

    assertTrue(secondary.start());
    assertEquals("D", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());
    assertEquals("E", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());
    assertEquals("F", secondary.getCurrent().get("name"));
    assertTrue((secondary.advance()));
    assertEquals("G", secondary.getCurrent().get("name"));
    assertFalse((secondary.advance()));
  }

  @Test
  public void testStreamSourceSplitAtFractionRepeatedWithArrowAndMultipleStreamsInBundle()
      throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.6),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.6, 1.0),
            createResponseArrow(ARROW_SCHEMA, names.subList(3, 6), values.subList(3, 6), 0.0, 1.0),
            createResponseArrow(ARROW_SCHEMA, names.subList(6, 7), values.subList(6, 7), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream3").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(3, 4)));

    List<ReadStream> primaryStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build(),
            ReadStream.newBuilder().setName("readStream3").build());

    BigQueryStorageStreamBundleSource<TableRow> primarySource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            primaryStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the primary bundle and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> primary = primarySource.createReader(options);
    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());

    // Now split the StreamBundle, and ensure that the returned source points to a non-null
    // secondary StreamBundle. Since there are 3 streams in this Bundle, splitting will only
    // occur when fraction >= 0.33.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.35);
    assertNotNull(secondarySource);
    BoundedReader<TableRow> secondary = secondarySource.createReader(options);

    assertEquals("C", primary.getCurrent().get("name"));
    assertFalse(primary.advance());

    assertTrue(secondary.start());
    assertEquals("D", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());
    assertEquals("E", secondary.getCurrent().get("name"));
    assertTrue(secondary.advance());

    // Now split the StreamBundle again, and ensure that the returned source points to a non-null
    // tertiary StreamBundle. Since there are 2 streams in this Bundle, splitting will only
    // occur when fraction >= 0.5.
    BoundedSource<TableRow> tertiarySource = secondary.splitAtFraction(0.5);
    assertNotNull(tertiarySource);
    BoundedReader<TableRow> tertiary = tertiarySource.createReader(options);

    assertEquals("F", secondary.getCurrent().get("name"));
    assertFalse((secondary.advance()));

    assertTrue(tertiary.start());
    assertEquals("G", tertiary.getCurrent().get("name"));
    assertFalse((tertiary.advance()));
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenParentIsPastSplitPointArrow()
      throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.66),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.66, 1.0),
            createResponseArrow(ARROW_SCHEMA, names.subList(3, 5), values.subList(3, 5), 0.0, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream1").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(0, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("readStream2").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses.subList(2, 3)));

    List<ReadStream> parentStreamBundle =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("readStream1").build(),
            ReadStream.newBuilder().setName("readStream2").build());

    BigQueryStorageStreamBundleSource<TableRow> streamBundleSource =
        BigQueryStorageStreamBundleSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            parentStreamBundle,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient),
            1L);

    // Read a few records from the parent bundle and ensure the records are returned in
    // the prescribed order.
    BoundedReader<TableRow> primary = streamBundleSource.createReader(options);
    assertTrue(primary.start());
    assertEquals("A", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("B", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));
    assertTrue(primary.advance());
    assertEquals("D", primary.getCurrent().get("name"));

    // We attempt to split the StreamBundle after starting to read the contents of the second
    // stream.
    BoundedSource<TableRow> secondarySource = primary.splitAtFraction(0.5);
    assertNull(secondarySource);

    assertTrue(primary.advance());
    assertEquals("E", primary.getCurrent().get("name"));
    assertFalse(primary.advance());
  }

  @Test
  public void testActuateProjectionPushdown() {
    org.apache.beam.sdk.schemas.Schema schema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addStringField("foo")
            .addStringField("bar")
            .build();
    TypedRead<Row> read =
        BigQueryIO.read(
                record ->
                    BigQueryUtils.toBeamRow(
                        record.getRecord(), schema, ConversionOptions.builder().build()))
            .withMethod(Method.DIRECT_READ)
            .withCoder(SchemaCoder.of(schema));

    assertTrue(read.supportsProjectionPushdown());
    PTransform<PBegin, PCollection<Row>> pushdownT =
        read.actuateProjectionPushdown(
            ImmutableMap.of(new TupleTag<>("output"), FieldAccessDescriptor.withFieldNames("foo")));

    TypedRead<Row> pushdownRead = (TypedRead<Row>) pushdownT;
    assertEquals(Method.DIRECT_READ, pushdownRead.getMethod());
    assertThat(pushdownRead.getSelectedFields().get(), Matchers.containsInAnyOrder("foo"));
    assertTrue(pushdownRead.getProjectionPushdownApplied());
  }

  @Test
  public void testReadFromQueryDoesNotSupportProjectionPushdown() {
    org.apache.beam.sdk.schemas.Schema schema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addStringField("foo")
            .addStringField("bar")
            .build();
    TypedRead<Row> read =
        BigQueryIO.read(
                record ->
                    BigQueryUtils.toBeamRow(
                        record.getRecord(), schema, ConversionOptions.builder().build()))
            .fromQuery("SELECT bar FROM `dataset.table`")
            .withMethod(Method.DIRECT_READ)
            .withCoder(SchemaCoder.of(schema));

    assertFalse(read.supportsProjectionPushdown());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read.actuateProjectionPushdown(
                ImmutableMap.of(
                    new TupleTag<>("output"), FieldAccessDescriptor.withFieldNames("foo"))));
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
}
