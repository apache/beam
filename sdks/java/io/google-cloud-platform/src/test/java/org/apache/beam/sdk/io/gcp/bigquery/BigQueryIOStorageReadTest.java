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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.FailedPreconditionException;
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
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.bigquery.storage.v1.StreamStats.Progress;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageStreamSource.BigQueryStorageStreamReader;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.mockito.ArgumentMatchers;

/** Tests for {@link BigQueryIO#readTableRows() using {@link Method#DIRECT_READ}}. */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadTest {

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

  // --- MICROS ---
  private static final TableSchema TABLE_SCHEMA_TIMESTAMP =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema()
                      .setName("ts")
                      .setType("TIMESTAMP")
                      .setMode("REQUIRED")
                      .setTimestampPrecision(12L)));

  private static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA_TS_MICROS =
      new org.apache.arrow.vector.types.pojo.Schema(
          asList(field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

  private static final String AVRO_SCHEMA_TS_MICROS_STRING =
      "{\"namespace\": \"example.avro\","
          + " \"type\": \"record\","
          + " \"name\": \"RowRecord\","
          + " \"fields\": ["
          + "     {\"name\": \"ts\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}"
          + " ]}";

  private static final Schema AVRO_SCHEMA_TS_MICROS =
      new Schema.Parser().parse(AVRO_SCHEMA_TS_MICROS_STRING);

  // --- NANOS ---
  private static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA_TS_NANOS =
      new org.apache.arrow.vector.types.pojo.Schema(
          asList(field("ts", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))));

  private static final String AVRO_SCHEMA_TS_NANOS_STRING =
      "{\"namespace\": \"example.avro\","
          + " \"type\": \"record\","
          + " \"name\": \"RowRecord\","
          + " \"fields\": ["
          + "     {\"name\": \"ts\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-nanos\"}}"
          + " ]}";

  private static final Schema AVRO_SCHEMA_TS_NANOS =
      new Schema.Parser().parse(AVRO_SCHEMA_TS_NANOS_STRING);

  // --- PICOS (string) ---
  private static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA_TS_PICOS =
      new org.apache.arrow.vector.types.pojo.Schema(asList(field("ts", new ArrowType.Utf8())));

  private static final String AVRO_SCHEMA_TS_PICOS_STRING =
      "{\"namespace\": \"example.avro\","
          + " \"type\": \"record\","
          + " \"name\": \"RowRecord\","
          + " \"fields\": ["
          + "     {\"name\": \"ts\", \"type\": \"string\"}"
          + " ]}";

  private static final Schema AVRO_SCHEMA_TS_PICOS =
      new Schema.Parser().parse(AVRO_SCHEMA_TS_PICOS_STRING);

  private void doTableSourceInitialSplitTest(long bundleSize, int streamCount) throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(1024L * 1024L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder()))
            .setMaxStreamCount(streamCount)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO);
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
    assertEquals(streamCount, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithSelectedFieldsAndRowRestriction() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table = new Table().setTableReference(tableRef).setNumBytes(100L).setSchema(TABLE_SCHEMA);

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
            .setMaxStreamCount(10)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO);
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

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(10L, options);
    assertEquals(10L, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithDefaultProject() throws Exception {
    fakeDatasetService.createDataset("project-id", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset.table");

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(1024L * 1024L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/project-id/datasets/dataset/tables/table")
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder()))
            .setMaxStreamCount(1024)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO);
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
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setReadOptions(ReadSession.TableReadOptions.newBuilder()))
            .setMaxStreamCount(1024)
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

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.getDefaultInstance(),
            ReadStream.getDefaultInstance(),
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
            ReadStream.getDefaultInstance(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices());

    assertThat(streamSource.split(0, options), containsInAnyOrder(streamSource));
  }

  @Test
  public void testSplitReadStreamAtFraction() throws IOException {

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();
    List<ReadRowsResponse> responses = ImmutableList.of();

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    PipelineOptions options = PipelineOptionsFactory.fromArgs("--enableStorageReadApiV2").create();
    BigQueryStorageStreamReader<TableRow> reader = streamSource.createReader(options);
    reader.start();
    // Beam does not split storage read api v2 stream
    assertNull(reader.splitAtFraction(0.5));
  }

  @Test
  public void testReadFromStreamSource() throws Exception {

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
            createRecord("C", 3, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.0, 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 3), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
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

  private static final double DELTA = 1e-6;

  @Test
  public void testFractionConsumed() throws Exception {
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

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

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
  public void testFractionConsumedWithSplit() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("parentStream").build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", 1, AVRO_SCHEMA),
            createRecord("B", 2, AVRO_SCHEMA),
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA),
            createRecord("G", 7, AVRO_SCHEMA));

    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.000, 0.250),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.300, 0.500),
            createResponse(AVRO_SCHEMA, records.subList(4, 7), 0.800, 0.875));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());

    List<ReadRowsResponse> primaryResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(1, 3), 0.25, 0.75),
            createResponse(AVRO_SCHEMA, records.subList(3, 4), 0.8, 1.0));

    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("primaryStream").setOffset(1).build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(primaryResponses));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);

    reader.splitAtFraction(0.5);
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.75, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads D.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance());
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);
  }

  @Test
  public void testStreamSourceSplitAtFractionSucceeds() throws Exception {
    List<ReadRowsResponse> parentResponses =
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
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream("primaryStream")
                // This test will read rows 0 and 1 from the parent before calling split,
                // so we expect the primary read to start at offset 2.
                .setOffset(2)
                .build(),
            ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses.subList(1, 2)));

    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("remainderStream").build(), ""))
        .thenReturn(
            new FakeBigQueryServerStream<>(parentResponses.subList(2, parentResponses.size())));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    // Now split the stream, and ensure that the "parent" reader has been replaced with the
    // primary stream and that the returned source points to the residual stream.
    BoundedReader<TableRow> primary = parent;
    BoundedSource<TableRow> residualSource = parent.splitAtFraction(0.5);
    assertNotNull(residualSource);
    BoundedReader<TableRow> residual = residualSource.createReader(options);

    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));
    assertFalse(primary.advance());

    assertTrue(residual.start());
    assertEquals("D", residual.getCurrent().get("name"));
    assertTrue(residual.advance());
    assertEquals("E", residual.getCurrent().get("name"));
    assertFalse(residual.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionRepeated() throws Exception {
    List<ReadStream> readStreams =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("stream1").build(),
            ReadStream.newBuilder().setName("stream2").build(),
            ReadStream.newBuilder().setName("stream3").build());

    StorageClient fakeStorageClient = mock(StorageClient.class);

    // Mock the initial ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream(readStreams.get(0).getName()).build(), ""))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                        0.0,
                        0.25),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("C", 3, AVRO_SCHEMA), createRecord("D", 4, AVRO_SCHEMA)),
                        0.25,
                        0.50),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("E", 5, AVRO_SCHEMA), createRecord("F", 6, AVRO_SCHEMA)),
                        0.5,
                        0.75))));

    // Mock the first SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setName(readStreams.get(0).getName())
                .setFraction(0.83f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(readStreams.get(1))
                .setRemainderStream(ReadStream.newBuilder().setName("ignored"))
                .build());

    // Mock the second ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream(readStreams.get(1).getName())
                .setOffset(1)
                .build(),
            ""))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("B", 2, AVRO_SCHEMA), createRecord("C", 3, AVRO_SCHEMA)),
                        0.0,
                        0.50),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                        0.5,
                        0.75))));

    // Mock the second SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setName(readStreams.get(1).getName())
                .setFraction(0.75f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(readStreams.get(2))
                .setRemainderStream(ReadStream.newBuilder().setName("ignored"))
                .build());

    // Mock the third ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream(readStreams.get(2).getName())
                .setOffset(2)
                .build(),
            ""))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("C", 3, AVRO_SCHEMA), createRecord("D", 4, AVRO_SCHEMA)),
                        0.80,
                        0.90))));

    BoundedSource<TableRow> source =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            readStreams.get(0),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<TableRow> reader = source.createReader(options);
    assertTrue(reader.start());
    assertEquals("A", reader.getCurrent().get("name"));

    BoundedSource<TableRow> residualSource = reader.splitAtFraction(0.83f);
    assertNotNull(residualSource);
    assertEquals("A", reader.getCurrent().get("name"));

    assertTrue(reader.advance());
    assertEquals("B", reader.getCurrent().get("name"));

    residualSource = reader.splitAtFraction(0.75f);
    assertNotNull(residualSource);
    assertEquals("B", reader.getCurrent().get("name"));

    assertTrue(reader.advance());
    assertEquals("C", reader.getCurrent().get("name"));
    assertTrue(reader.advance());
    assertEquals("D", reader.getCurrent().get("name"));
    assertFalse(reader.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenSplitIsNotPossible() throws Exception {
    List<ReadRowsResponse> parentResponses =
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
                0.5,
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(SplitReadStreamResponse.getDefaultInstance());

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    assertNull(parent.splitAtFraction(0.5));
    verify(fakeStorageClient, times(1)).splitReadStream(ArgumentMatchers.any());

    // Verify that subsequent splitAtFraction() calls after a failed splitAtFraction() attempt
    // do NOT invoke SplitReadStream.
    assertNull(parent.splitAtFraction(0.5));
    verify(fakeStorageClient, times(1)).splitReadStream(ArgumentMatchers.any());

    // Verify that the parent source still works okay even after an unsuccessful split attempt.
    assertTrue(parent.advance());
    assertEquals("C", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("D", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("E", parent.getCurrent().get("name"));
    assertFalse(parent.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenParentIsPastSplitPoint() throws Exception {
    List<ReadRowsResponse> parentResponses =
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
                0.5,
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream("primaryStream")
                // This test will read rows 0 and 1 from the parent before calling split,
                // so we expect the primary read to start at offset 2.
                .setOffset(2)
                .build(),
            ""))
        .thenThrow(
            new FailedPreconditionException(
                "Given row offset is invalid for stream.",
                new StatusRuntimeException(Status.FAILED_PRECONDITION),
                GrpcStatusCode.of(Code.FAILED_PRECONDITION),
                /* retryable = */ false));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    assertNull(parent.splitAtFraction(0.5));

    // Verify that the parent source still works okay even after an unsuccessful split attempt.
    assertTrue(parent.advance());
    assertEquals("C", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("D", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("E", parent.getCurrent().get("name"));
    assertFalse(parent.advance());
  }

  private static final class ParseKeyValue
      implements SerializableFunction<SchemaAndRecord, KV<String, Long>> {

    @Override
    public KV<String, Long> apply(SchemaAndRecord input) {
      return KV.of(
          input.getRecord().get("name").toString(), (Long) input.getRecord().get("number"));
    }
  }

  /**
   * A mock response that stuck indefinitely when the returned iterator's hasNext get called,
   * intended to simulate server side issue.
   */
  static class StuckResponse extends ArrayList<ReadRowsResponse> {

    private CountDownLatch latch;

    public StuckResponse(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public Iterator<ReadRowsResponse> iterator() {
      return new StuckIterator();
    }

    private class StuckIterator implements Iterator<ReadRowsResponse> {

      @Override
      public boolean hasNext() {
        latch.countDown();
        try {
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return false;
      }

      @Override
      public ReadRowsResponse next() {
        return null;
      }
    }
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenReaderRunning() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();

    CountDownLatch latch = new CountDownLatch(1);
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(new StuckResponse(latch)));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BigQueryStorageStreamReader<TableRow> reader = streamSource.createReader(options);

    Thread t =
        new Thread(
            () -> {
              try {
                reader.start();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    t.start();

    // wait until thread proceed
    latch.await();

    // SplitAfFraction rejected while response iterator busy
    assertNull(reader.splitAtFraction(0.5));
    t.interrupt();
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
                new TableRow().set("name", "D")));

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
                Row.withSchema(beamSchema).addValue("D").build()));

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
            ImmutableList.of(KV.of("A", 1L), KV.of("B", 2L), KV.of("C", 3L), KV.of("D", 4L)));

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

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
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
  public void testFractionConsumedArrow() throws Exception {
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

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

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
  public void testFractionConsumedWithSplitArrow() throws Exception {
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
        ReadRowsRequest.newBuilder().setReadStream("parentStream").build();

    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> parentResponse =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.3, 0.5),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(4, 7), values.subList(4, 7), 0.7, 0.875));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponse));

    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());
    List<ReadRowsResponse> primaryResponses =
        Lists.newArrayList(
            createResponseArrow(
                ARROW_SCHEMA, names.subList(1, 3), values.subList(1, 3), 0.25, 0.75),
            createResponseArrow(ARROW_SCHEMA, names.subList(3, 4), values.subList(3, 4), 0.8, 1.0));

    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("primaryStream").setOffset(1).build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(primaryResponses));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(0.0, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.start()); // Reads A.
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);

    reader.splitAtFraction(0.5);
    assertEquals(0.125, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads B.
    assertEquals(0.5, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads C.
    assertEquals(0.75, reader.getFractionConsumed(), DELTA);

    assertTrue(reader.advance()); // Reads D.
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);

    assertFalse(reader.advance());
    assertEquals(1.0, reader.getFractionConsumed(), DELTA);
  }

  @Test
  public void testStreamSourceSplitAtFractionSucceedsArrow() throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.25, 0.5),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(3, 5), values.subList(3, 5), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream("primaryStream")
                // This test will read rows 0 and 1 from the parent before calling split,
                // so we expect the primary read to start at offset 2.
                .setOffset(2)
                .build(),
            ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses.subList(1, 2)));

    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("remainderStream").build(), ""))
        .thenReturn(
            new FakeBigQueryServerStream<>(parentResponses.subList(2, parentResponses.size())));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    // Now split the stream, and ensure that the "parent" reader has been replaced with the
    // primary stream and that the returned source points to the residual stream.
    BoundedReader<TableRow> primary = parent;
    BoundedSource<TableRow> residualSource = parent.splitAtFraction(0.5);
    assertNotNull(residualSource);
    BoundedReader<TableRow> residual = residualSource.createReader(options);

    assertTrue(primary.advance());
    assertEquals("C", primary.getCurrent().get("name"));
    assertFalse(primary.advance());

    assertTrue(residual.start());
    assertEquals("D", residual.getCurrent().get("name"));
    assertTrue(residual.advance());
    assertEquals("E", residual.getCurrent().get("name"));
    assertFalse(residual.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionRepeatedArrow() throws Exception {
    List<ReadStream> readStreams =
        Lists.newArrayList(
            ReadStream.newBuilder().setName("stream1").build(),
            ReadStream.newBuilder().setName("stream2").build(),
            ReadStream.newBuilder().setName("stream3").build());

    StorageClient fakeStorageClient = mock(StorageClient.class);

    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);
    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.25, 0.5),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(4, 6), values.subList(4, 6), 0.5, 0.75));

    // Mock the initial ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream(readStreams.get(0).getName()).build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mock the first SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setName(readStreams.get(0).getName())
                .setFraction(0.83f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(readStreams.get(1))
                .setRemainderStream(ReadStream.newBuilder().setName("ignored"))
                .build());

    List<ReadRowsResponse> otherResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(1, 3), values.subList(1, 3), 0.0, 0.50),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(3, 4), values.subList(3, 4), 0.5, 0.75));

    // Mock the second ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream(readStreams.get(1).getName())
                .setOffset(1)
                .build(),
            ""))
        .thenReturn(new FakeBigQueryServerStream<>(otherResponses));

    // Mock the second SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setName(readStreams.get(1).getName())
                .setFraction(0.75f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(readStreams.get(2))
                .setRemainderStream(ReadStream.newBuilder().setName("ignored"))
                .build());

    List<ReadRowsResponse> lastResponses =
        Lists.newArrayList(
            createResponseArrow(
                ARROW_SCHEMA, names.subList(2, 4), values.subList(2, 4), 0.80, 0.90));

    // Mock the third ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream(readStreams.get(2).getName())
                .setOffset(2)
                .build(),
            ""))
        .thenReturn(new FakeBigQueryServerStream<>(lastResponses));

    BoundedSource<TableRow> source =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            readStreams.get(0),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<TableRow> reader = source.createReader(options);
    assertTrue(reader.start());
    assertEquals("A", reader.getCurrent().get("name"));

    BoundedSource<TableRow> residualSource = reader.splitAtFraction(0.83f);
    assertNotNull(residualSource);
    assertEquals("A", reader.getCurrent().get("name"));

    assertTrue(reader.advance());
    assertEquals("B", reader.getCurrent().get("name"));

    residualSource = reader.splitAtFraction(0.75f);
    assertNotNull(residualSource);
    assertEquals("B", reader.getCurrent().get("name"));

    assertTrue(reader.advance());
    assertEquals("C", reader.getCurrent().get("name"));
    assertTrue(reader.advance());
    assertEquals("D", reader.getCurrent().get("name"));
    assertFalse(reader.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenSplitIsNotPossibleArrow() throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);
    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.25, 0.5),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(3, 5), values.subList(3, 5), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(SplitReadStreamResponse.getDefaultInstance());

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    assertNull(parent.splitAtFraction(0.5));
    verify(fakeStorageClient, times(1)).splitReadStream(ArgumentMatchers.any());

    // Verify that subsequent splitAtFraction() calls after a failed splitAtFraction() attempt
    // do NOT invoke SplitReadStream.
    assertNull(parent.splitAtFraction(0.5));
    verify(fakeStorageClient, times(1)).splitReadStream(ArgumentMatchers.any());

    // Verify that the parent source still works okay even after an unsuccessful split attempt.
    assertTrue(parent.advance());
    assertEquals("C", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("D", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("E", parent.getCurrent().get("name"));
    assertFalse(parent.advance());
  }

  @Test
  public void testStreamSourceSplitAtFractionFailsWhenParentIsPastSplitPointArrow()
      throws Exception {
    List<String> names = Arrays.asList("A", "B", "C", "D", "E", "F");
    List<Long> values = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L);
    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponseArrow(ARROW_SCHEMA, names.subList(0, 2), values.subList(0, 2), 0.0, 0.25),
            createResponseArrow(ARROW_SCHEMA, names.subList(2, 3), values.subList(2, 3), 0.25, 0.5),
            createResponseArrow(
                ARROW_SCHEMA, names.subList(3, 5), values.subList(3, 5), 0.5, 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder().setReadStream("parentStream").build(), ""))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder().setName("parentStream").setFraction(0.5f).build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(ReadStream.newBuilder().setName("primaryStream"))
                .setRemainderStream(ReadStream.newBuilder().setName("remainderStream"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadStream("primaryStream")
                // This test will read rows 0 and 1 from the parent before calling split,
                // so we expect the primary read to start at offset 2.
                .setOffset(2)
                .build(),
            ""))
        .thenThrow(
            new FailedPreconditionException(
                "Given row offset is invalid for stream.",
                new StatusRuntimeException(Status.FAILED_PRECONDITION),
                GrpcStatusCode.of(Code.FAILED_PRECONDITION),
                /* retryable = */ false));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setArrowSchema(
                    ArrowSchema.newBuilder()
                        .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                        .build())
                .setDataFormat(DataFormat.ARROW)
                .build(),
            ReadStream.newBuilder().setName("parentStream").build(),
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    // Read a few records from the parent stream and ensure that records are returned in the
    // prescribed order.
    BoundedReader<TableRow> parent = streamSource.createReader(options);
    assertTrue(parent.start());
    assertEquals("A", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("B", parent.getCurrent().get("name"));

    assertNull(parent.splitAtFraction(0.5));

    // Verify that the parent source still works okay even after an unsuccessful split attempt.
    assertTrue(parent.advance());
    assertEquals("C", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("D", parent.getCurrent().get("name"));
    assertTrue(parent.advance());
    assertEquals("E", parent.getCurrent().get("name"));
    assertFalse(parent.advance());
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

  @Test
  public void testReadFromBigQueryAvroObjectsMutation() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder().setReadStream("readStream").build();

    List<GenericRecord> records =
        Lists.newArrayList(createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(0, 1), 0.0, 0.5),
            createResponse(AVRO_SCHEMA, records.subList(1, 2), 0.5, 1.0));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest, ""))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

    BigQueryStorageStreamSource<GenericRecord> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            ReadStream.newBuilder().setName("readStream").build(),
            TABLE_SCHEMA,
            SchemaAndRecord::getRecord,
            AvroCoder.of(AVRO_SCHEMA),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    BoundedReader<GenericRecord> reader = streamSource.createReader(options);

    // Reads A.
    assertTrue(reader.start());
    GenericRecord rowA = reader.getCurrent();
    assertEquals(new Utf8("A"), rowA.get("name"));

    // Reads B.
    assertTrue(reader.advance());
    GenericRecord rowB = reader.getCurrent();
    assertEquals(new Utf8("B"), rowB.get("name"));

    // Make sure rowA has not mutated after advance
    assertEquals(new Utf8("A"), rowA.get("name"));
  }

  @Test
  public void testTimestampPrecisionDefaultValue() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table");

    assertNull(typedRead.getDirectReadPicosTimestampPrecision());
  }

  @Test
  public void testwithDirectReadPicosTimestampPrecisionNanos() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withDirectReadPicosTimestampPrecision(TimestampPrecision.NANOS);

    assertEquals(TimestampPrecision.NANOS, typedRead.getDirectReadPicosTimestampPrecision());
  }

  @Test
  public void testwithDirectReadPicosTimestampPrecisionPicos() {
    BigQueryIO.TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withDirectReadPicosTimestampPrecision(TimestampPrecision.PICOS);

    assertEquals(TimestampPrecision.PICOS, typedRead.getDirectReadPicosTimestampPrecision());
  }

  @Test
  public void testTableSourceInitialSplit_withDirectReadPicosTimestampPrecisionNanos_Arrow()
      throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(100L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setDataFormat(DataFormat.ARROW)
                    .setReadOptions(
                        ReadSession.TableReadOptions.newBuilder()
                            .setArrowSerializationOptions(
                                com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions
                                    .newBuilder()
                                    .setPicosTimestampPrecision(
                                        com.google.cloud.bigquery.storage.v1
                                            .ArrowSerializationOptions.PicosTimestampPrecision
                                            .TIMESTAMP_PRECISION_NANOS))))
            .setMaxStreamCount(10)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setArrowSchema(
                ArrowSchema.newBuilder().setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA)))
            .setDataFormat(DataFormat.ARROW);
    for (int i = 0; i < 10; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            DataFormat.ARROW,
            null, /* selectedFields */
            null, /* rowRestriction */
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient),
            false, /* projectionPushdownApplied */
            TimestampPrecision.NANOS);

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(10L, options);
    assertEquals(10L, sources.size());
  }

  private org.apache.arrow.vector.types.pojo.Schema getArrowSchemaTs(TimestampPrecision precision) {
    switch (precision) {
      case NANOS:
        return ARROW_SCHEMA_TS_NANOS;
      case PICOS:
        return ARROW_SCHEMA_TS_PICOS;
      case MICROS:
      default:
        return ARROW_SCHEMA_TS_MICROS;
    }
  }

  private Schema getAvroSchemaTs(TimestampPrecision precision) {
    switch (precision) {
      case NANOS:
        return AVRO_SCHEMA_TS_NANOS;
      case PICOS:
        return AVRO_SCHEMA_TS_PICOS;
      case MICROS:
      default:
        return AVRO_SCHEMA_TS_MICROS;
    }
  }

  private String getAvroSchemaStringTs(TimestampPrecision precision) {
    switch (precision) {
      case NANOS:
        return AVRO_SCHEMA_TS_NANOS_STRING;
      case PICOS:
        return AVRO_SCHEMA_TS_PICOS_STRING;
      case MICROS:
      default:
        return AVRO_SCHEMA_TS_MICROS_STRING;
    }
  }

  /**
   * Converts ISO timestamp strings to the appropriate format for the precision. - MICROS: Long
   * (epoch microseconds) - NANOS: Long (epoch nanoseconds) - PICOS: String (formatted as
   * "yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS UTC")
   */
  private List<Object> convertInputsForPrecision(
      List<String> isoTimestamps, TimestampPrecision precision) {
    return isoTimestamps.stream()
        .map(
            iso -> {
              if (precision == TimestampPrecision.PICOS) {
                // For PICOS, input IS the string (already formatted)
                return iso;
              }
              java.time.Instant instant = java.time.Instant.parse(iso);
              if (precision == TimestampPrecision.NANOS) {
                return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
              } else {
                // MICROS (default)
                return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000;
              }
            })
        .collect(Collectors.toList());
  }

  private ReadSession createTsReadSession(
      DataFormat dataFormat,
      org.apache.arrow.vector.types.pojo.Schema arrowSchema,
      String avroSchemaString) {
    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .addStreams(ReadStream.newBuilder().setName("streamName"))
            .setDataFormat(dataFormat);

    if (dataFormat == DataFormat.ARROW) {
      builder.setArrowSchema(
          ArrowSchema.newBuilder().setSerializedSchema(serializeArrowSchema(arrowSchema)).build());
    } else {
      builder.setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchemaString).build());
    }
    return builder.build();
  }

  private ReadRowsResponse createArrowTsResponse(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema,
      TimestampPrecision precision,
      List<Object> inputValues) {
    ArrowRecordBatch serializedRecord;
    try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)) {
      schemaRoot.allocateNew();
      schemaRoot.setRowCount(inputValues.size());

      switch (precision) {
        case NANOS:
          TimeStampNanoTZVector nanoVector =
              (TimeStampNanoTZVector) schemaRoot.getFieldVectors().get(0);
          for (int i = 0; i < inputValues.size(); i++) {
            nanoVector.set(i, (Long) inputValues.get(i));
          }
          break;
        case PICOS:
          VarCharVector stringVector = (VarCharVector) schemaRoot.getFieldVectors().get(0);
          for (int i = 0; i < inputValues.size(); i++) {
            stringVector.set(i, new Text((String) inputValues.get(i)));
          }
          break;
        case MICROS:
        default:
          TimeStampMicroTZVector microVector =
              (TimeStampMicroTZVector) schemaRoot.getFieldVectors().get(0);
          for (int i = 0; i < inputValues.size(); i++) {
            microVector.set(i, (Long) inputValues.get(i));
          }
          break;
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
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ReadRowsResponse.newBuilder()
        .setArrowRecordBatch(serializedRecord)
        .setRowCount(inputValues.size())
        .setStats(
            StreamStats.newBuilder()
                .setProgress(Progress.newBuilder().setAtResponseStart(0.0).setAtResponseEnd(1.0)))
        .build();
  }

  private ReadRowsResponse createAvroTsResponse(
      Schema avroSchema, TimestampPrecision precision, List<Object> inputValues) throws Exception {
    List<GenericRecord> records = new ArrayList<>();
    for (Object value : inputValues) {
      GenericRecord record = new Record(avroSchema);
      record.put("ts", value);
      records.add(record);
    }

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    for (GenericRecord record : records) {
      writer.write(record, binaryEncoder);
    }
    binaryEncoder.flush();

    return ReadRowsResponse.newBuilder()
        .setAvroRows(
            AvroRows.newBuilder()
                .setSerializedBinaryRows(ByteString.copyFrom(outputStream.toByteArray()))
                .setRowCount(records.size()))
        .setRowCount(records.size())
        .setStats(
            StreamStats.newBuilder()
                .setProgress(Progress.newBuilder().setAtResponseStart(0.0).setAtResponseEnd(1.0)))
        .build();
  }

  private void runTimestampTest(
      DataFormat dataFormat,
      TimestampPrecision precision,
      boolean useSchema,
      List<String> inputTimestamps,
      List<String> expectedOutputs)
      throws Exception {

    TimestampPrecision effectivePrecision =
        (precision != null) ? precision : TimestampPrecision.MICROS;

    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table =
        new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA_TIMESTAMP);
    fakeDatasetService.createTable(table);

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = getArrowSchemaTs(effectivePrecision);
    Schema avroSchema = getAvroSchemaTs(effectivePrecision);
    String avroSchemaString = getAvroSchemaStringTs(effectivePrecision);

    List<Object> inputValues = convertInputsForPrecision(inputTimestamps, effectivePrecision);

    ReadSession readSession = createTsReadSession(dataFormat, arrowSchema, avroSchemaString);

    List<ReadRowsResponse> readRowsResponses;
    if (dataFormat == DataFormat.ARROW) {
      readRowsResponses =
          Lists.newArrayList(createArrowTsResponse(arrowSchema, effectivePrecision, inputValues));
    } else {
      readRowsResponses =
          Lists.newArrayList(createAvroTsResponse(avroSchema, effectivePrecision, inputValues));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(any(CreateReadSessionRequest.class)))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(any(ReadRowsRequest.class), eq("")))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    TypedRead<TableRow> read =
        useSchema ? BigQueryIO.readTableRowsWithSchema() : BigQueryIO.readTableRows();

    read =
        read.from("foo.com:project:dataset.table")
            .withMethod(Method.DIRECT_READ)
            .withFormat(dataFormat)
            .withTestServices(
                new FakeBigQueryServices()
                    .withDatasetService(fakeDatasetService)
                    .withStorageClient(fakeStorageClient));

    if (precision != null) {
      read = read.withDirectReadPicosTimestampPrecision(precision);
    }

    PCollection<TableRow> output = p.apply(read);

    PAssert.that(output)
        .satisfies(
            rows -> {
              List<TableRow> rowList = Lists.newArrayList(rows);
              assertEquals(expectedOutputs.size(), rowList.size());

              List<String> actualTimestamps =
                  rowList.stream()
                      .map(r -> (String) r.get("ts"))
                      .sorted()
                      .collect(Collectors.toList());

              List<String> sortedExpected =
                  expectedOutputs.stream().sorted().collect(Collectors.toList());

              assertEquals(sortedExpected, actualTimestamps);
              return null;
            });

    p.run();
  }

  // ===== Avro + readTableRows =====

  @Test
  public void testReadTableRows_Avro_DefaultPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        null,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123456 UTC", "2024-06-15 12:30:45.987654 UTC"));
  }

  @Test
  public void testReadTableRows_Avro_MicrosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.MICROS,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123456 UTC", "2024-06-15 12:30:45.987654 UTC"));
  }

  @Test
  public void testReadTableRows_Avro_NanosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.NANOS,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456789Z", "2024-06-15T12:30:45.987654321Z"),
        Arrays.asList("2024-01-01 00:00:00.123456789 UTC", "2024-06-15 12:30:45.987654321 UTC"));
  }

  @Test
  public void testReadTableRows_Avro_PicosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.PICOS,
        false,
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"),
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"));
  }

  // ===== Avro + readTableRowsWithSchema =====

  @Test
  public void testReadTableRowsWithSchema_Avro_DefaultPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        null,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123456 UTC", "2024-06-15 12:30:45.987654 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Avro_MicrosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.MICROS,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123456 UTC", "2024-06-15 12:30:45.987654 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Avro_NanosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.NANOS,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456789Z", "2024-06-15T12:30:45.987654321Z"),
        Arrays.asList("2024-01-01 00:00:00.123456789 UTC", "2024-06-15 12:30:45.987654321 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Avro_PicosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.AVRO,
        TimestampPrecision.PICOS,
        true,
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"),
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"));
  }

  // ===== Arrow + readTableRows =====

  @Test
  public void testReadTableRows_Arrow_DefaultPrecision() throws Exception {
    //    Avro records are always converted to beam Row and then to GenericRecord in
    // ArrowConversion.java
    //    ArrowConversion.java is a generic utility to convert Arrow records and it does not take
    // into account
    //    the BigQuery TableSchema to determine the appropriate beam type. Historically arrow
    // microsecond timestamps
    //    are converted to FieldType.DATETIME, which maps to joda Instants, which only supports up
    // to millisecond precision
    //    hence precision is lost.
    runTimestampTest(
        DataFormat.ARROW,
        null,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123 UTC", "2024-06-15 12:30:45.987 UTC"));
  }

  @Test
  public void testReadTableRows_Arrow_MicrosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.MICROS,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123 UTC", "2024-06-15 12:30:45.987 UTC"));
  }

  @Test
  public void testReadTableRows_Arrow_NanosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.NANOS,
        false,
        Arrays.asList("2024-01-01T00:00:00.123456789Z", "2024-06-15T12:30:45.987654321Z"),
        Arrays.asList("2024-01-01 00:00:00.123456789 UTC", "2024-06-15 12:30:45.987654321 UTC"));
  }

  @Test
  public void testReadTableRows_Arrow_PicosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.PICOS,
        false,
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"),
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"));
  }

  // ===== Arrow + readTableRowsWithSchema =====

  @Test
  public void testReadTableRowsWithSchema_Arrow_DefaultPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        null,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123 UTC", "2024-06-15 12:30:45.987 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Arrow_MicrosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.MICROS,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456Z", "2024-06-15T12:30:45.987654Z"),
        Arrays.asList("2024-01-01 00:00:00.123 UTC", "2024-06-15 12:30:45.987 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Arrow_NanosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.NANOS,
        true,
        Arrays.asList("2024-01-01T00:00:00.123456789Z", "2024-06-15T12:30:45.987654321Z"),
        Arrays.asList("2024-01-01 00:00:00.123456789 UTC", "2024-06-15 12:30:45.987654321 UTC"));
  }

  @Test
  public void testReadTableRowsWithSchema_Arrow_PicosPrecision() throws Exception {
    runTimestampTest(
        DataFormat.ARROW,
        TimestampPrecision.PICOS,
        true,
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"),
        Arrays.asList(
            "2024-01-01 00:00:00.123456789012 UTC", "2024-06-15 12:30:45.987654321098 UTC"));
  }

  @Test
  public void testTableSourceInitialSplit_withDirectReadPicosTimestampPrecisionNanos_Avro()
      throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(100L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    // Expected request should include AvroSerializationOptions with NANOS precision
    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable("projects/foo.com:project/datasets/dataset/tables/table")
                    .setDataFormat(DataFormat.AVRO)
                    .setReadOptions(
                        ReadSession.TableReadOptions.newBuilder()
                            .setAvroSerializationOptions(
                                com.google.cloud.bigquery.storage.v1.AvroSerializationOptions
                                    .newBuilder()
                                    .setPicosTimestampPrecision(
                                        com.google.cloud.bigquery.storage.v1
                                            .AvroSerializationOptions.PicosTimestampPrecision
                                            .TIMESTAMP_PRECISION_NANOS))))
            .setMaxStreamCount(10)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .setDataFormat(DataFormat.AVRO);
    for (int i = 0; i < 10; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            DataFormat.AVRO,
            null, /* selectedFields */
            null, /* rowRestriction */
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient),
            false, /* projectionPushdownApplied */
            TimestampPrecision.NANOS);

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(10L, options);
    assertEquals(10L, sources.size());
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
