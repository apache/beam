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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
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
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroRows;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroSchema;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ShardingStrategy;
import com.google.cloud.bigquery.storage.v1beta1.Storage.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamStatus;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
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
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
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
  public void testBuildSourceWithReadOptionsAndSelectedFields() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("withReadOptions() already called");
    p.apply(
        "ReadMyTable",
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withReadOptions(TableReadOptions.newBuilder().build())
            .withSelectedFields(Lists.newArrayList("field1")));
  }

  @Test
  public void testBuildSourceWithReadOptionsAndRowRestriction() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("withReadOptions() already called");
    p.apply(
        "ReadMyTable",
        BigQueryIO.read(new TableRowParser())
            .withCoder(TableRowJsonCoder.of())
            .withMethod(Method.DIRECT_READ)
            .from("foo.com:project:dataset.table")
            .withReadOptions(TableReadOptions.newBuilder().build())
            .withRowRestriction("field > 1"));
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

  private void doTableSourceInitialSplitTest(long bundleSize, int streamCount) throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table =
        new Table().setTableReference(tableRef).setNumBytes(1024L * 1024L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(streamCount)
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING));
    for (int i = 0; i < streamCount; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
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
  public void testTableSourceInitialSplit_WithTableReadOptions() throws Throwable {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table = new Table().setTableReference(tableRef).setNumBytes(100L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    TableReadOptions readOptions =
        TableReadOptions.newBuilder()
            .addSelectedFields("name")
            .setRowRestriction("number > 5")
            .build();

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .setReadOptions(readOptions)
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING));
    for (int i = 0; i < 10; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            readOptions,
            null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = tableSource.split(10L, options);
    assertEquals(10L, sources.size());
  }

  @Test
  public void testTableSourceInitialSplit_WithSelectedFieldsAndRowRestriction() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");

    Table table = new Table().setTableReference(tableRef).setNumBytes(100L).setSchema(TABLE_SCHEMA);

    fakeDatasetService.createTable(table);

    TableReadOptions readOptions =
        TableReadOptions.newBuilder()
            .addSelectedFields("name")
            .setRowRestriction("number > 5")
            .build();

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .setReadOptions(readOptions)
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING));
    for (int i = 0; i < 10; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
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
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(1024)
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING));
    for (int i = 0; i < 50; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(BigQueryHelpers.parseTableSpec("dataset.table")),
            null,
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
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(1024)
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageTableSource<TableRow> tableSource =
        BigQueryStorageTableSource.create(
            ValueProvider.StaticValueProvider.of(tableRef),
            null,
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

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  private static ReadRowsResponse createResponse(
      Schema schema, Collection<GenericRecord> genericRecords, double fractionConsumed)
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
        .setStatus(StreamStatus.newBuilder().setFractionConsumed((float) fractionConsumed))
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
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 3), 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

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
  public void testFractionConsumed() throws Exception {
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
            createRecord("C", 3, AVRO_SCHEMA),
            createRecord("D", 4, AVRO_SCHEMA),
            createRecord("E", 5, AVRO_SCHEMA),
            createRecord("F", 6, AVRO_SCHEMA),
            createRecord("G", 7, AVRO_SCHEMA));

    List<ReadRowsResponse> responses =
        Lists.newArrayList(
            // N.B.: All floating point numbers used in this test can be represented without
            // a loss of precision.
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.250),
            // Some responses may contain zero results, so we must ensure that we can are resilient
            // to such responses.
            createResponse(AVRO_SCHEMA, Lists.newArrayList(), 0.250),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.500),
            createResponse(AVRO_SCHEMA, records.subList(4, 7), 0.875));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest))
        .thenReturn(new FakeBigQueryServerStream<>(responses));

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

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(Double.valueOf(0.000), reader.getFractionConsumed());

    assertTrue(reader.start()); // Reads A.
    assertEquals(Double.valueOf(0.125), reader.getFractionConsumed());
    assertTrue(reader.advance()); // Reads B.
    assertEquals(Double.valueOf(0.250), reader.getFractionConsumed());

    assertTrue(reader.advance()); // Reads C.
    assertEquals(Double.valueOf(0.375), reader.getFractionConsumed());
    assertTrue(reader.advance()); // Reads D.
    assertEquals(Double.valueOf(0.500), reader.getFractionConsumed());

    assertTrue(reader.advance()); // Reads E.
    assertEquals(Double.valueOf(0.625), reader.getFractionConsumed());
    assertTrue(reader.advance()); // Reads F.
    assertEquals(Double.valueOf(0.750), reader.getFractionConsumed());
    assertTrue(reader.advance()); // Reads G.
    assertEquals(Double.valueOf(0.875), reader.getFractionConsumed());

    assertFalse(reader.advance()); // Reaches the end.

    // We are done with the stream, so we should report 100% consumption.
    assertEquals(Double.valueOf(1.00), reader.getFractionConsumed());
  }

  @Test
  public void testFractionConsumedWithSplit() throws Exception {
    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSession")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
            .build();

    Stream parentStream = Stream.newBuilder().setName("stream").build();

    ReadRowsRequest expectedRequest =
        ReadRowsRequest.newBuilder()
            .setReadPosition(StreamPosition.newBuilder().setStream(parentStream))
            .build();

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
            // N.B.: All floating point numbers used in this test can be represented without
            // a loss of precision.
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.250),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.500),
            createResponse(AVRO_SCHEMA, records.subList(4, 7), 0.875));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(expectedRequest))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(parentStream)
                .setFraction(0.5f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(Stream.newBuilder().setName("primary"))
                .setRemainderStream(Stream.newBuilder().setName("residual"))
                .build());

    List<ReadRowsResponse> primaryResponses =
        Lists.newArrayList(
            createResponse(AVRO_SCHEMA, records.subList(1, 3), 0.500),
            createResponse(AVRO_SCHEMA, records.subList(3, 4), 0.875));

    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(
                    StreamPosition.newBuilder()
                        .setStream(Stream.newBuilder().setName("primary"))
                        .setOffset(1))
                .build()))
        .thenReturn(new FakeBigQueryServerStream<>(primaryResponses));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            readSession,
            parentStream,
            TABLE_SCHEMA,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices().withStorageClient(fakeStorageClient));

    List<TableRow> rows = new ArrayList<>();
    BoundedReader<TableRow> reader = streamSource.createReader(options);

    // Before call to BoundedReader#start, fraction consumed must be zero.
    assertEquals(Double.valueOf(0.0000), reader.getFractionConsumed());

    assertTrue(reader.start()); // Reads A.
    assertEquals(Double.valueOf(0.1250), reader.getFractionConsumed());

    reader.splitAtFraction(0.5f);
    assertEquals(Double.valueOf(0.1250), reader.getFractionConsumed());

    assertTrue(reader.advance()); // Reads B.

    // Once the split has completed but no new rows have been read, the consumed value is at the
    // last calculated value of 0.125. For the first response of the primary stream, the progress
    // report interpolation is done between the progress before split and the progress from the
    // first response of the primary stream. In this case, the value is:
    //
    //   0.125 + (0.5 - 0.125) * 1.0 / 2
    //
    assertEquals(Double.valueOf(0.3125), reader.getFractionConsumed());

    assertTrue(reader.advance()); // Reads C.
    assertEquals(Double.valueOf(0.5000), reader.getFractionConsumed());

    assertTrue(reader.advance()); // Reads D.
    assertEquals(Double.valueOf(0.8750), reader.getFractionConsumed());

    assertFalse(reader.advance());
    assertEquals(Double.valueOf(1.0000), reader.getFractionConsumed());
  }

  @Test
  public void testStreamSourceSplitAtFractionSucceeds() throws Exception {
    Stream parentStream = Stream.newBuilder().setName("parent").build();

    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.25),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.50),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(parentStream))
                .build()))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(parentStream)
                .setFraction(0.5f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(Stream.newBuilder().setName("primary"))
                .setRemainderStream(Stream.newBuilder().setName("residual"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(
                    StreamPosition.newBuilder()
                        .setStream(Stream.newBuilder().setName("primary"))
                        // This test will read rows 0 and 1 from the parent before calling split,
                        // so we expect the primary read to start at offset 2.
                        .setOffset(2))
                .build()))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses.subList(1, 2)));
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(
                    StreamPosition.newBuilder()
                        .setStream(Stream.newBuilder().setName("residual"))
                        .setOffset(0))
                .build()))
        .thenReturn(
            new FakeBigQueryServerStream<>(parentResponses.subList(2, parentResponses.size())));

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            parentStream,
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
    List<Stream> streams =
        Lists.newArrayList(
            Stream.newBuilder().setName("stream1").build(),
            Stream.newBuilder().setName("stream2").build(),
            Stream.newBuilder().setName("stream3").build());

    StorageClient fakeStorageClient = mock(StorageClient.class);

    // Mock the initial ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(streams.get(0)))
                .build()))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                        0.25),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("C", 3, AVRO_SCHEMA), createRecord("D", 4, AVRO_SCHEMA)),
                        0.50),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("E", 5, AVRO_SCHEMA), createRecord("F", 6, AVRO_SCHEMA)),
                        0.75))));

    // Mock the first SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(streams.get(0))
                .setFraction(0.83f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(streams.get(1))
                .setRemainderStream(Stream.newBuilder().setName("ignored"))
                .build());

    // Mock the second ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(streams.get(1)).setOffset(1))
                .build()))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("B", 2, AVRO_SCHEMA), createRecord("C", 3, AVRO_SCHEMA)),
                        0.50),
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                        0.75))));

    // Mock the second SplitReadStream call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(streams.get(1))
                .setFraction(0.75f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(streams.get(2))
                .setRemainderStream(Stream.newBuilder().setName("ignored"))
                .build());

    // Mock the third ReadRows call.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(streams.get(2)).setOffset(2))
                .build()))
        .thenReturn(
            new FakeBigQueryServerStream<>(
                Lists.newArrayList(
                    createResponse(
                        AVRO_SCHEMA,
                        Lists.newArrayList(
                            createRecord("C", 3, AVRO_SCHEMA), createRecord("D", 4, AVRO_SCHEMA)),
                        0.90))));

    BoundedSource<TableRow> source =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            streams.get(0),
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
    Stream parentStream = Stream.newBuilder().setName("parent").build();

    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.25),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.50),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(parentStream))
                .build()))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(parentStream)
                .setFraction(0.5f)
                .build()))
        .thenReturn(SplitReadStreamResponse.getDefaultInstance());

    BigQueryStorageStreamSource<TableRow> streamSource =
        BigQueryStorageStreamSource.create(
            ReadSession.newBuilder()
                .setName("readSession")
                .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
                .build(),
            parentStream,
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
  public void testStreamSourceSplitAtFractionFailsWhenParentIsPastSplitPoint() throws Exception {
    Stream parentStream = Stream.newBuilder().setName("parent").build();

    List<ReadRowsResponse> parentResponses =
        Lists.newArrayList(
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("A", 1, AVRO_SCHEMA), createRecord("B", 2, AVRO_SCHEMA)),
                0.25),
            createResponse(
                AVRO_SCHEMA, Lists.newArrayList(createRecord("C", 3, AVRO_SCHEMA)), 0.50),
            createResponse(
                AVRO_SCHEMA,
                Lists.newArrayList(
                    createRecord("D", 4, AVRO_SCHEMA), createRecord("E", 5, AVRO_SCHEMA)),
                0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(StreamPosition.newBuilder().setStream(parentStream))
                .build()))
        .thenReturn(new FakeBigQueryServerStream<>(parentResponses));

    // Mocks the split call. A response without a primary_stream and remainder_stream means
    // that the split is not possible.
    // Mocks the split call.
    when(fakeStorageClient.splitReadStream(
            SplitReadStreamRequest.newBuilder()
                .setOriginalStream(parentStream)
                .setFraction(0.5f)
                .build()))
        .thenReturn(
            SplitReadStreamResponse.newBuilder()
                .setPrimaryStream(Stream.newBuilder().setName("primary"))
                .setRemainderStream(Stream.newBuilder().setName("residual"))
                .build());

    // Mocks the ReadRows calls expected on the primary and residual streams.
    when(fakeStorageClient.readRows(
            ReadRowsRequest.newBuilder()
                .setReadPosition(
                    StreamPosition.newBuilder()
                        .setStream(Stream.newBuilder().setName("primary"))
                        // This test will read rows 0 and 1 from the parent before calling split,
                        // so we expect the primary read to start at offset 2.
                        .setOffset(2))
                .build()))
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
            parentStream,
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

  @Test
  public void testReadFromBigQueryIO() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .setShardingStrategy(ShardingStrategy.BALANCED)
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
            createResponse(AVRO_SCHEMA, records.subList(0, 2), 0.50),
            createResponse(AVRO_SCHEMA, records.subList(2, 4), 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

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

  @Test
  public void testReadFromBigQueryIOWithTrimmedSchema() throws Exception {
    fakeDatasetService.createDataset("foo.com:project", "dataset", "", "", null);
    TableReference tableRef = BigQueryHelpers.parseTableSpec("foo.com:project:dataset.table");
    Table table = new Table().setTableReference(tableRef).setNumBytes(10L).setSchema(TABLE_SCHEMA);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/project-id")
            .setTableReference(BigQueryHelpers.toTableRefProto(tableRef))
            .setRequestedStreams(10)
            .setReadOptions(TableReadOptions.newBuilder().addSelectedFields("name"))
            .setShardingStrategy(ShardingStrategy.BALANCED)
            .build();

    ReadSession readSession =
        ReadSession.newBuilder()
            .setName("readSessionName")
            .setAvroSchema(AvroSchema.newBuilder().setSchema(TRIMMED_AVRO_SCHEMA_STRING))
            .addStreams(Stream.newBuilder().setName("streamName"))
            .build();

    ReadRowsRequest expectedReadRowsRequest =
        ReadRowsRequest.newBuilder()
            .setReadPosition(
                StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("streamName")))
            .build();

    List<GenericRecord> records =
        Lists.newArrayList(
            createRecord("A", TRIMMED_AVRO_SCHEMA),
            createRecord("B", TRIMMED_AVRO_SCHEMA),
            createRecord("C", TRIMMED_AVRO_SCHEMA),
            createRecord("D", TRIMMED_AVRO_SCHEMA));

    List<ReadRowsResponse> readRowsResponses =
        Lists.newArrayList(
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(0, 2), 0.50),
            createResponse(TRIMMED_AVRO_SCHEMA, records.subList(2, 4), 0.75));

    StorageClient fakeStorageClient = mock(StorageClient.class, withSettings().serializable());
    when(fakeStorageClient.createReadSession(expectedCreateReadSessionRequest))
        .thenReturn(readSession);
    when(fakeStorageClient.readRows(expectedReadRowsRequest))
        .thenReturn(new FakeBigQueryServerStream<>(readRowsResponses));

    PCollection<TableRow> output =
        p.apply(
            BigQueryIO.readTableRows()
                .from("foo.com:project:dataset.table")
                .withMethod(Method.DIRECT_READ)
                .withSelectedFields(Lists.newArrayList("name"))
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
}
