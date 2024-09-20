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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.createTempTableReference;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.MoreCollectors;
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
 * Tests for {@link BigQueryIO#read(SerializableFunction)} using {@link Method#DIRECT_READ} and
 * {@link BigQueryIO.TypedRead#fromQuery(String)}.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageQueryTest {

  private transient BigQueryOptions options;
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
                  options = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
                  options.setProject("project-id");
                  if (description.getAnnotations().stream()
                      .anyMatch(a -> a.annotationType().equals(ProjectOverride.class))) {
                    options.as(BigQueryOptions.class).setBigQueryProject("bigquery-project-id");
                  }
                  options.setTempLocation(testFolder.getRoot().getAbsolutePath());
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };

          return testFolder.apply(withPipeline, description);
        }
      };

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();

  private FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
  }

  private static final String DEFAULT_QUERY = "SELECT * FROM `dataset.table` LIMIT 1";

  @Test
  public void testDefaultQueryBasedSource() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertTrue(typedRead.getValidate());
    assertTrue(typedRead.getFlattenResults());
    assertTrue(typedRead.getUseLegacySql());
    assertNull(typedRead.getQueryPriority());
    assertNull(typedRead.getQueryLocation());
    assertNull(typedRead.getKmsKey());
    assertFalse(typedRead.getWithTemplateCompatibility());
  }

  @Test
  public void testQueryBasedSourceWithCustomQuery() throws Exception {
    TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .fromQuery("SELECT * FROM `google.com:project.dataset.table`")
            .withCoder(TableRowJsonCoder.of());
    checkTypedReadQueryObject(typedRead, "SELECT * FROM `google.com:project.dataset.table`");
  }

  @Test
  public void testQueryBasedSourceWithoutValidation() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withoutValidation();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getValidate());
  }

  @Test
  public void testQueryBasedSourceWithoutResultFlattening() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withoutResultFlattening();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getFlattenResults());
  }

  @Test
  public void testQueryBasedSourceWithStandardSql() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().usingStandardSql();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getUseLegacySql());
  }

  @Test
  public void testQueryBasedSourceWithPriority() throws Exception {
    TypedRead<TableRow> typedRead =
        getDefaultTypedRead().withQueryPriority(QueryPriority.INTERACTIVE);
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals(QueryPriority.INTERACTIVE, typedRead.getQueryPriority());
  }

  @Test
  public void testQueryBasedSourceWithQueryLocation() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withQueryLocation("US");
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals("US", typedRead.getQueryLocation());
  }

  @Test
  public void testQueryBasedSourceWithKmsKey() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withKmsKey("kms_key");
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals("kms_key", typedRead.getKmsKey());
  }

  @Test
  public void testQueryBasedSourceWithTemplateCompatibility() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withTemplateCompatibility();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertTrue(typedRead.getWithTemplateCompatibility());
  }

  private TypedRead<TableRow> getDefaultTypedRead() {
    return BigQueryIO.read(new TableRowParser())
        .fromQuery(DEFAULT_QUERY)
        .withCoder(TableRowJsonCoder.of())
        .withMethod(Method.DIRECT_READ);
  }

  private void checkTypedReadQueryObject(TypedRead<?> typedRead, String query) {
    assertNull(typedRead.getTable());
    assertEquals(query, typedRead.getQuery().get());
  }

  @Test
  public void testBuildQueryBasedSourceWithSelectedFields() throws Exception {
    TypedRead<TableRow> typedRead =
        getDefaultTypedRead().withSelectedFields(Lists.newArrayList("a"));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies selected fields, "
            + "which only applies when reading from a table");
    p.apply(typedRead);
    p.run();
  }

  @Test
  public void testBuildQueryBasedSourceWithRowRestriction() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withRowRestriction("a > 5");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies row restriction, "
            + "which only applies when reading from a table");
    p.apply(typedRead);
    p.run();
  }

  @Test
  public void testDisplayData() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead();
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("query", DEFAULT_QUERY));
  }

  @Test
  public void testName() {
    assertEquals("BigQueryIO.TypedRead", getDefaultTypedRead().getName());
  }

  @Test
  public void testCoderInference() {
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
  public void testQuerySourceEstimatedSize() throws Exception {

    String fakeQuery = "fake query text";

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        fakeQuery,
        new JobStatistics().setQuery(new JobStatistics2().setTotalBytesProcessed(125L)));

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            /* stepUuid = */ "stepUuid",
            ValueProvider.StaticValueProvider.of(fakeQuery),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.INTERACTIVE,
            /* location = */ null,
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            fakeBigQueryServices);

    assertEquals(125L, querySource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testQuerySourceInitialSplit() throws Exception {
    doQuerySourceInitialSplit(1024L, 1024, 50);
  }

  @Test
  public void testQuerySourceInitialSplit_MinSplitCount() throws Exception {
    doQuerySourceInitialSplit(1024L * 1024L, 10, 1);
  }

  @Test
  public void testQuerySourceInitialSplit_MaxSplitCount() throws Exception {
    doQuerySourceInitialSplit(10, 10_000, 200);
  }

  private void doQuerySourceInitialSplit(
      long bundleSize, int requestedStreamCount, int expectedStreamCount) throws Exception {

    TableReference sourceTableRef = BigQueryHelpers.parseTableSpec("project:dataset.table");

    fakeDatasetService.createDataset(
        sourceTableRef.getProjectId(),
        sourceTableRef.getDatasetId(),
        "asia-northeast1",
        "Fake plastic tree^H^H^H^Htables",
        null);

    fakeDatasetService.createTable(
        new Table().setTableReference(sourceTableRef).setLocation("asia-northeast1"));

    Table queryResultTable =
        new Table()
            .setSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("name").setType("STRING"),
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .setNumBytes(1024L * 1024L);

    String encodedQuery = FakeBigQueryServices.encodeQueryResult(queryResultTable);

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        encodedQuery,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of(sourceTableRef))));

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getProject(),
            BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.QUERY),
            Optional.empty());

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getProject())
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable(BigQueryHelpers.toTableResourceName(tempTableReference)))
            .setMaxStreamCount(requestedStreamCount)
            .build();

    Schema sessionSchema =
        SchemaBuilder.record("__root__")
            .fields()
            .name("name")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("number")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .endRecord();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(sessionSchema.toString()))
            .setDataFormat(DataFormat.AVRO);
    for (int i = 0; i < expectedStreamCount; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(encodedQuery),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(bundleSize, options);
    assertEquals(expectedStreamCount, sources.size());
    assertEquals(
        TableRowJsonCoder.of(),
        sources.stream()
            .map(BoundedSource<TableRow>::getOutputCoder)
            .distinct()
            .collect(MoreCollectors.onlyElement()));
  }

  /**
   * This test simulates the scenario where the SQL text which is executed by the query job doesn't
   * by itself refer to any tables (e.g. "SELECT 17 AS value"), and thus there are no referenced
   * tables when the dry run of the query is performed.
   */
  @Test
  public void testQuerySourceInitialSplit_NoReferencedTables() throws Exception {

    Table queryResultTable =
        new Table()
            .setSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("name").setType("STRING"),
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .setNumBytes(1024L * 1024L);

    String encodedQuery = FakeBigQueryServices.encodeQueryResult(queryResultTable);

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        encodedQuery,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of())));

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getProject(),
            BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.QUERY),
            Optional.empty());

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getProject())
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable(BigQueryHelpers.toTableResourceName(tempTableReference)))
            .setMaxStreamCount(1024)
            .build();

    Schema sessionSchema =
        SchemaBuilder.record("__root__")
            .fields()
            .name("name")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("number")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .endRecord();

    ReadSession.Builder builder =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema(sessionSchema.toString()))
            .setDataFormat(DataFormat.AVRO);
    for (int i = 0; i < 1024; i++) {
      builder.addStreams(ReadStream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(encodedQuery),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(1024, options);
    assertEquals(1024, sources.size());
    assertEquals(
        TableRowJsonCoder.of(),
        sources.stream()
            .map(BoundedSource<TableRow>::getOutputCoder)
            .distinct()
            .collect(MoreCollectors.onlyElement()));
  }

  private static final String AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"RowRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
          + "     {\"name\": \"number\", \"type\": [\"null\", \"long\"]}\n"
          + " ]\n"
          + "}";

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA_STRING);

  private static final TableSchema TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("name").setType("STRING"),
                  new TableFieldSchema().setName("number").setType("INTEGER")));

  private static GenericRecord createRecord(String name, long number, Schema schema) {
    GenericRecord genericRecord = new Record(schema);
    genericRecord.put("name", name);
    genericRecord.put("number", number);
    return genericRecord;
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

  private static final class ParseKeyValue
      implements SerializableFunction<SchemaAndRecord, KV<String, Long>> {
    @Override
    public KV<String, Long> apply(SchemaAndRecord input) {
      return KV.of(
          input.getRecord().get("name").toString(), (Long) input.getRecord().get("number"));
    }
  }

  @Test
  @ProjectOverride
  public void testQuerySourceInitialSplitWithBigQueryProject_EmptyResult() throws Exception {

    TableReference sourceTableRef =
        BigQueryHelpers.parseTableSpec("bigquery-project-id:dataset.table");

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
        options.getBigQueryProject(),
        encodedQuery,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of(sourceTableRef))));

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getBigQueryProject(),
            BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.QUERY),
            Optional.empty());

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getBigQueryProject())
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable(BigQueryHelpers.toTableResourceName(tempTableReference))
                    .setDataFormat(DataFormat.AVRO))
            .setMaxStreamCount(10)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(encodedQuery),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            DataFormat.AVRO,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(1024L, options);
    assertTrue(sources.isEmpty());
  }

  @Test
  public void testQuerySourceInitialSplit_EmptyResult() throws Exception {

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

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getProject(),
            BigQueryResourceNaming.createJobIdPrefix(options.getJobName(), stepUuid, JobType.QUERY),
            Optional.empty());

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getProject())
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable(BigQueryHelpers.toTableResourceName(tempTableReference)))
            .setMaxStreamCount(10)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(encodedQuery),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(1024L, options);
    assertTrue(sources.isEmpty());
  }

  @Test
  public void testQuerySourceCreateReader() throws Exception {
    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            /* stepUuid = */ "testStepUuid",
            ValueProvider.StaticValueProvider.of("SELECT * FROM `dataset.table`"),
            /* flattenResults = */ false,
            /* useLegacySql = */ false,
            /* priority = */ QueryPriority.INTERACTIVE,
            /* location = */ "asia-northeast1",
            /* queryTempDataset = */ null,
            /* queryTempProject = */ null,
            /* kmsKey = */ null,
            null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            fakeBigQueryServices);

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("BigQuery storage source must be split before reading");
    querySource.createReader(options);
  }

  public TypedRead<KV<String, Long>> configureTypedRead(
      SerializableFunction<SchemaAndRecord, KV<String, Long>> parseFn) throws Exception {
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

    return BigQueryIO.read(parseFn)
        .fromQuery(encodedQuery)
        .withMethod(Method.DIRECT_READ)
        .withTestServices(
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));
  }

  @Test
  public void testReadFromBigQueryIO() throws Exception {
    doReadFromBigQueryIO(false);
  }

  @Test
  public void testReadFromBigQueryIOWithTemplateCompatibility() throws Exception {
    doReadFromBigQueryIO(true);
  }

  private void doReadFromBigQueryIO(boolean templateCompatibility) throws Exception {

    BigQueryIO.TypedRead<KV<String, Long>> typedRead = configureTypedRead(new ParseKeyValue());

    if (templateCompatibility) {
      typedRead = typedRead.withTemplateCompatibility();
    }

    PCollection<KV<String, Long>> output = p.apply(typedRead);

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

    TypedRead<KV<String, Long>> typedRead = configureTypedRead(new FailingParseKeyValue());

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());
    typedRead = typedRead.withErrorHandler(errorHandler);
    PCollection<KV<String, Long>> output = p.apply(typedRead);
    errorHandler.close();

    PAssert.that(output)
        .containsInAnyOrder(ImmutableList.of(KV.of("A", 1L), KV.of("C", 3L), KV.of("D", 4L)));

    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(1L);

    p.run();
  }
}
