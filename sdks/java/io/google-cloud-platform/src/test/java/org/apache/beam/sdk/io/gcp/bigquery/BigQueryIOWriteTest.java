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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.toJsonString;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.bigquery.WritePartition.ResultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteTables.Result;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;

/** Tests for {@link BigQueryIO#write}. */
@RunWith(Parameterized.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigQueryIOWriteTest implements Serializable {
  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline p;

  @Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(
        new Object[] {false, false, false},
        new Object[] {false, false, true},
        new Object[] {true, false, false},
        new Object[] {true, false, true},
        new Object[] {true, true, true});
  }

  @Parameter(0)
  public boolean useStorageApi;

  @Parameter(1)
  public boolean useStorageApiApproximate;

  @Parameter(2)
  public boolean useStreaming;

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          // We need to set up the temporary folder, and then set up the TestPipeline based on the
          // chosen folder. Unfortunately, since rule evaluation order is unspecified and unrelated
          // to field order, and is separate from construction, that requires manually creating this
          // TestRule.
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
                  bqOptions.setProject("project-id");
                  if (description.getAnnotations().stream()
                      .anyMatch(a -> a.annotationType().equals(ProjectOverride.class))) {
                    options.as(BigQueryOptions.class).setBigQueryProject("bigquery-project-id");
                  }
                  bqOptions.setTempLocation(testFolder.getRoot().getAbsolutePath());
                  if (useStorageApi) {
                    bqOptions.setUseStorageWriteApi(true);
                    if (useStorageApiApproximate) {
                      bqOptions.setUseStorageWriteApiAtLeastOnce(true);
                    }
                    if (useStreaming) {
                      bqOptions.setNumStorageWriteApiStreams(2);
                      bqOptions.setStorageWriteApiTriggeringFrequencySec(1);
                    }
                  }
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public transient ExpectedLogs loggedWriteRename = ExpectedLogs.none(WriteRename.class);

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  @Before
  public void setUp() throws IOException, InterruptedException {
    FakeDatasetService.setUp();
    BigQueryIO.clearCreatedTables();
    fakeDatasetService.createDataset("bigquery-project-id", "dataset-id", "", "", null);
    fakeDatasetService.createDataset("bigquery-project-id", "temp-dataset-id", "", "", null);
    fakeDatasetService.createDataset("project-id", "dataset-id", "", "", null);
    fakeDatasetService.createDataset("project-id", "temp-dataset-id", "", "", null);
  }

  @After
  public void tearDown() throws IOException {
    testNumFiles(new File(options.getTempLocation()), 0);
  }

  // Create an intermediate type to ensure that coder inference up the inheritance tree is tested.
  abstract static class StringLongDestinations extends DynamicDestinations<String, Long> {}

  @Test
  public void testWriteEmptyPCollection() throws Exception {
    if (useStreaming || useStorageApi) {
      return;
    }
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(new TableFieldSchema().setName("number").setType("INTEGER")));

    p.apply(Create.empty(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withTestServices(fakeBqServices)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(schema)
                .withoutValidation());
    p.run();

    checkNotNull(
        fakeDatasetService.getTable(
            BigQueryHelpers.parseTableSpec("project-id:dataset-id.table-id")));
  }

  @Test
  public void testWriteDynamicDestinations() throws Exception {
    writeDynamicDestinations(false, false);
  }

  @Test
  public void testWriteDynamicDestinationsBatchWithSchemas() throws Exception {
    writeDynamicDestinations(true, false);
  }

  @Test
  public void testWriteDynamicDestinationsStreamingWithAutoSharding() throws Exception {
    if (useStorageApi) {
      return;
    }
    if (!useStreaming) {
      return;
    }
    writeDynamicDestinations(true, true);
  }

  public void writeDynamicDestinations(boolean schemas, boolean autoSharding) throws Exception {
    final Schema schema =
        Schema.builder().addField("name", FieldType.STRING).addField("id", FieldType.INT32).build();

    final Pattern userPattern = Pattern.compile("([a-z]+)([0-9]+)");

    final PCollectionView<List<String>> sideInput1 =
        p.apply("Create SideInput 1", Create.of("a", "b", "c").withCoder(StringUtf8Coder.of()))
            .apply("asList", View.asList());
    final PCollectionView<Map<String, String>> sideInput2 =
        p.apply("Create SideInput2", Create.of(KV.of("a", "a"), KV.of("b", "b"), KV.of("c", "c")))
            .apply("AsMap", View.asMap());

    final List<String> allUsernames = ImmutableList.of("bill", "bob", "randolph");
    List<String> userList = Lists.newArrayList();
    // Make sure that we generate enough users so that WriteBundlesToFiles is forced to spill to
    // WriteGroupedRecordsToFiles.
    for (int i = 0; i < BatchLoads.DEFAULT_MAX_NUM_WRITERS_PER_BUNDLE * 10; ++i) {
      // Every user has 10 nicknames.
      for (int j = 0; j < 10; ++j) {
        String nickname =
            allUsernames.get(ThreadLocalRandom.current().nextInt(allUsernames.size()));
        userList.add(nickname + i);
      }
    }
    PCollection<String> users =
        p.apply("CreateUsers", Create.of(userList))
            .apply(Window.into(new PartitionedGlobalWindows<>(arg -> arg)));

    if (useStreaming) {
      users = users.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    }

    if (schemas) {
      users =
          users.setSchema(
              schema,
              TypeDescriptors.strings(),
              user -> {
                Matcher matcher = userPattern.matcher(user);
                checkState(matcher.matches());
                return Row.withSchema(schema)
                    .addValue(matcher.group(1))
                    .addValue(Integer.valueOf(matcher.group(2)))
                    .build();
              },
              r -> r.getString(0) + r.getInt32(1));
    }

    // Use a partition decorator to verify that partition decorators are supported.
    final String partitionDecorator = "20171127";

    BigQueryIO.Write<String> write =
        BigQueryIO.<String>write()
            .withTestServices(fakeBqServices)
            .withMaxFilesPerBundle(5)
            .withMaxFileSize(10)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .to(
                new StringLongDestinations() {
                  @Override
                  public Long getDestination(ValueInSingleWindow<String> element) {
                    assertThat(
                        element.getWindow(), Matchers.instanceOf(PartitionedGlobalWindow.class));
                    Matcher matcher = userPattern.matcher(element.getValue());
                    checkState(matcher.matches());
                    // Since we name tables by userid, we can simply store a Long to represent
                    // a table.
                    return Long.valueOf(matcher.group(2));
                  }

                  @Override
                  public TableDestination getTable(Long userId) {
                    verifySideInputs();
                    // Each user in it's own table.
                    return new TableDestination(
                        "dataset-id.userid-" + userId + "$" + partitionDecorator,
                        "table for userid " + userId);
                  }

                  @Override
                  public TableSchema getSchema(Long userId) {
                    verifySideInputs();
                    return new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("name").setType("STRING"),
                                new TableFieldSchema().setName("id").setType("INTEGER")));
                  }

                  @Override
                  public List<PCollectionView<?>> getSideInputs() {
                    return ImmutableList.of(sideInput1, sideInput2);
                  }

                  private void verifySideInputs() {
                    assertThat(sideInput(sideInput1), containsInAnyOrder("a", "b", "c"));
                    Map<String, String> mapSideInput = sideInput(sideInput2);
                    assertEquals(3, mapSideInput.size());
                    assertThat(
                        mapSideInput,
                        allOf(hasEntry("a", "a"), hasEntry("b", "b"), hasEntry("c", "c")));
                  }
                })
            .withoutValidation();
    if (schemas) {
      write = write.useBeamSchema();
    } else {
      write =
          write.withFormatFunction(
              user -> {
                Matcher matcher = userPattern.matcher(user);
                checkState(matcher.matches());
                return new TableRow().set("name", matcher.group(1)).set("id", matcher.group(2));
              });
    }
    if (autoSharding) {
      write = write.withAutoSharding();
    }
    WriteResult results = users.apply("WriteBigQuery", write);

    if (!useStreaming && !useStorageApi) {
      PCollection<TableDestination> successfulBatchInserts = results.getSuccessfulTableLoads();
      TableDestination[] expectedTables =
          userList.stream()
              .map(
                  user -> {
                    Matcher matcher = userPattern.matcher(user);
                    checkState(matcher.matches());
                    String userId = matcher.group(2);
                    return new TableDestination(
                        String.format("project-id:dataset-id.userid-%s$20171127", userId),
                        String.format("table for userid %s", userId));
                  })
              .distinct()
              .toArray(TableDestination[]::new);

      PAssert.that(successfulBatchInserts.apply(Distinct.create()))
          .containsInAnyOrder(expectedTables);
    }

    p.run();

    Map<Long, List<TableRow>> expectedTableRows = Maps.newHashMap();
    for (String anUserList : userList) {
      Matcher matcher = userPattern.matcher(anUserList);
      checkState(matcher.matches());
      String nickname = matcher.group(1);
      Long userid = Long.valueOf(matcher.group(2));
      List<TableRow> expected =
          expectedTableRows.computeIfAbsent(userid, k -> Lists.newArrayList());
      expected.add(new TableRow().set("name", nickname).set("id", userid.toString()));
    }

    for (Map.Entry<Long, List<TableRow>> entry : expectedTableRows.entrySet()) {
      assertThat(
          fakeDatasetService.getAllRows("project-id", "dataset-id", "userid-" + entry.getKey()),
          containsInAnyOrder(Iterables.toArray(entry.getValue(), TableRow.class)));
    }
  }

  void testTimePartitioningClustering(
      BigQueryIO.Write.Method insertMethod, boolean enablePartitioning, boolean enableClustering)
      throws Exception {
    TableRow row1 = new TableRow().set("date", "2018-01-01").set("number", "1");
    TableRow row2 = new TableRow().set("date", "2018-01-02").set("number", "2");

    TimePartitioning timePartitioning = new TimePartitioning().setType("DAY").setField("date");
    Clustering clustering = new Clustering().setFields(ImmutableList.of("date"));
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("date").setType("DATE"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));

    Write<TableRow> writeTransform =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withTestServices(fakeBqServices)
            .withMethod(insertMethod)
            .withSchema(schema)
            .withoutValidation();

    if (enablePartitioning) {
      writeTransform = writeTransform.withTimePartitioning(timePartitioning);
    }
    if (enableClustering) {
      writeTransform = writeTransform.withClustering(clustering);
    }

    p.apply(Create.of(row1, row2)).apply(writeTransform);
    p.run();
    Table table =
        fakeDatasetService.getTable(
            BigQueryHelpers.parseTableSpec("project-id:dataset-id.table-id"));

    assertEquals(schema, table.getSchema());
    if (enablePartitioning) {
      assertEquals(timePartitioning, table.getTimePartitioning());
    }
    if (enableClustering) {
      assertEquals(clustering, table.getClustering());
    }
  }

  void testTimePartitioning(BigQueryIO.Write.Method insertMethod) throws Exception {
    testTimePartitioningClustering(insertMethod, true, false);
  }

  void testClustering(BigQueryIO.Write.Method insertMethod) throws Exception {
    testTimePartitioningClustering(insertMethod, true, true);
  }

  @Test
  public void testTimePartitioning() throws Exception {
    BigQueryIO.Write.Method method;
    if (useStorageApi) {
      method =
          useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;
    } else if (useStreaming) {
      method = Method.STREAMING_INSERTS;
    } else {
      method = Method.FILE_LOADS;
    }
    testTimePartitioning(method);
  }

  @Test
  public void testTimePartitioningStorageApi() throws Exception {
    if (!useStorageApi) {
      return;
    }
    testTimePartitioning(Method.STORAGE_WRITE_API);
  }

  @Test
  public void testClusteringStorageApi() throws Exception {
    if (useStorageApi) {
      testClustering(
          useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API);
    }
  }

  @Test
  public void testClusteringTableFunction() throws Exception {
    TableRow row1 = new TableRow().set("date", "2018-01-01").set("number", "1");
    TableRow row2 = new TableRow().set("date", "2018-01-02").set("number", "2");

    TimePartitioning timePartitioning = new TimePartitioning().setType("DAY").setField("date");
    Clustering clustering = new Clustering().setFields(ImmutableList.of("date"));
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("date").setType("DATE"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));

    // withMethod overrides the pipeline option, so we need to explicitly request
    // STORAGE_API_WRITES.
    BigQueryIO.Write.Method method =
        useStorageApi
            ? (useStorageApiApproximate
                ? Method.STORAGE_API_AT_LEAST_ONCE
                : Method.STORAGE_WRITE_API)
            : Method.FILE_LOADS;
    p.apply(Create.of(row1, row2))
        .apply(
            BigQueryIO.writeTableRows()
                .to(
                    (ValueInSingleWindow<TableRow> vsw) -> {
                      String tableSpec =
                          "project-id:dataset-id.table-" + vsw.getValue().get("number");
                      return new TableDestination(
                          tableSpec,
                          null,
                          new TimePartitioning().setType("DAY").setField("date"),
                          new Clustering().setFields(ImmutableList.of("date")));
                    })
                .withTestServices(fakeBqServices)
                .withMethod(method)
                .withSchema(schema)
                .withClustering()
                .withoutValidation());
    p.run();
    Table table =
        fakeDatasetService.getTable(
            BigQueryHelpers.parseTableSpec("project-id:dataset-id.table-1"));
    assertEquals(schema, table.getSchema());
    assertEquals(timePartitioning, table.getTimePartitioning());
    assertEquals(clustering, table.getClustering());
  }

  @Test
  public void testTriggeredFileLoads() throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }

    TestStream<TableRow> testStream =
        TestStream.create(TableRowJsonCoder.of())
            .addElements(
                elements.get(0), Iterables.toArray(elements.subList(1, 10), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), TableRow.class))
            .advanceWatermarkToInfinity();

    BigQueryIO.Write.Method method = Method.FILE_LOADS;
    p.apply(testStream)
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withTriggeringFrequency(Duration.standardSeconds(30))
                .withNumFileShards(2)
                .withMethod(method)
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  @Test
  public void testTriggeredFileLoadsWithTempTablesAndDataset() throws Exception {
    String tableRef = "bigquery-project-id:dataset-id.table-id";
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }
    TestStream<TableRow> testStream =
        TestStream.create(TableRowJsonCoder.of())
            .addElements(
                elements.get(0), Iterables.toArray(elements.subList(1, 10), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), TableRow.class))
            .advanceWatermarkToInfinity();

    BigQueryIO.Write.Method method = Method.FILE_LOADS;
    p.apply(testStream)
        .apply(
            BigQueryIO.writeTableRows()
                .to(tableRef)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withTriggeringFrequency(Duration.standardSeconds(30))
                .withNumFileShards(2)
                .withMaxBytesPerPartition(1)
                .withMaxFilesPerPartition(1)
                .withMethod(method)
                .withoutValidation()
                .withWriteTempDataset("temp-dataset-id"));
    p.run();

    final int projectIdSplitter = tableRef.indexOf(':');
    final String projectId =
        projectIdSplitter == -1 ? "project-id" : tableRef.substring(0, projectIdSplitter);

    assertThat(
        fakeDatasetService.getAllRows(projectId, "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  public void testTriggeredFileLoadsWithTempTables(String tableRef) throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }

    TestStream<TableRow> testStream =
        TestStream.create(TableRowJsonCoder.of())
            .addElements(
                elements.get(0), Iterables.toArray(elements.subList(1, 10), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), TableRow.class))
            .advanceWatermarkToInfinity();

    BigQueryIO.Write.Method method = Method.FILE_LOADS;
    p.apply(testStream)
        .apply(
            BigQueryIO.writeTableRows()
                .to(tableRef)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withTriggeringFrequency(Duration.standardSeconds(30))
                .withNumFileShards(2)
                .withMaxBytesPerPartition(1)
                .withMaxFilesPerPartition(1)
                .withMethod(method)
                .withoutValidation());
    p.run();

    final int projectIdSplitter = tableRef.indexOf(':');
    final String projectId =
        projectIdSplitter == -1 ? "project-id" : tableRef.substring(0, projectIdSplitter);

    assertThat(
        fakeDatasetService.getAllRows(projectId, "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  @Test
  @ProjectOverride
  public void testTriggeredFileLoadsWithTempTablesBigQueryProject() throws Exception {
    testTriggeredFileLoadsWithTempTables("bigquery-project-id:dataset-id.table-id");
  }

  @Test
  public void testTriggeredFileLoadsWithTempTables() throws Exception {
    testTriggeredFileLoadsWithTempTables("project-id:dataset-id.table-id");
  }

  @Test
  public void testUntriggeredFileLoadsWithTempTables() throws Exception {
    // Test only non-streaming inserts.
    if (useStorageApi || useStreaming) {
      return;
    }
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }
    p.apply(Create.of(elements))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withMaxBytesPerPartition(1)
                .withMaxFilesPerPartition(1)
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  @Test
  public void testTriggeredFileLoadsWithTempTablesDefaultProject() throws Exception {
    testTriggeredFileLoadsWithTempTables("dataset-id.table-id");
  }

  @Test
  public void testTriggeredFileLoadsWithAutoSharding() throws Exception {
    if (useStorageApi || !useStreaming) {
      // This test does not make sense for the storage API.
      return;
    }
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }

    Instant startInstant = new Instant(0L);
    TestStream<TableRow> testStream =
        TestStream.create(TableRowJsonCoder.of())
            // Initialize watermark for timer to be triggered correctly.
            .advanceWatermarkTo(startInstant)
            .addElements(
                elements.get(0), Iterables.toArray(elements.subList(1, 10), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(10)))
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(30)))
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), TableRow.class))
            .advanceProcessingTime(Duration.standardMinutes(2))
            .advanceWatermarkToInfinity();

    int numTables = 3;
    p.apply(testStream)
        .apply(
            BigQueryIO.writeTableRows()
                .to(
                    (ValueInSingleWindow<TableRow> vsw) -> {
                      String tableSpec =
                          "project-id:dataset-id.table-"
                              + ((int) vsw.getValue().get("number") % numTables);
                      return new TableDestination(tableSpec, null);
                    })
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                // Set a triggering frequency without needing to also specify numFileShards when
                // using autoSharding.
                .withTriggeringFrequency(Duration.standardSeconds(100))
                .withAutoSharding()
                .withMaxBytesPerPartition(1000)
                .withMaxFilesPerPartition(10)
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withoutValidation());
    p.run();

    Map<Integer, List<TableRow>> elementsByTableIdx = new HashMap<>();
    for (int i = 0; i < elements.size(); i++) {
      elementsByTableIdx
          .computeIfAbsent(i % numTables, k -> new ArrayList<>())
          .add(elements.get(i));
    }
    for (Map.Entry<Integer, List<TableRow>> entry : elementsByTableIdx.entrySet()) {
      assertThat(
          fakeDatasetService.getAllRows("project-id", "dataset-id", "table-" + entry.getKey()),
          containsInAnyOrder(Iterables.toArray(entry.getValue(), TableRow.class)));
    }
    // For each table destination, it's expected to create two load jobs based on the triggering
    // frequency and processing time intervals.
    assertEquals(2 * numTables, fakeDatasetService.getInsertCount());
  }

  @Test
  public void testFailuresNoRetryPolicy() throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    TableRow row1 = new TableRow().set("name", "a").set("number", "1");
    TableRow row2 = new TableRow().set("name", "b").set("number", "2");
    TableRow row3 = new TableRow().set("name", "c").set("number", "3");

    TableDataInsertAllResponse.InsertErrors ephemeralError =
        new TableDataInsertAllResponse.InsertErrors()
            .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout")));

    fakeDatasetService.failOnInsert(
        ImmutableMap.of(
            row1, ImmutableList.of(ephemeralError, ephemeralError),
            row2, ImmutableList.of(ephemeralError, ephemeralError)));

    p.apply(Create.of(row1, row2, row3))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("name").setType("STRING"),
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(row1, row2, row3));
  }

  @Test
  public void testRetryPolicy() throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    TableRow row1 = new TableRow().set("name", "a").set("number", "1");
    TableRow row2 = new TableRow().set("name", "b").set("number", "2");
    TableRow row3 = new TableRow().set("name", "c").set("number", "3");

    TableDataInsertAllResponse.InsertErrors ephemeralError =
        new TableDataInsertAllResponse.InsertErrors()
            .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout")));
    TableDataInsertAllResponse.InsertErrors persistentError =
        new TableDataInsertAllResponse.InsertErrors()
            .setErrors(ImmutableList.of(new ErrorProto().setReason("invalidQuery")));

    fakeDatasetService.failOnInsert(
        ImmutableMap.of(
            row1, ImmutableList.of(ephemeralError, ephemeralError),
            row2, ImmutableList.of(ephemeralError, ephemeralError, persistentError)));

    WriteResult result =
        p.apply(Create.of(row1, row2, row3))
            .apply(
                BigQueryIO.writeTableRows()
                    .to("project-id:dataset-id.table-id")
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(Method.STREAMING_INSERTS)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("name").setType("STRING"),
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withTestServices(fakeBqServices)
                    .withoutValidation());

    PCollection<TableRow> failedRows = result.getFailedInserts();
    // row2 finally fails with a non-retryable error, so we expect to see it in the collection of
    // failed rows.
    PAssert.that(failedRows).containsInAnyOrder(row2);
    if (useStorageApi || !useStreaming) {
      PAssert.that(result.getSuccessfulInserts()).containsInAnyOrder(row1, row3);
    }
    p.run();

    // Only row1 and row3 were successfully inserted.
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(row1, row3));
  }

  @Test
  public void testWrite() throws Exception {
    p.apply(
            Create.of(
                    new TableRow().set("name", "a").set("number", 1),
                    new TableRow().set("name", "b").set("number", 2),
                    new TableRow().set("name", "c").set("number", 3))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("name").setType("STRING"),
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();
  }

  @Test
  public void testWriteWithSuccessfulBatchInserts() throws Exception {
    if (useStreaming || useStorageApi) {
      return;
    }

    WriteResult result =
        p.apply(
                Create.of(
                        new TableRow().set("name", "a").set("number", 1),
                        new TableRow().set("name", "b").set("number", 2),
                        new TableRow().set("name", "c").set("number", 3))
                    .withCoder(TableRowJsonCoder.of()))
            .apply(
                BigQueryIO.writeTableRows()
                    .to("dataset-id.table-id")
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("name").setType("STRING"),
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withTestServices(fakeBqServices)
                    .withoutValidation());

    PAssert.that(result.getSuccessfulTableLoads())
        .containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));

    p.run();
  }

  @Test
  public void testWriteWithSuccessfulBatchInsertsAndWriteRename() throws Exception {
    if (useStreaming || useStorageApi) {
      return;
    }

    WriteResult result =
        p.apply(
                Create.of(
                        new TableRow().set("name", "a").set("number", 1),
                        new TableRow().set("name", "b").set("number", 2),
                        new TableRow().set("name", "c").set("number", 3))
                    .withCoder(TableRowJsonCoder.of()))
            .apply(
                BigQueryIO.writeTableRows()
                    .to("dataset-id.table-id")
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("name").setType("STRING"),
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withMaxFileSize(1)
                    .withMaxFilesPerPartition(1)
                    .withTestServices(fakeBqServices)
                    .withoutValidation());

    PAssert.that(result.getSuccessfulTableLoads())
        .containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));

    p.run();
  }

  @Test
  public void testWriteWithoutInsertId() throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    TableRow row1 = new TableRow().set("name", "a").set("number", 1);
    TableRow row2 = new TableRow().set("name", "b").set("number", 2);
    TableRow row3 = new TableRow().set("name", "c").set("number", 3);
    p.apply(Create.of(row1, row2, row3).withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("name").setType("STRING"),
                                new TableFieldSchema().setName("number").setType("INTEGER"))))
                .withTestServices(fakeBqServices)
                .ignoreInsertIds()
                .withoutValidation());
    p.run();
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(row1, row2, row3));
    // Verify no insert id is added.
    assertThat(
        fakeDatasetService.getAllIds("project-id", "dataset-id", "table-id"), containsInAnyOrder());
  }

  @AutoValue
  abstract static class InputRecord implements Serializable {

    public static InputRecord create(
        String strValue, long longVal, double doubleVal, Instant instantVal) {
      return new AutoValue_BigQueryIOWriteTest_InputRecord(
          strValue, longVal, doubleVal, instantVal);
    }

    abstract String strVal();

    abstract long longVal();

    abstract double doubleVal();

    abstract Instant instantVal();
  }

  private static final Coder<InputRecord> INPUT_RECORD_CODER =
      SerializableCoder.of(InputRecord.class);

  @Test
  public void testWriteAvro() throws Exception {
    if (useStorageApi || useStreaming) {
      return;
    }
    p.apply(
            Create.of(
                    InputRecord.create("test", 1, 1.0, Instant.parse("2019-01-01T00:00:00Z")),
                    InputRecord.create("test2", 2, 2.0, Instant.parse("2019-02-01T00:00:00Z")))
                .withCoder(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("strVal").setType("STRING"),
                                new TableFieldSchema().setName("longVal").setType("INTEGER"),
                                new TableFieldSchema().setName("doubleVal").setType("FLOAT"),
                                new TableFieldSchema().setName("instantVal").setType("TIMESTAMP"))))
                .withTestServices(fakeBqServices)
                .withAvroFormatFunction(
                    r -> {
                      GenericRecord rec = new GenericData.Record(r.getSchema());
                      InputRecord i = r.getElement();
                      rec.put("strVal", i.strVal());
                      rec.put("longVal", i.longVal());
                      rec.put("doubleVal", i.doubleVal());
                      rec.put("instantVal", i.instantVal().getMillis() * 1000);
                      return rec;
                    })
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow()
                .set("strVal", "test")
                .set("longVal", "1")
                .set("doubleVal", 1.0D)
                .set("instantVal", "2019-01-01 00:00:00 UTC"),
            new TableRow()
                .set("strVal", "test2")
                .set("longVal", "2")
                .set("doubleVal", 2.0D)
                .set("instantVal", "2019-02-01 00:00:00 UTC")));
  }

  @Test
  public void testWriteAvroWithCustomWriter() throws Exception {
    if (useStorageApi || useStreaming) {
      return;
    }
    SerializableFunction<AvroWriteRequest<InputRecord>, GenericRecord> formatFunction =
        r -> {
          GenericRecord rec = new GenericData.Record(r.getSchema());
          InputRecord i = r.getElement();
          rec.put("strVal", i.strVal());
          rec.put("longVal", i.longVal());
          rec.put("doubleVal", i.doubleVal());
          rec.put("instantVal", i.instantVal().getMillis() * 1000);
          return rec;
        };

    SerializableFunction<org.apache.avro.Schema, DatumWriter<GenericRecord>> customWriterFactory =
        s ->
            new GenericDatumWriter<GenericRecord>() {
              @Override
              protected void writeString(org.apache.avro.Schema schema, Object datum, Encoder out)
                  throws IOException {
                super.writeString(schema, datum.toString() + "_custom", out);
              }
            };

    p.apply(
            Create.of(
                    InputRecord.create("test", 1, 1.0, Instant.parse("2019-01-01T00:00:00Z")),
                    InputRecord.create("test2", 2, 2.0, Instant.parse("2019-02-01T00:00:00Z")))
                .withCoder(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("strVal").setType("STRING"),
                                new TableFieldSchema().setName("longVal").setType("INTEGER"),
                                new TableFieldSchema().setName("doubleVal").setType("FLOAT"),
                                new TableFieldSchema().setName("instantVal").setType("TIMESTAMP"))))
                .withTestServices(fakeBqServices)
                .withAvroWriter(formatFunction, customWriterFactory)
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow()
                .set("strVal", "test_custom")
                .set("longVal", "1")
                .set("doubleVal", 1.0D)
                .set("instantVal", "2019-01-01 00:00:00 UTC"),
            new TableRow()
                .set("strVal", "test2_custom")
                .set("longVal", "2")
                .set("doubleVal", 2.0D)
                .set("instantVal", "2019-02-01 00:00:00 UTC")));
  }

  @Test
  public void testStreamingWrite() throws Exception {
    streamingWrite(false);
  }

  @Test
  public void testStreamingWriteWithAutoSharding() throws Exception {
    if (useStorageApi) {
      return;
    }
    streamingWrite(true);
  }

  private void streamingWrite(boolean autoSharding) throws Exception {
    if (!useStreaming) {
      return;
    }
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("name").setType("STRING"),
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withTestServices(fakeBqServices)
            .withoutValidation();
    if (autoSharding) {
      write = write.withAutoSharding();
    }
    p.apply(
            Create.of(
                    new TableRow().set("name", "a").set("number", "1"),
                    new TableRow().set("name", "b").set("number", "2"),
                    new TableRow().set("name", "c").set("number", "3"),
                    new TableRow().set("name", "d").set("number", "4"))
                .withCoder(TableRowJsonCoder.of()))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply("WriteToBQ", write);
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow().set("name", "a").set("number", "1"),
            new TableRow().set("name", "b").set("number", "2"),
            new TableRow().set("name", "c").set("number", "3"),
            new TableRow().set("name", "d").set("number", "4")));
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class SchemaPojo {
    final String name;
    final int number;

    @SchemaCreate
    SchemaPojo(String name, int number) {
      this.name = name;
      this.number = number;
    }
  }

  @Test
  public void testSchemaWriteLoads() throws Exception {
    // withMethod overrides the pipeline option, so we need to explicitly request
    // STORAGE_API_WRITES.
    BigQueryIO.Write.Method method =
        useStorageApi
            ? (useStorageApiApproximate
                ? Method.STORAGE_API_AT_LEAST_ONCE
                : Method.STORAGE_WRITE_API)
            : Method.FILE_LOADS;
    p.apply(
            Create.of(
                new SchemaPojo("a", 1),
                new SchemaPojo("b", 2),
                new SchemaPojo("c", 3),
                new SchemaPojo("d", 4)))
        .apply(
            BigQueryIO.<SchemaPojo>write()
                .to("project-id:dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(method)
                .useBeamSchema()
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();

    System.err.println(
        "Wrote: " + fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"));
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow().set("name", "a").set("number", "1"),
            new TableRow().set("name", "b").set("number", "2"),
            new TableRow().set("name", "c").set("number", "3"),
            new TableRow().set("name", "d").set("number", "4")));
  }

  @Test
  public void testSchemaWriteStreams() throws Exception {
    if (useStorageApi || !useStreaming) {
      return;
    }
    WriteResult result =
        p.apply(
                Create.of(
                    new SchemaPojo("a", 1),
                    new SchemaPojo("b", 2),
                    new SchemaPojo("c", 3),
                    new SchemaPojo("d", 4)))
            .apply(
                BigQueryIO.<SchemaPojo>write()
                    .to("project-id:dataset-id.table-id")
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(Method.STREAMING_INSERTS)
                    .useBeamSchema()
                    .withTestServices(fakeBqServices)
                    .withoutValidation());

    PAssert.that(result.getSuccessfulInserts())
        .satisfies(
            new SerializableFunction<Iterable<TableRow>, Void>() {
              @Override
              public Void apply(Iterable<TableRow> input) {
                assertThat(Lists.newArrayList(input).size(), is(4));
                return null;
              }
            });
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow().set("name", "a").set("number", "1"),
            new TableRow().set("name", "b").set("number", "2"),
            new TableRow().set("name", "c").set("number", "3"),
            new TableRow().set("name", "d").set("number", "4")));
  }

  /**
   * A generic window function that allows partitioning data into windows by a string value.
   *
   * <p>Logically, creates multiple global windows, and the user provides a function that decides
   * which global window a value should go into.
   */
  private static class PartitionedGlobalWindows<T>
      extends NonMergingWindowFn<T, PartitionedGlobalWindow> {
    private SerializableFunction<T, String> extractPartition;

    public PartitionedGlobalWindows(SerializableFunction<T, String> extractPartition) {
      this.extractPartition = extractPartition;
    }

    @Override
    public Collection<PartitionedGlobalWindow> assignWindows(AssignContext c) {
      return Collections.singletonList(
          new PartitionedGlobalWindow(extractPartition.apply(c.element())));
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> o) {
      return o instanceof PartitionedGlobalWindows;
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "%s is only compatible with %s.",
                PartitionedGlobalWindows.class.getSimpleName(),
                PartitionedGlobalWindows.class.getSimpleName()));
      }
    }

    @Override
    public Coder<PartitionedGlobalWindow> windowCoder() {
      return new PartitionedGlobalWindowCoder();
    }

    @Override
    public WindowMappingFn<PartitionedGlobalWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException(
          "PartitionedGlobalWindows is not allowed in side inputs");
    }
  }

  /** Custom Window object that encodes a String value. */
  private static class PartitionedGlobalWindow extends BoundedWindow {
    String value;

    public PartitionedGlobalWindow(String value) {
      this.value = value;
    }

    @Override
    public Instant maxTimestamp() {
      return GlobalWindow.INSTANCE.maxTimestamp();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof PartitionedGlobalWindow) {
        return value.equals(((PartitionedGlobalWindow) other).value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  /** Coder for @link{PartitionedGlobalWindow}. */
  private static class PartitionedGlobalWindowCoder extends AtomicCoder<PartitionedGlobalWindow> {
    @Override
    public void encode(PartitionedGlobalWindow window, OutputStream outStream) throws IOException {
      encode(window, outStream, Context.NESTED);
    }

    @Override
    public void encode(PartitionedGlobalWindow window, OutputStream outStream, Context context)
        throws IOException {
      StringUtf8Coder.of().encode(window.value, outStream, context);
    }

    @Override
    public PartitionedGlobalWindow decode(InputStream inStream) throws IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public PartitionedGlobalWindow decode(InputStream inStream, Context context)
        throws IOException {
      return new PartitionedGlobalWindow(StringUtf8Coder.of().decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() {}
  }

  @Test
  public void testWriteWithDynamicTables() throws Exception {
    List<Integer> inserts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      inserts.add(i);
    }

    // Create a windowing strategy that puts the input into five different windows depending on
    // record value.
    WindowFn<Integer, PartitionedGlobalWindow> windowFn =
        new PartitionedGlobalWindows<>(i -> Integer.toString(i % 5));

    final Map<Integer, TableDestination> targetTables = Maps.newHashMap();
    Map<String, String> schemas = Maps.newHashMap();
    for (int i = 0; i < 5; i++) {
      TableDestination destination =
          new TableDestination("project-id:dataset-id" + ".table-id-" + i, "");
      targetTables.put(i, destination);
      // Make sure each target table has its own custom table.
      schemas.put(
          destination.getTableSpec(),
          toJsonString(
              new TableSchema()
                  .setFields(
                      ImmutableList.of(
                          new TableFieldSchema().setName("name").setType("STRING"),
                          new TableFieldSchema().setName("number").setType("INTEGER"),
                          new TableFieldSchema().setName("custom_" + i).setType("STRING")))));
    }

    SerializableFunction<ValueInSingleWindow<Integer>, TableDestination> tableFunction =
        input -> {
          PartitionedGlobalWindow window = (PartitionedGlobalWindow) input.getWindow();
          // Check that we can access the element as well here and that it matches the window.
          checkArgument(
              window.value.equals(Integer.toString(input.getValue() % 5)), "Incorrect element");
          return targetTables.get(input.getValue() % 5);
        };

    PCollection<Integer> input = p.apply("CreateSource", Create.of(inserts));
    if (useStreaming) {
      input = input.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    }

    PCollectionView<Map<String, String>> schemasView =
        p.apply("CreateSchemaMap", Create.of(schemas)).apply("ViewSchemaAsMap", View.asMap());

    input
        .apply(Window.into(windowFn))
        .apply(
            BigQueryIO.<Integer>write()
                .to(tableFunction)
                .withFormatFunction(
                    i ->
                        new TableRow().set("name", "number" + i).set("number", Integer.toString(i)))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchemaFromView(schemasView)
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();

    for (int i = 0; i < 5; ++i) {
      String tableId = String.format("table-id-%d", i);
      String tableSpec = String.format("project-id:dataset-id.%s", tableId);

      // Verify that table was created with the correct schema.
      assertThat(
          toJsonString(
              fakeDatasetService
                  .getTable(
                      new TableReference()
                          .setProjectId("project-id")
                          .setDatasetId("dataset-id")
                          .setTableId(tableId))
                  .getSchema()),
          equalTo(schemas.get(tableSpec)));

      // Verify that the table has the expected contents.
      assertThat(
          fakeDatasetService.getAllRows("project-id", "dataset-id", tableId),
          containsInAnyOrder(
              new TableRow()
                  .set("name", String.format("number%d", i))
                  .set("number", Integer.toString(i)),
              new TableRow()
                  .set("name", String.format("number%d", i + 5))
                  .set("number", Integer.toString(i + 5))));
    }
  }

  @Test
  public void testWriteUnknown() throws Exception {
    if (useStorageApi) {
      return;
    }
    p.apply(
            Create.of(
                    new TableRow().set("name", "a").set("number", 1),
                    new TableRow().set("name", "b").set("number", 2),
                    new TableRow().set("name", "c").set("number", 3))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withTestServices(fakeBqServices)
                .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to create job");
    p.run();
  }

  @Test
  public void testWriteFailedJobs() throws Exception {
    if (useStorageApi) {
      return;
    }
    p.apply(
            Create.of(
                    new TableRow().set("name", "a").set("number", 1),
                    new TableRow().set("name", "b").set("number", 2),
                    new TableRow().set("name", "c").set("number", 3))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withTestServices(fakeBqServices)
                .withoutValidation());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to create job with prefix");
    thrown.expectMessage("reached max retries");
    thrown.expectMessage("last failed job");

    p.run();
  }

  @Test
  public void testWriteWithMissingSchemaFromView() throws Exception {
    // Because no messages
    PCollectionView<Map<String, String>> view =
        p.apply("Create schema view", Create.of(KV.of("foo", "bar"), KV.of("bar", "boo")))
            .apply(View.asMap());
    p.apply(
            Create.of(
                    new TableRow().set("name", "a").set("number", 1),
                    new TableRow().set("name", "b").set("number", 2),
                    new TableRow().set("name", "c").set("number", 3))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("dataset-id.table-id")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchemaFromView(view)
                .withTestServices(fakeBqServices)
                .withoutValidation());

    thrown.expectMessage("does not contain data for table destination dataset-id.table-id");
    p.run();
  }

  @Test
  public void testWriteWithBrokenGetTable() throws Exception {
    p.apply(Create.<TableRow>of(new TableRow().set("foo", "bar")))
        .apply(
            BigQueryIO.writeTableRows()
                .to(input -> null)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withTestServices(fakeBqServices)
                .withoutValidation());

    thrown.expectMessage("result of tableFunction can not be null");
    thrown.expectMessage("foo");
    p.run();
  }

  @Test
  public void testWriteBuilderMethods() {
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows().to("foo.com:project:somedataset.sometable");
    assertEquals("foo.com:project", write.getTable().get().getProjectId());
    assertEquals("somedataset", write.getTable().get().getDatasetId());
    assertEquals("sometable", write.getTable().get().getTableId());
    assertNull(write.getJsonSchema());
    assertNull(write.getSchemaFromView());
    assertEquals(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED, write.getCreateDisposition());
    assertEquals(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY, write.getWriteDisposition());
    assertEquals(null, write.getTableDescription());
    assertTrue(write.getValidate());
    assertFalse(write.getAutoSharding());

    assertFalse(write.withoutValidation().getValidate());
    TableSchema schema = new TableSchema();
    assertEquals(
        schema,
        BigQueryHelpers.fromJsonString(
            write.withSchema(schema).getJsonSchema().get(), TableSchema.class));
  }

  @Test
  public void testBuildWriteDefaultProject() {
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows().to("somedataset.sometable");
    assertEquals(null, write.getTable().get().getProjectId());
    assertEquals("somedataset", write.getTable().get().getDatasetId());
    assertEquals("sometable", write.getTable().get().getTableId());
  }

  @Test
  public void testBuildWriteWithTableReference() {
    TableReference table =
        new TableReference()
            .setProjectId("foo.com:project")
            .setDatasetId("somedataset")
            .setTableId("sometable");
    BigQueryIO.Write<TableRow> write = BigQueryIO.writeTableRows().to(table);
    assertEquals("foo.com:project", write.getTable().get().getProjectId());
    assertEquals("somedataset", write.getTable().get().getDatasetId());
    assertEquals("sometable", write.getTable().get().getTableId());
  }

  @Test
  public void testBuildWriteDisplayData() {
    String tableSpec = "project:dataset.table";
    TableSchema schema = new TableSchema().set("col1", "type1").set("col2", "type2");
    final String tblDescription = "foo bar table";

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withSchema(schema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchemaUpdateOptions(
                EnumSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
            .withTableDescription(tblDescription)
            .withoutValidation();

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("table"));
    assertThat(displayData, hasDisplayItem("schema"));
    assertThat(
        displayData,
        hasDisplayItem(
            "createDisposition", BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED.toString()));
    assertThat(
        displayData,
        hasDisplayItem(
            "writeDisposition", BigQueryIO.Write.WriteDisposition.WRITE_APPEND.toString()));
    assertThat(
        displayData,
        hasDisplayItem(
            "schemaUpdateOptions",
            EnumSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION).toString()));
    assertThat(displayData, hasDisplayItem("tableDescription", tblDescription));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testWriteValidatesDataset() throws Exception {
    TableReference tableRef = new TableReference();
    tableRef.setDatasetId("somedataset");
    tableRef.setTableId("sometable");

    PCollection<TableRow> tableRows;
    if (useStreaming) {
      tableRows =
          p.apply(GenerateSequence.from(0))
              .apply(
                  MapElements.via(
                      new SimpleFunction<Long, TableRow>() {
                        @Override
                        public TableRow apply(Long input) {
                          return null;
                        }
                      }))
              .setCoder(TableRowJsonCoder.of());
    } else {
      tableRows = p.apply(Create.empty(TableRowJsonCoder.of()));
    }

    thrown.expect(RuntimeException.class);
    // Message will be one of following depending on the execution environment.
    thrown.expectMessage(
        Matchers.either(Matchers.containsString("Unable to confirm BigQuery dataset presence"))
            .or(Matchers.containsString("BigQuery dataset not found for table")));
    tableRows.apply(
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(new TableSchema())
            .withTestServices(fakeBqServices));
    p.run();
  }

  @Test
  public void testCreateNever() throws Exception {
    BigQueryIO.Write.Method method =
        useStreaming
            ? (useStorageApi
                ? (useStorageApiApproximate
                    ? Method.STORAGE_API_AT_LEAST_ONCE
                    : Method.STORAGE_WRITE_API)
                : Method.STREAMING_INSERTS)
            : useStorageApi ? Method.STORAGE_WRITE_API : Method.FILE_LOADS;
    p.enableAbandonedNodeEnforcement(false);

    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset-id.table");
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));
    fakeDatasetService.createTable(new Table().setTableReference(tableRef).setSchema(tableSchema));

    PCollection<TableRow> tableRows =
        p.apply(GenerateSequence.from(0).to(10))
            .apply(
                MapElements.via(
                    new SimpleFunction<Long, TableRow>() {
                      @Override
                      public TableRow apply(Long input) {
                        return new TableRow().set("name", "name " + input).set("number", input);
                      }
                    }))
            .setCoder(TableRowJsonCoder.of());
    tableRows.apply(
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withTestServices(fakeBqServices)
            .withoutValidation());
    p.run();
  }

  @Test
  public void testBigQueryIOGetName() {
    assertEquals(
        "BigQueryIO.Write", BigQueryIO.<TableRow>write().to("somedataset.sometable").getName());
  }

  @Test
  public void testWriteValidateFailsCreateNoSchema() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("no schema was provided");
    p.apply(Create.empty(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to("dataset.table")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsNoFormatFunction() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "A function must be provided to convert the input type into a TableRow or GenericRecord");
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset.table")
                .withSchema(new TableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsBothFormatFunctions() {
    if (useStorageApi) {
      return;
    }
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Only one of withFormatFunction or withAvroFormatFunction/withAvroWriter maybe set, not both.");
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset.table")
                .withSchema(new TableSchema())
                .withFormatFunction(r -> new TableRow())
                .withAvroFormatFunction(r -> new GenericData.Record(r.getSchema()))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsWithBeamSchemaAndAvroFormatFunction() {
    if (useStorageApi) {
      return;
    }
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("avroFormatFunction is unsupported when using Beam schemas");
    p.apply(Create.of(new SchemaPojo("a", 1)))
        .apply(
            BigQueryIO.<SchemaPojo>write()
                .to("dataset.table")
                .useBeamSchema()
                .withAvroFormatFunction(r -> new GenericData.Record(r.getSchema()))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsWithAvroFormatAndStreamingInserts() {
    if (!useStreaming && !useStorageApi) {
      return;
    }
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Writing avro formatted data is only supported for FILE_LOADS");
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset.table")
                .withSchema(new TableSchema())
                .withAvroFormatFunction(r -> new GenericData.Record(r.getSchema()))
                .withMethod(Method.STREAMING_INSERTS)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWriteValidateFailsWithBatchAutoSharding() {
    if (useStorageApi) {
      return;
    }
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Auto-sharding is only applicable to unbounded input.");
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset.table")
                .withSchema(new TableSchema())
                .withMethod(Method.STREAMING_INSERTS)
                .withAutoSharding()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testWritePartitionEmptyData() throws Exception {
    long numFiles = 0;
    long fileSize = 0;

    // An empty file is created for no input data. One partition is needed.
    long expectedNumPartitions = 1;
    testWritePartition(1, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionSinglePartition() throws Exception {
    long numFiles = BatchLoads.DEFAULT_MAX_FILES_PER_PARTITION;
    long fileSize = 1;

    // One partition is needed.
    long expectedNumPartitions = 1;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionManyFiles() throws Exception {
    long numFiles = BatchLoads.DEFAULT_MAX_FILES_PER_PARTITION * 3;
    long fileSize = 1;

    // One partition is needed for each group of BigQueryWrite.DEFAULT_MAX_FILES_PER_PARTITION
    // files.
    long expectedNumPartitions = 3;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  @Test
  public void testWritePartitionLargeFileSize() throws Exception {
    long numFiles = 10;
    long fileSize = BatchLoads.DEFAULT_MAX_BYTES_PER_PARTITION / 3;

    // One partition is needed for each group of three files.
    long expectedNumPartitions = 4;
    testWritePartition(2, numFiles, fileSize, expectedNumPartitions);
  }

  private void testWritePartition(
      long numTables, long numFilesPerTable, long fileSize, long expectedNumPartitionsPerTable)
      throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    // In the case where a static destination is specified (i.e. not through a dynamic table
    // function) and there is no input data, WritePartition will generate an empty table. This
    // code is to test that path.
    boolean isSingleton = numTables == 1 && numFilesPerTable == 0;
    DynamicDestinations<String, TableDestination> dynamicDestinations =
        new DynamicDestinationsHelpers.ConstantTableDestinations<>(
            ValueProvider.StaticValueProvider.of("SINGLETON"), "", false);
    List<ShardedKey<TableDestination>> expectedPartitions = Lists.newArrayList();
    if (isSingleton) {
      expectedPartitions.add(ShardedKey.of(new TableDestination("SINGLETON", ""), 1));
    } else {
      for (int i = 0; i < numTables; ++i) {
        for (int j = 1; j <= expectedNumPartitionsPerTable; ++j) {
          String tableName = String.format("project-id:dataset-id.tables%05d", i);
          expectedPartitions.add(ShardedKey.of(new TableDestination(tableName, ""), j));
        }
      }
    }

    List<WriteBundlesToFiles.Result<TableDestination>> files = Lists.newArrayList();
    Map<String, List<String>> filenamesPerTable = Maps.newHashMap();
    for (int i = 0; i < numTables; ++i) {
      String tableName = String.format("project-id:dataset-id.tables%05d", i);
      List<String> filenames =
          filenamesPerTable.computeIfAbsent(tableName, k -> Lists.newArrayList());
      for (int j = 0; j < numFilesPerTable; ++j) {
        String fileName = String.format("%s_files%05d", tableName, j);
        filenames.add(fileName);
        files.add(
            new WriteBundlesToFiles.Result<>(
                fileName, fileSize, new TableDestination(tableName, "")));
      }
    }

    TupleTag<KV<ShardedKey<TableDestination>, WritePartition.Result>> multiPartitionsTag =
        new TupleTag<KV<ShardedKey<TableDestination>, WritePartition.Result>>(
            "multiPartitionsTag") {};
    TupleTag<KV<ShardedKey<TableDestination>, WritePartition.Result>> singlePartitionTag =
        new TupleTag<KV<ShardedKey<TableDestination>, WritePartition.Result>>(
            "singlePartitionTag") {};

    String tempFilePrefix = testFolder.newFolder("BigQueryIOTest").getAbsolutePath();
    PCollectionView<String> tempFilePrefixView =
        p.apply(Create.of(tempFilePrefix)).apply(View.asSingleton());

    WritePartition<TableDestination> writePartition =
        new WritePartition<>(
            isSingleton,
            dynamicDestinations,
            tempFilePrefixView,
            BatchLoads.DEFAULT_MAX_FILES_PER_PARTITION,
            BatchLoads.DEFAULT_MAX_BYTES_PER_PARTITION,
            multiPartitionsTag,
            singlePartitionTag,
            RowWriterFactory.tableRows(
                SerializableFunctions.identity(), SerializableFunctions.identity()));

    DoFnTester<
            Iterable<WriteBundlesToFiles.Result<TableDestination>>,
            KV<ShardedKey<TableDestination>, WritePartition.Result>>
        tester = DoFnTester.of(writePartition);
    tester.setSideInput(tempFilePrefixView, GlobalWindow.INSTANCE, tempFilePrefix);
    tester.processElement(files);

    List<KV<ShardedKey<TableDestination>, WritePartition.Result>> partitions;
    if (expectedNumPartitionsPerTable > 1) {
      partitions = tester.takeOutputElements(multiPartitionsTag);
    } else {
      partitions = tester.takeOutputElements(singlePartitionTag);
    }

    List<ShardedKey<TableDestination>> partitionsResult = Lists.newArrayList();
    Map<String, List<String>> filesPerTableResult = Maps.newHashMap();
    for (KV<ShardedKey<TableDestination>, WritePartition.Result> partition : partitions) {
      String table = partition.getKey().getKey().getTableSpec();
      partitionsResult.add(partition.getKey());
      List<String> tableFilesResult =
          filesPerTableResult.computeIfAbsent(table, k -> Lists.newArrayList());
      tableFilesResult.addAll(partition.getValue().getFilenames());
    }

    assertThat(
        partitionsResult,
        containsInAnyOrder(Iterables.toArray(expectedPartitions, ShardedKey.class)));

    if (isSingleton) {
      assertEquals(1, filesPerTableResult.size());
      List<String> singletonFiles = filesPerTableResult.values().iterator().next();
      assertTrue(Files.exists(Paths.get(singletonFiles.get(0))));
      assertThat(Files.readAllBytes(Paths.get(singletonFiles.get(0))).length, equalTo(0));
    } else {
      assertEquals(filenamesPerTable, filesPerTableResult);
    }
    for (List<String> filenames : filesPerTableResult.values()) {
      for (String filename : filenames) {
        Files.deleteIfExists(Paths.get(filename));
      }
    }
  }

  static class IdentityDynamicTables extends DynamicDestinations<String, String> {
    @Override
    public String getDestination(ValueInSingleWindow<String> element) {
      throw new UnsupportedOperationException("getDestination not expected in this test.");
    }

    @Override
    public TableDestination getTable(String destination) {
      return new TableDestination(destination, destination);
    }

    @Override
    public TableSchema getSchema(String destination) {
      return new TableSchema();
    }
  }

  @Test
  public void testWriteTables() throws Exception {
    long numTables = 3;
    long numPartitions = 3;
    long numFilesPerPartition = 10;
    String jobIdToken = "jobId";
    final Multimap<TableDestination, String> expectedTempTables = ArrayListMultimap.create();

    List<KV<ShardedKey<String>, WritePartition.Result>> partitions = Lists.newArrayList();
    for (int i = 0; i < numTables; ++i) {
      String tableName = String.format("project-id:dataset-id.table%05d", i);
      TableDestination tableDestination = new TableDestination(tableName, tableName);
      for (int j = 0; j < numPartitions; ++j) {
        String tempTableId =
            BigQueryResourceNaming.createJobIdWithDestination(jobIdToken, tableDestination, j, 0);
        List<String> filesPerPartition = Lists.newArrayList();
        for (int k = 0; k < numFilesPerPartition; ++k) {
          String filename =
              Paths.get(
                      testFolder.getRoot().getAbsolutePath(),
                      String.format("files0x%08x_%05d", tempTableId.hashCode(), k))
                  .toString();
          TableRowWriter<TableRow> writer =
              new TableRowWriter<>(filename, SerializableFunctions.identity());
          try (TableRowWriter<TableRow> ignored = writer) {
            TableRow tableRow = new TableRow().set("name", tableName);
            writer.write(tableRow);
          }
          filesPerPartition.add(writer.getResult().resourceId.toString());
        }
        partitions.add(
            KV.of(
                ShardedKey.of(tableDestination.getTableSpec(), j),
                new AutoValue_WritePartition_Result(filesPerPartition, true)));

        String json =
            String.format(
                "{\"datasetId\":\"dataset-id\",\"projectId\":\"project-id\",\"tableId\":\"%s\"}",
                tempTableId);
        expectedTempTables.put(tableDestination, json);
      }
    }

    PCollection<KV<ShardedKey<String>, WritePartition.Result>> writeTablesInput =
        p.apply(
            Create.of(partitions)
                .withCoder(
                    KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), ResultCoder.INSTANCE)));
    PCollectionView<String> jobIdTokenView =
        p.apply("CreateJobId", Create.of("jobId")).apply(View.asSingleton());
    List<PCollectionView<?>> sideInputs = ImmutableList.of(jobIdTokenView);

    fakeJobService.setNumFailuresExpected(3);
    WriteTables<String> writeTables =
        new WriteTables<>(
            true,
            fakeBqServices,
            jobIdTokenView,
            BigQueryIO.Write.WriteDisposition.WRITE_EMPTY,
            BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
            sideInputs,
            new IdentityDynamicTables(),
            null,
            4,
            false,
            null,
            "NEWLINE_DELIMITED_JSON",
            false,
            Collections.emptySet(),
            null);

    PCollection<KV<TableDestination, WriteTables.Result>> writeTablesOutput =
        writeTablesInput
            .apply(writeTables)
            .setCoder(KvCoder.of(TableDestinationCoderV3.of(), WriteTables.ResultCoder.INSTANCE));

    PAssert.thatMultimap(writeTablesOutput)
        .satisfies(
            input -> {
              assertEquals(input.keySet(), expectedTempTables.keySet());
              for (Map.Entry<TableDestination, Iterable<WriteTables.Result>> entry :
                  input.entrySet()) {
                Iterable<String> tableNames =
                    StreamSupport.stream(entry.getValue().spliterator(), false)
                        .map(Result::getTableName)
                        .collect(Collectors.toList());
                @SuppressWarnings("unchecked")
                String[] expectedValues =
                    Iterables.toArray(expectedTempTables.get(entry.getKey()), String.class);
                assertThat(tableNames, containsInAnyOrder(expectedValues));
              }
              return null;
            });
    p.run();
  }

  @Test
  public void testRemoveTemporaryFiles() throws Exception {
    int numFiles = 10;
    List<String> fileNames = Lists.newArrayList();
    String tempFilePrefix = options.getTempLocation() + "/";
    for (int i = 0; i < numFiles; ++i) {
      TableRowWriter<TableRow> writer =
          new TableRowWriter<>(tempFilePrefix, SerializableFunctions.identity());
      writer.close();
      fileNames.add(writer.getResult().resourceId.toString());
    }
    fileNames.add(tempFilePrefix + String.format("files%05d", numFiles));

    File tempDir = new File(options.getTempLocation());
    testNumFiles(tempDir, 10);

    WriteTables.removeTemporaryFiles(fileNames);
  }

  @Test
  public void testWriteRename() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    final int numFinalTables = 3;
    final int numTempTablesPerFinalTable = 3;
    final int numRecordsPerTempTable = 10;

    Multimap<TableDestination, TableRow> expectedRowsPerTable = ArrayListMultimap.create();
    String jobIdToken = "jobIdToken";
    Multimap<TableDestination, String> tempTables = ArrayListMultimap.create();
    List<KV<TableDestination, WriteTables.Result>> tempTablesElement = Lists.newArrayList();
    for (int i = 0; i < numFinalTables; ++i) {
      String tableName = "project-id:dataset-id.table_" + i;
      TableDestination tableDestination = new TableDestination(tableName, "table_" + i + "_desc");
      for (int j = 0; j < numTempTablesPerFinalTable; ++j) {
        TableReference tempTable =
            new TableReference()
                .setProjectId("project-id")
                .setDatasetId("dataset-id")
                .setTableId(String.format("%s_%05d_%05d", jobIdToken, i, j));
        fakeDatasetService.createTable(new Table().setTableReference(tempTable));

        List<TableRow> rows = Lists.newArrayList();
        for (int k = 0; k < numRecordsPerTempTable; ++k) {
          rows.add(new TableRow().set("number", j * numTempTablesPerFinalTable + k));
        }
        fakeDatasetService.insertAll(tempTable, rows, null);
        expectedRowsPerTable.putAll(tableDestination, rows);
        String tableJson = toJsonString(tempTable);
        tempTables.put(tableDestination, tableJson);
        tempTablesElement.add(
            KV.of(tableDestination, new AutoValue_WriteTables_Result(tableJson, true)));
      }
    }

    PCollectionView<String> jobIdTokenView =
        p.apply("CreateJobId", Create.of("jobId")).apply(View.asSingleton());

    WriteRename writeRename =
        new WriteRename(
            fakeBqServices,
            jobIdTokenView,
            BigQueryIO.Write.WriteDisposition.WRITE_EMPTY,
            BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
            3,
            "kms_key",
            null);

    DoFnTester<Iterable<KV<TableDestination, WriteTables.Result>>, TableDestination> tester =
        DoFnTester.of(writeRename);
    tester.setSideInput(jobIdTokenView, GlobalWindow.INSTANCE, jobIdToken);
    tester.processElement(tempTablesElement);
    tester.finishBundle();

    for (Map.Entry<TableDestination, Collection<String>> entry : tempTables.asMap().entrySet()) {
      TableDestination tableDestination = entry.getKey();
      TableReference tableReference = tableDestination.getTableReference();
      Table table = checkNotNull(fakeDatasetService.getTable(tableReference));
      assertEquals(tableReference.getTableId() + "_desc", tableDestination.getTableDescription());
      assertEquals("kms_key", table.getEncryptionConfiguration().getKmsKeyName());

      Collection<TableRow> expectedRows = expectedRowsPerTable.get(tableDestination);
      assertThat(
          fakeDatasetService.getAllRows(
              tableReference.getProjectId(),
              tableReference.getDatasetId(),
              tableReference.getTableId()),
          containsInAnyOrder(Iterables.toArray(expectedRows, TableRow.class)));

      // Temp tables should be deleted.
      for (String tempTableJson : entry.getValue()) {
        TableReference tempTable =
            BigQueryHelpers.fromJsonString(tempTableJson, TableReference.class);
        assertEquals(null, fakeDatasetService.getTable(tempTable));
      }
    }
  }

  @Test
  public void testRemoveTemporaryTables() throws Exception {
    FakeDatasetService datasetService = new FakeDatasetService();
    String projectId = "project";
    String datasetId = "dataset";
    datasetService.createDataset(projectId, datasetId, "", "", null);
    List<TableReference> tableRefs =
        Lists.newArrayList(
            BigQueryHelpers.parseTableSpec(
                String.format("%s:%s.%s", projectId, datasetId, "table1")),
            BigQueryHelpers.parseTableSpec(
                String.format("%s:%s.%s", projectId, datasetId, "table2")),
            BigQueryHelpers.parseTableSpec(
                String.format("%s:%s.%s", projectId, datasetId, "table3")));
    for (TableReference tableRef : tableRefs) {
      datasetService.createTable(new Table().setTableReference(tableRef));
    }

    // Add one more table to delete that does not actually exist.
    tableRefs.add(
        BigQueryHelpers.parseTableSpec(String.format("%s:%s.%s", projectId, datasetId, "table4")));

    WriteRename.removeTemporaryTables(datasetService, tableRefs);

    for (TableReference ref : tableRefs) {
      loggedWriteRename.verifyDebug("Deleting table " + toJsonString(ref));
      checkState(datasetService.getTable(ref) == null, "Table " + ref + " was not deleted!");
    }
  }

  @Test
  public void testRuntimeOptionsNotCalledInApplyOutput() {
    p.enableAbandonedNodeEnforcement(false);

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(p.newProvider("some-table"))
            .withSchema(
                ValueProvider.NestedValueProvider.of(
                    p.newProvider("some-schema"), new BigQueryHelpers.JsonSchemaToTableSchema()))
            .withoutValidation();
    p.apply(Create.empty(TableRowJsonCoder.of())).apply(write);
    // Test that this doesn't throw.
    DisplayData.from(write);
  }

  private static void testNumFiles(File tempDir, int expectedNumFiles) {
    assertEquals(expectedNumFiles, tempDir.listFiles(File::isFile).length);
  }

  @Test
  public void testWriteToTableDecorator() throws Exception {
    TableRow row1 = new TableRow().set("name", "a").set("number", "1");
    TableRow row2 = new TableRow().set("name", "b").set("number", "2");

    // withMethod overrides the pipeline option, so we need to explicitly requiest
    // STORAGE_API_WRITES.
    BigQueryIO.Write.Method method =
        useStorageApi
            ? (useStorageApiApproximate
                ? Method.STORAGE_API_AT_LEAST_ONCE
                : Method.STORAGE_WRITE_API)
            : Method.STREAMING_INSERTS;
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));
    p.apply(Create.of(row1, row2))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id$20171127")
                .withTestServices(fakeBqServices)
                .withMethod(method)
                .withSchema(schema)
                .withoutValidation());
    p.run();
  }

  @Test
  public void testExtendedErrorRetrieval() throws Exception {
    if (useStorageApi) {
      return;
    }
    TableRow row1 = new TableRow().set("name", "a").set("number", "1");
    TableRow row2 = new TableRow().set("name", "b").set("number", "2");
    TableRow row3 = new TableRow().set("name", "c").set("number", "3");
    String tableSpec = "project-id:dataset-id.table-id";

    TableDataInsertAllResponse.InsertErrors ephemeralError =
        new TableDataInsertAllResponse.InsertErrors()
            .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout")));
    TableDataInsertAllResponse.InsertErrors persistentError =
        new TableDataInsertAllResponse.InsertErrors()
            .setErrors(Lists.newArrayList(new ErrorProto().setReason("invalidQuery")));

    fakeDatasetService.failOnInsert(
        ImmutableMap.of(
            row1, ImmutableList.of(ephemeralError, ephemeralError),
            row2, ImmutableList.of(ephemeralError, ephemeralError, persistentError)));

    PCollection<BigQueryInsertError> failedRows =
        p.apply(Create.of(row1, row2, row3))
            .apply(
                BigQueryIO.writeTableRows()
                    .to(tableSpec)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("name").setType("STRING"),
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withTestServices(fakeBqServices)
                    .withoutValidation()
                    .withExtendedErrorInfo())
            .getFailedInsertsWithErr();

    // row2 finally fails with a non-retryable error, so we expect to see it in the collection of
    // failed rows.
    PAssert.that(failedRows)
        .containsInAnyOrder(
            new BigQueryInsertError(
                row2, persistentError, BigQueryHelpers.parseTableSpec(tableSpec)));
    p.run();

    // Only row1 and row3 were successfully inserted.
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(row1, row3));
  }

  @Test
  public void testWrongErrorConfigs() {
    if (useStorageApi) {
      return;
    }
    p.enableAutoRunIfMissing(true);
    TableRow row1 = new TableRow().set("name", "a").set("number", "1");

    BigQueryIO.Write<TableRow> bqIoWrite =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("name").setType("STRING"),
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .withTestServices(fakeBqServices)
            .withoutValidation();

    try {
      p.apply("Create1", Create.<TableRow>of(row1))
          .apply("Write 1", bqIoWrite)
          .getFailedInsertsWithErr();
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(
          e.getMessage(),
          is(
              "Cannot use getFailedInsertsWithErr as this WriteResult "
                  + "does not use extended errors. Use getFailedInserts instead"));
    }

    try {
      p.apply("Create2", Create.<TableRow>of(row1))
          .apply("Write2", bqIoWrite.withExtendedErrorInfo())
          .getFailedInserts();
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(
          e.getMessage(),
          is(
              "Cannot use getFailedInserts as this WriteResult "
                  + "uses extended errors information. Use getFailedInsertsWithErr instead"));
    }
  }

  void schemaUpdateOptionsTest(
      BigQueryIO.Write.Method insertMethod, Set<SchemaUpdateOption> schemaUpdateOptions)
      throws Exception {
    TableRow row = new TableRow().set("date", "2019-01-01").set("number", "1");

    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema()
                        .setName("date")
                        .setType("DATE")
                        .setName("number")
                        .setType("INTEGER")));

    Write<TableRow> writeTransform =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withTestServices(fakeBqServices)
            .withMethod(insertMethod)
            .withSchema(schema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchemaUpdateOptions(schemaUpdateOptions);

    p.apply("Create" + insertMethod, Create.<TableRow>of(row))
        .apply("Write" + insertMethod, writeTransform);
    p.run();

    List<String> expectedOptions =
        schemaUpdateOptions.stream().map(SchemaUpdateOption::name).collect(Collectors.toList());

    for (Job job : fakeJobService.getAllJobs()) {
      JobConfigurationLoad configuration = job.getConfiguration().getLoad();
      assertEquals(expectedOptions, configuration.getSchemaUpdateOptions());
    }
  }

  @Test
  public void testWriteFileSchemaUpdateOptionAllowFieldAddition() throws Exception {
    Set<SchemaUpdateOption> options = EnumSet.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    schemaUpdateOptionsTest(BigQueryIO.Write.Method.FILE_LOADS, options);
  }

  @Test
  public void testWriteFileSchemaUpdateOptionAllowFieldRelaxation() throws Exception {
    Set<SchemaUpdateOption> options = EnumSet.of(SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    schemaUpdateOptionsTest(BigQueryIO.Write.Method.FILE_LOADS, options);
  }

  @Test
  public void testWriteFileSchemaUpdateOptionAll() throws Exception {
    Set<SchemaUpdateOption> options = EnumSet.allOf(SchemaUpdateOption.class);
    schemaUpdateOptionsTest(BigQueryIO.Write.Method.FILE_LOADS, options);
  }

  @Test
  public void testSchemaUpdateOptionsFailsStreamingInserts() throws Exception {
    if (!useStreaming && !useStorageApi) {
      return;
    }
    Set<SchemaUpdateOption> options = EnumSet.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    p.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("SchemaUpdateOptions are not supported when method == STREAMING_INSERTS");
    schemaUpdateOptionsTest(BigQueryIO.Write.Method.STREAMING_INSERTS, options);
  }
}
