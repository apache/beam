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
import static org.apache.beam.sdk.io.gcp.bigquery.WriteTables.ResultCoder.INSTANCE;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryFileLoadsSchemaTransformProvider.BigQueryFileLoadsSchemaTransform;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.bigquery.WritePartition.ResultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteRename.TempTableCleanupFn;
import org.apache.beam.sdk.io.gcp.bigquery.WriteTables.Result;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryFileLoadsSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.EchoErrorTransform;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;

/** Tests for {@link BigQueryIO#write}. */
@RunWith(Parameterized.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
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

  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

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

  @Rule
  public transient ExpectedLogs loggedWriteRename = ExpectedLogs.none(TempTableCleanupFn.class);

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  private void checkLineageSinkMetric(PipelineResult pipelineResult, String tableName) {
    assertThat(
        Lineage.query(pipelineResult.metrics(), Lineage.Type.SINK),
        hasItem("bigquery:" + tableName.replace(':', '.')));
  }

  @Before
  public void setUp() throws ExecutionException, IOException, InterruptedException {
    FakeDatasetService.setUp();
    BigQueryIO.clearStaticCaches();
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
    assumeTrue(!useStreaming);
    assumeTrue(!useStorageApi);
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
  public void testWriteDynamicDestinationsStreamingWithAutoSharding() throws Exception {
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApiApproximate); // STORAGE_API_AT_LEAST_ONCE ignores auto-sharding
    writeDynamicDestinations(true, true);
  }

  @Test
  public void testWriteDynamicDestinationsWithBeamSchemas() throws Exception {
    writeDynamicDestinations(true, false);
  }

  public void writeDynamicDestinations(boolean schemas, boolean autoSharding) throws Exception {
    final Schema schema =
        Schema.builder().addField("name", FieldType.STRING).addField("id", FieldType.INT64).build();

    final Pattern userPattern = Pattern.compile("([a-z]+)([0-9]+)");

    // Explicitly set maxNumWritersPerBundle and the parallelism for the runner to make sure
    // bundle size is predictable and maxNumWritersPerBundle can be reached.
    final int maxNumWritersPerBundle = 5;
    p.getOptions().as(DirectOptions.class).setTargetParallelism(3);

    final PCollectionView<List<String>> sideInput1 =
        p.apply("Create SideInput 1", Create.of("a", "b", "c").withCoder(StringUtf8Coder.of()))
            .apply("asList", View.asList());
    final PCollectionView<Map<String, String>> sideInput2 =
        p.apply("Create SideInput2", Create.of(KV.of("a", "a"), KV.of("b", "b"), KV.of("c", "c")))
            .apply("AsMap", View.asMap());

    final List<String> allUsernames = ImmutableList.of("bill", "bob", "randolph");
    List<String> userList = Lists.newArrayList();
    // i controls the number of destinations
    for (int i = 0; i < maxNumWritersPerBundle * 2; ++i) {
      // Make sure that we generate enough users so that WriteBundlesToFiles is forced to spill to
      // WriteGroupedRecordsToFiles.
      for (int j = 0; j < 40; ++j) {
        String nickname =
            allUsernames.get(ThreadLocalRandom.current().nextInt(allUsernames.size()));
        userList.add(nickname + i);
      }
    }
    // shuffle the input so that each bundle gets evenly distributed destinations
    Collections.shuffle(userList);

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
                    .addValue(Long.valueOf(matcher.group(2)))
                    .build();
              },
              r -> r.getString(0) + r.getInt64(1));
    }

    // Use a partition decorator to verify that partition decorators are supported.
    final String partitionDecorator = "20171127";

    BigQueryIO.Write<String> write =
        BigQueryIO.<String>write()
            .withTestServices(fakeBqServices)
            .withMaxFilesPerBundle(maxNumWritersPerBundle)
            // a file can contain a couple of records in this size limit
            .withMaxFileSize(30)
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
                        "project-id:dataset-id.userid-" + userId + "$" + partitionDecorator,
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

    PipelineResult pipelineResult = p.run();

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
      checkLineageSinkMetric(pipelineResult, "project-id.dataset-id.userid-" + entry.getKey());
    }
  }

  void testTimePartitioningAndClustering(
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

  void testTimePartitioningAndClusteringWithAllMethods(
      Boolean enablePartitioning, Boolean enableClustering) throws Exception {
    BigQueryIO.Write.Method method;
    if (useStorageApi) {
      method =
          useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;
    } else if (useStreaming) {
      method = Method.STREAMING_INSERTS;
    } else {
      method = Method.FILE_LOADS;
    }
    testTimePartitioningAndClustering(method, enablePartitioning, enableClustering);
  }

  @Test
  public void testTimePartitioningWithoutClustering() throws Exception {
    testTimePartitioningAndClusteringWithAllMethods(true, false);
  }

  @Test
  public void testTimePartitioningWithClustering() throws Exception {
    testTimePartitioningAndClusteringWithAllMethods(true, true);
  }

  @Test
  public void testClusteringWithoutPartitioning() throws Exception {
    testTimePartitioningAndClusteringWithAllMethods(false, true);
  }

  @Test
  public void testNoClusteringNoPartitioning() throws Exception {
    testTimePartitioningAndClusteringWithAllMethods(false, false);
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

  public void runStreamingFileLoads(String tableRef, boolean useTempTables, boolean useTempDataset)
      throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);
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

    BigQueryIO.Write<TableRow> writeTransform =
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withTestServices(fakeBqServices)
            .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
            .withTriggeringFrequency(Duration.standardSeconds(30))
            .withNumFileShards(2)
            .withMethod(Method.FILE_LOADS)
            .withoutValidation();

    if (useTempTables) {
      writeTransform = writeTransform.withMaxBytesPerPartition(1).withMaxFilesPerPartition(1);
    }
    if (useTempDataset) {
      writeTransform = writeTransform.withWriteTempDataset("temp-dataset-id");
    }

    p.apply(testStream).apply(writeTransform);
    PipelineResult pipelineResult = p.run();

    final int projectIdSplitter = tableRef.indexOf(':');
    final String projectId =
        projectIdSplitter == -1 ? "project-id" : tableRef.substring(0, projectIdSplitter);

    assertThat(
        fakeDatasetService.getAllRows(projectId, "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));

    checkLineageSinkMetric(
        pipelineResult, tableRef.contains(projectId) ? tableRef : projectId + ":" + tableRef);
  }

  public void runStreamingFileLoads(String tableRef) throws Exception {
    runStreamingFileLoads(tableRef, true, false);
  }

  @Test
  public void testStreamingFileLoads() throws Exception {
    runStreamingFileLoads("project-id:dataset-id.table-id", false, false);
  }

  @Test
  public void testStreamingFileLoadsWithTempTables() throws Exception {
    runStreamingFileLoads("project-id:dataset-id.table-id");
  }

  @Test
  public void testStreamingFileLoadsWithTempTablesDefaultProject() throws Exception {
    runStreamingFileLoads("dataset-id.table-id");
  }

  @Test
  @ProjectOverride
  public void testStreamingFileLoadsWithTempTablesBigQueryProject() throws Exception {
    runStreamingFileLoads("bigquery-project-id:dataset-id.table-id");
  }

  @Test
  public void testStreamingFileLoadsWithTempTablesAndDataset() throws Exception {
    runStreamingFileLoads("bigquery-project-id:dataset-id.table-id", true, true);
  }

  @Test
  public void testStreamingFileLoadsWithTempTablesToExistingNullSchemaTable() throws Exception {
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeDatasetService.createTable(new Table().setTableReference(ref).setSchema(null));
    runStreamingFileLoads("project-id:dataset-id.table-id");
  }

  @Test
  public void testStreamingFileLoadsWithAutoSharding() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);
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
  public void testFileLoadSchemaTransformUsesAvroFormat() {
    // ensure we are writing with the more performant avro format
    assumeTrue(!useStreaming);
    assumeTrue(!useStorageApi);
    BigQueryFileLoadsSchemaTransformProvider provider =
        new BigQueryFileLoadsSchemaTransformProvider();
    Row configuration =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("table", "some-table")
            .build();
    BigQueryFileLoadsSchemaTransform schemaTransform =
        (BigQueryFileLoadsSchemaTransform) provider.from(configuration);
    BigQueryIO.Write<Row> write =
        schemaTransform.toWrite(Schema.of(), PipelineOptionsFactory.create());
    assertNull(write.getFormatFunction());
    assertNotNull(write.getAvroRowWriterFactory());
  }

  @Test
  public void testBatchFileLoads() throws Exception {
    assumeTrue(!useStreaming);
    assumeTrue(!useStorageApi);
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }

    WriteResult result =
        p.apply(Create.of(elements).withCoder(TableRowJsonCoder.of()))
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
    PipelineResult pipelineResult = p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
    checkLineageSinkMetric(pipelineResult, "project-id.dataset-id.table-id");
  }

  @Test
  public void testBatchFileLoadsWithTempTables() throws Exception {
    // Test only non-streaming inserts.
    assumeTrue(!useStorageApi);
    assumeTrue(!useStreaming);
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", i));
    }
    WriteResult result =
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

    PAssert.that(result.getSuccessfulTableLoads())
        .containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));
    PipelineResult pipelineResult = p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
    checkLineageSinkMetric(pipelineResult, "project-id.dataset-id.table-id");
  }

  @Test
  public void testBatchFileLoadsWithTempTablesCreateNever() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(!useStreaming);

    // Create table and give it a schema
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("str").setType("STRING"),
                    new TableFieldSchema().setName("num").setType("INTEGER")));
    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(schema);
    fakeTable.setTableReference(ref);
    fakeDatasetService.createTable(fakeTable);

    List<TableRow> elements = Lists.newArrayList();
    for (int i = 1; i < 10; i++) {
      elements.add(new TableRow().set("str", "a").set("num", i));
    }

    // Write to table with CREATE_NEVER and with no schema
    p.apply(Create.of(elements))
        .apply(
            BigQueryIO.writeTableRows()
                .to("project-id:dataset-id.table-id")
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withTestServices(fakeBqServices)
                .withMaxBytesPerPartition(1)
                .withMaxFilesPerPartition(1)
                .withoutValidation());
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  private static final SerializableFunction<Integer, TableRow> failingIntegerToTableRow =
      new SerializableFunction<Integer, TableRow>() {
        @Override
        public TableRow apply(Integer input) {
          if (input == 15) {
            throw new RuntimeException("Expected Exception");
          }
          return new TableRow().set("number", input);
        }
      };

  @Test
  public void testBatchLoadsWithTableRowErrorHandling() throws Exception {
    assumeTrue(!useStreaming);
    assumeTrue(!useStorageApi);
    List<Integer> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(i);
    }

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());

    WriteResult result =
        p.apply(Create.of(elements).withCoder(BigEndianIntegerCoder.of()))
            .apply(
                BigQueryIO.<Integer>write()
                    .to("dataset-id.table-id")
                    .withFormatFunction(failingIntegerToTableRow)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("name").setType("STRING"),
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withTestServices(fakeBqServices)
                    .withErrorHandler(errorHandler)
                    .withoutValidation());

    errorHandler.close();

    PAssert.that(result.getSuccessfulTableLoads())
        .containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));
    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(1L);
    p.run();

    elements.remove(15);
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id").stream()
            .map(tr -> ((Integer) tr.get("number")))
            .collect(Collectors.toList()),
        containsInAnyOrder(Iterables.toArray(elements, Integer.class)));
  }

  private static final org.apache.avro.Schema avroSchema =
      org.apache.avro.Schema.createRecord(
          ImmutableList.of(
              new Field(
                  "number",
                  org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                  "nodoc",
                  0)));
  private static final SerializableFunction<AvroWriteRequest<Long>, GenericRecord>
      failingLongToAvro =
          new SerializableFunction<AvroWriteRequest<Long>, GenericRecord>() {
            @Override
            public GenericRecord apply(AvroWriteRequest<Long> input) {
              if (input.getElement() == 15) {
                throw new RuntimeException("Expected Exception");
              }
              return new GenericRecordBuilder(avroSchema).set("number", input.getElement()).build();
            }
          };

  @Test
  public void testBatchLoadsWithAvroErrorHandling() throws Exception {
    assumeTrue(!useStreaming);
    assumeTrue(!useStorageApi);
    List<Long> elements = Lists.newArrayList();
    for (long i = 0L; i < 30L; ++i) {
      elements.add(i);
    }

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());

    WriteResult result =
        p.apply(Create.of(elements).withCoder(VarLongCoder.of()))
            .apply(
                BigQueryIO.<Long>write()
                    .to("dataset-id.table-id")
                    .withAvroFormatFunction(failingLongToAvro)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName("number").setType("INTEGER"))))
                    .withTestServices(fakeBqServices)
                    .withErrorHandler(errorHandler)
                    .withoutValidation());

    errorHandler.close();

    PAssert.that(result.getSuccessfulTableLoads())
        .containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));
    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(1L);
    p.run();

    elements.remove(15);
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id").stream()
            .map(tr -> Long.valueOf((String) tr.get("number")))
            .collect(Collectors.toList()),
        containsInAnyOrder(Iterables.toArray(elements, Long.class)));
  }

  @Test
  public void testStreamingInsertsFailuresNoRetryPolicy() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);
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
  public void testStreamingInsertsRetryPolicy() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);
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
                .to("project-id:dataset-id.table-id")
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
  public void testWriteWithoutInsertId() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);
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

  public void runTestWriteAvro(boolean schemaFromView) throws Exception {
    String tableName = "project-id:dataset-id.table-id";
    BigQueryIO.Write<InputRecord> bqWrite =
        BigQueryIO.<InputRecord>write()
            .to(tableName)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withTestServices(fakeBqServices)
            .withAvroFormatFunction(
                r -> {
                  GenericRecord rec = new GenericData.Record(r.getSchema());
                  InputRecord i = r.getElement();
                  rec.put("strval", i.strVal());
                  rec.put("longval", i.longVal());
                  rec.put("doubleval", i.doubleVal());
                  rec.put("instantval", i.instantVal().getMillis() * 1000);
                  return rec;
                })
            .withoutValidation();
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("strval").setType("STRING"),
                    new TableFieldSchema().setName("longval").setType("INTEGER"),
                    new TableFieldSchema().setName("doubleval").setType("FLOAT"),
                    new TableFieldSchema().setName("instantval").setType("TIMESTAMP")));
    if (schemaFromView) {
      bqWrite =
          bqWrite.withSchemaFromView(
              p.apply(
                      "CreateTableSchemaString",
                      Create.of(KV.of(tableName, BigQueryHelpers.toJsonString(tableSchema))))
                  .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                  .apply(View.<String, String>asMap()));
    } else {
      bqWrite = bqWrite.withSchema(tableSchema);
    }

    p.apply(
            Create.of(
                    InputRecord.create("test", 1, 1.0, Instant.parse("2019-01-01T00:00:00Z")),
                    InputRecord.create("test2", 2, 2.0, Instant.parse("2019-02-01T00:00:00Z")))
                .withCoder(INPUT_RECORD_CODER))
        .apply(bqWrite);

    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(
            new TableRow()
                .set("strval", "test")
                .set("longval", "1")
                .set("doubleval", 1.0)
                .set(
                    "instantval",
                    useStorageApi || useStorageApiApproximate
                        ? String.valueOf(Instant.parse("2019-01-01T00:00:00Z").getMillis() * 1000)
                        : "2019-01-01 00:00:00 UTC"),
            new TableRow()
                .set("strval", "test2")
                .set("longval", "2")
                .set("doubleval", 2.0)
                .set(
                    "instantval",
                    useStorageApi || useStorageApiApproximate
                        ? String.valueOf(Instant.parse("2019-02-01T00:00:00Z").getMillis() * 1000)
                        : "2019-02-01 00:00:00 UTC")));
  }

  @Test
  public void testWriteAvro() throws Exception {
    // only streaming inserts don't support avro types
    assumeTrue(!useStreaming);

    runTestWriteAvro(false);
  }

  @Test
  public void testWriteAvroWithSchemaFromView() throws Exception {
    // only streaming inserts don't support avro types
    assumeTrue(useStorageApi);

    runTestWriteAvro(true);
  }

  @Test
  public void testWriteAvroWithCustomWriter() throws Exception {
    assumeTrue(!useStorageApi);
    assumeTrue(!useStreaming);
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
                .set("doubleVal", 1.0)
                .set("instantVal", "2019-01-01 00:00:00 UTC"),
            new TableRow()
                .set("strVal", "test2_custom")
                .set("longVal", "2")
                .set("doubleVal", 2.0)
                .set("instantVal", "2019-02-01 00:00:00 UTC")));
  }

  @Test
  public void testStreamingWrite() throws Exception {
    streamingWrite(false);
  }

  @Test
  public void testStreamingWriteWithAutoSharding() throws Exception {
    streamingWrite(true);
  }

  private void streamingWrite(boolean autoSharding) throws Exception {
    assumeTrue(useStreaming);
    List<TableRow> elements =
        ImmutableList.of(
            new TableRow().set("name", "a").set("number", "1"),
            new TableRow().set("name", "b").set("number", "2"),
            new TableRow().set("name", "c").set("number", "3"),
            new TableRow().set("name", "d").set("number", "4"));
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
    p.apply(Create.of(elements).withCoder(TableRowJsonCoder.of()))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply("WriteToBQ", write);
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  private void storageWrite(boolean autoSharding) throws Exception {
    assumeTrue(useStorageApi);
    if (autoSharding) {
      assumeTrue(!useStorageApiApproximate);
      assumeTrue(useStreaming);
    }
    List<TableRow> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(new TableRow().set("number", String.valueOf(i)));
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

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withTestServices(fakeBqServices)
            .withoutValidation();

    if (useStreaming) {
      if (!useStorageApiApproximate) {
        write =
            write
                .withTriggeringFrequency(Duration.standardSeconds(30))
                .withNumStorageWriteApiStreams(2);
      }
      if (autoSharding) {
        write = write.withAutoSharding();
      }
    }

    PTransform<PBegin, PCollection<TableRow>> source =
        useStreaming ? testStream : Create.of(elements).withCoder(TableRowJsonCoder.of());

    p.apply(source).apply("WriteToBQ", write);
    p.run().waitUntilFinish();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  @Test
  public void testBatchStorageApiWrite() throws Exception {
    assumeTrue(!useStreaming);
    storageWrite(false);
  }

  @Test
  public void testStreamingStorageApiWrite() throws Exception {
    assumeTrue(useStreaming);
    storageWrite(false);
  }

  @Test
  public void testStreamingStorageApiWriteWithAutoSharding() throws Exception {
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApiApproximate);
    storageWrite(true);
  }

  // There are two failure scenarios in storage write.
  // first is in conversion, which is triggered by using a bad format function
  // second is in actually sending to BQ, which is triggered by telling te dataset service
  // to fail a row
  private void storageWriteWithErrorHandling(boolean autoSharding) throws Exception {
    assumeTrue(useStorageApi);
    if (autoSharding) {
      assumeTrue(!useStorageApiApproximate);
      assumeTrue(useStreaming);
    }
    List<Integer> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add(i);
    }

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable)
            tr ->
                tr.containsKey("number")
                    && (tr.get("number").equals("27") || tr.get("number").equals("3"));
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    TestStream<Integer> testStream =
        TestStream.create(BigEndianIntegerCoder.of())
            .addElements(elements.get(0), Iterables.toArray(elements.subList(1, 10), Integer.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), Integer.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), Integer.class))
            .advanceWatermarkToInfinity();

    ErrorHandler<BadRecord, PCollection<BadRecord>> errorHandler =
        p.registerBadRecordErrorHandler(new EchoErrorTransform());

    BigQueryIO.Write<Integer> write =
        BigQueryIO.<Integer>write()
            .to("project-id:dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withFormatFunction(failingIntegerToTableRow)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema().setName("number").setType("INTEGER"))))
            .withTestServices(fakeBqServices)
            .withErrorHandler(errorHandler)
            .withoutValidation();

    if (useStreaming) {
      if (!useStorageApiApproximate) {
        write =
            write
                .withTriggeringFrequency(Duration.standardSeconds(30))
                .withNumStorageWriteApiStreams(2);
      }
      if (autoSharding) {
        write = write.withAutoSharding();
      }
    }

    PTransform<PBegin, PCollection<Integer>> source =
        useStreaming ? testStream : Create.of(elements).withCoder(BigEndianIntegerCoder.of());

    p.apply(source).apply("WriteToBQ", write);

    errorHandler.close();

    PAssert.that(errorHandler.getOutput())
        .satisfies(
            badRecords -> {
              int count = 0;
              Iterator<BadRecord> iterator = badRecords.iterator();
              while (iterator.hasNext()) {
                count++;
                iterator.next();
              }
              Assert.assertEquals("Wrong number of bad records", 3, count);
              return null;
            });

    p.run().waitUntilFinish();

    // remove the "bad" elements from the expected elements written
    elements.remove(27);
    elements.remove(15);
    elements.remove(3);
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id").stream()
            .map(tr -> Integer.valueOf((String) tr.get("number")))
            .collect(Collectors.toList()),
        containsInAnyOrder(Iterables.toArray(elements, Integer.class)));
  }

  @Test
  public void testBatchStorageApiWriteWithErrorHandling() throws Exception {
    assumeTrue(!useStreaming);
    storageWriteWithErrorHandling(false);
  }

  @Test
  public void testStreamingStorageApiWriteWithErrorHandling() throws Exception {
    assumeTrue(useStreaming);
    storageWriteWithErrorHandling(false);
  }

  @Test
  public void testStreamingStorageApiWriteWithAutoShardingWithErrorHandling() throws Exception {
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApiApproximate);
    storageWriteWithErrorHandling(true);
  }

  private void storageWriteWithSuccessHandling(boolean columnSubset) throws Exception {
    assumeTrue(useStorageApi);
    if (!useStreaming) {
      assumeFalse(useStorageApiApproximate);
    }
    List<TableRow> elements =
        IntStream.range(0, 30)
            .mapToObj(Integer::toString)
            .map(
                i ->
                    new TableRow()
                        .set("number", i)
                        .set("string", i)
                        .set("nested", new TableRow().set("number", i)))
            .collect(Collectors.toList());

    List<TableRow> expectedSuccessElements = elements;
    if (columnSubset) {
      expectedSuccessElements =
          elements.stream()
              .map(
                  tr ->
                      new TableRow()
                          .set("number", tr.get("number"))
                          .set("nested", new TableRow().set("number", tr.get("number"))))
              .collect(Collectors.toList());
    }

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("string").setType("STRING"),
                    new TableFieldSchema()
                        .setName("nested")
                        .setType("RECORD")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("number").setType("INTEGER")))));

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

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("project-id:dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(tableSchema)
            .withMethod(Method.STORAGE_WRITE_API)
            .withTestServices(fakeBqServices)
            .withPropagateSuccessfulStorageApiWrites(true)
            .withoutValidation();
    if (columnSubset) {
      write =
          write.withPropagateSuccessfulStorageApiWrites(
              (Serializable & Predicate<String>)
                  s -> s.equals("number") || s.equals("nested") || s.equals("nested.number"));
    }
    if (useStreaming) {
      if (useStorageApiApproximate) {
        write = write.withMethod(Method.STORAGE_API_AT_LEAST_ONCE);
      } else {
        write = write.withAutoSharding();
      }
    }

    PTransform<PBegin, PCollection<TableRow>> source =
        useStreaming ? testStream : Create.of(elements).withCoder(TableRowJsonCoder.of());
    PCollection<TableRow> success =
        p.apply(source).apply("WriteToBQ", write).getSuccessfulStorageApiInserts();

    PAssert.that(success)
        .containsInAnyOrder(Iterables.toArray(expectedSuccessElements, TableRow.class));

    p.run().waitUntilFinish();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(elements, TableRow.class)));
  }

  @Test
  public void testStorageApiWriteWithSuccessfulRows() throws Exception {
    storageWriteWithSuccessHandling(false);
  }

  @Test
  public void testStorageApiWriteWithSuccessfulRowsColumnSubset() throws Exception {
    storageWriteWithSuccessHandling(true);
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
  public void testBatchSchemaWriteLoads() throws Exception {
    // This test is actually for batch!
    // We test on the useStreaming parameter however because we need it as
    // true to test STORAGE_API_AT_LEAST_ONCE
    assumeTrue(useStreaming);
    p.getOptions().as(BigQueryOptions.class).setStorageWriteApiTriggeringFrequencySec(null);
    // withMethod overrides the pipeline option, so we need to explicitly request
    // STORAGE_WRITE_API.
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
    assumeTrue(!useStorageApi);
    assumeTrue(useStreaming);

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
            (SerializableFunction<Iterable<TableRow>, Void>)
                input -> {
                  assertThat(Lists.newArrayList(input).size(), is(4));
                  return null;
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
    assumeTrue(!useStorageApi);
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
    assumeTrue(!useStorageApi);
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
            .withNumStorageWriteApiStreams(2)
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
                        return new TableRow().set("NaMe", "name " + input).set("numBEr", input);
                      }
                    }))
            .setCoder(TableRowJsonCoder.of());
    tableRows.apply(
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withNumStorageWriteApiStreams(2)
            .withTestServices(fakeBqServices)
            .withoutValidation());
    p.run();
  }

  @Test
  public void testUpdateTableSchemaUseSet() throws Exception {
    updateTableSchemaTest(true);
  }

  @Test
  public void testUpdateTableSchemaUseSetF() throws Exception {
    updateTableSchemaTest(false);
  }

  @Test
  public void testUpdateTableSchemaNoUnknownValues() throws Exception {
    assumeTrue(useStreaming);
    assumeTrue(useStorageApi);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Auto schema update currently only supported when ignoreUnknownValues also set.");
    p.apply("create", Create.empty(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(BigQueryHelpers.parseTableSpec("project-id:dataset-id.table"))
                .withMethod(Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withAutoSchemaUpdate(true)
                .withTestServices(fakeBqServices)
                .withoutValidation());
    p.run();
  }

  @SuppressWarnings({"unused"})
  static class UpdateTableSchemaDoFn extends DoFn<KV<String, TableRow>, TableRow> {
    @TimerId("updateTimer")
    private final TimerSpec updateTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final Duration timerOffset;
    private final String updatedSchema;
    private final FakeDatasetService fakeDatasetService;

    UpdateTableSchemaDoFn(
        Duration timerOffset, TableSchema updatedSchema, FakeDatasetService fakeDatasetService) {
      this.timerOffset = timerOffset;
      this.updatedSchema = BigQueryHelpers.toJsonString(updatedSchema);
      this.fakeDatasetService = fakeDatasetService;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, TableRow> element,
        @TimerId("updateTimer") Timer updateTimer,
        OutputReceiver<TableRow> o)
        throws IOException {
      updateTimer.offset(timerOffset).setRelative();
      o.output(element.getValue());
    }

    @OnTimer("updateTimer")
    public void onTimer(@Key String tableSpec) throws IOException {
      fakeDatasetService.updateTableSchema(
          BigQueryHelpers.parseTableSpec(tableSpec),
          BigQueryHelpers.fromJsonString(updatedSchema, TableSchema.class));
    }
  }

  public void updateTableSchemaTest(boolean useSet) throws Exception {
    assumeTrue(useStreaming);
    assumeTrue(useStorageApi);

    // Make sure that GroupIntoBatches does not buffer data.
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(1);
    p.getOptions().as(BigQueryOptions.class).setNumStorageWriteApiStreams(1);

    BigQueryIO.Write.Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;
    p.enableAbandonedNodeEnforcement(false);

    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset-id.table");
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("req").setType("STRING").setMode("REQUIRED")));

    // Add new fields to the update schema. Also reorder some existing fields to validate that we
    // handle update
    // field reordering correctly.
    TableSchema tableSchemaUpdated =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("req").setType("STRING"),
                    new TableFieldSchema().setName("double_number").setType("INTEGER")));
    fakeDatasetService.createTable(new Table().setTableReference(tableRef).setSchema(tableSchema));

    LongFunction<TableRow> getRowSet =
        (LongFunction<TableRow> & Serializable)
            (long i) -> {
              TableRow row =
                  new TableRow()
                      .set("name", "name" + i)
                      .set("number", Long.toString(i))
                      .set("double_number", Long.toString(i * 2));
              if (i <= 5) {
                row = row.set("req", "foo");
              }
              return row;
            };

    LongFunction<TableRow> getRowSetF =
        (LongFunction<TableRow> & Serializable)
            (long i) ->
                new TableRow()
                    .setF(
                        ImmutableList.of(
                            new TableCell().setV(Long.toString(i)),
                            new TableCell().setV("name" + i),
                            new TableCell().setV(i > 5 ? null : "foo"),
                            new TableCell().setV(Long.toString(i * 2))));

    LongFunction<TableRow> getRow = useSet ? getRowSet : getRowSetF;

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    // These rows contain unknown fields, which should be dropped.
    for (long i = 0; i < 5; i++) {
      testStream = testStream.addElements(i);
    }
    // Expire the timer, which should update the schema.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(10));
    // Add one element to trigger discovery of new schema.
    testStream = testStream.addElements(5L);
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(10));

    // Now all fields should be known.
    for (long i = 6; i < 10; i++) {
      testStream = testStream.addElements(i);
    }

    PCollection<TableRow> tableRows =
        p.apply(testStream.advanceWatermarkToInfinity())
            .apply("getRow", MapElements.into(TypeDescriptor.of(TableRow.class)).via(getRow::apply))
            .apply("add key", WithKeys.of("project-id:dataset-id.table"))
            .apply(
                "update schema",
                ParDo.of(
                    new UpdateTableSchemaDoFn(
                        Duration.standardSeconds(5), tableSchemaUpdated, fakeDatasetService)))
            .setCoder(TableRowJsonCoder.of());

    tableRows.apply(
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .ignoreUnknownValues()
            .withAutoSchemaUpdate(true)
            .withTestServices(fakeBqServices)
            .withoutValidation());

    p.run();

    Iterable<TableRow> expectedDroppedValues =
        LongStream.range(0, 6)
            .mapToObj(getRowSet)
            .map(tr -> filterUnknownValues(tr, tableSchema.getFields()))
            .collect(Collectors.toList());
    Iterable<TableRow> expectedFullValues =
        LongStream.range(6, 10).mapToObj(getRowSet).collect(Collectors.toList());
    assertThat(
        fakeDatasetService.getAllRows(
            tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.concat(expectedDroppedValues, expectedFullValues), TableRow.class)));
  }

  TableRow filterUnknownValues(TableRow row, List<TableFieldSchema> tableSchemaFields) {
    Map<String, String> schemaTypes =
        tableSchemaFields.stream()
            .collect(Collectors.toMap(TableFieldSchema::getName, TableFieldSchema::getType));
    Map<String, List<TableFieldSchema>> schemaFields =
        tableSchemaFields.stream()
            .filter(tf -> tf.getFields() != null && !tf.getFields().isEmpty())
            .collect(Collectors.toMap(TableFieldSchema::getName, TableFieldSchema::getFields));
    TableRow filtered = new TableRow();
    if (row.getF() != null) {
      List<TableCell> values = Lists.newArrayList();
      for (int i = 0; i < tableSchemaFields.size(); ++i) {
        String fieldType = tableSchemaFields.get(i).getType();
        Object value = row.getF().get(i).getV();
        if (fieldType.equals("STRUCT") || fieldType.equals("RECORD")) {
          value = filterUnknownValues((TableRow) value, tableSchemaFields.get(i).getFields());
        }
        values.add(new TableCell().setV(value));
      }
      filtered = filtered.setF(values);
    } else {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        Object value = entry.getValue();
        @Nullable String fieldType = schemaTypes.get(entry.getKey());
        if (fieldType != null) {
          if (fieldType.equals("STRUCT") || fieldType.equals("RECORD")) {
            value = filterUnknownValues((TableRow) value, schemaFields.get(entry.getKey()));
          }
          filtered = filtered.set(entry.getKey(), value);
        }
      }
    }
    return filtered;
  }

  @Test
  public void testBatchStorageWriteWithIgnoreUnknownValues() throws Exception {
    batchStorageWriteWithIgnoreUnknownValues(false);
  }

  @Test
  public void testBatchStorageWriteWithIgnoreUnknownValuesWithInputSchema() throws Exception {
    batchStorageWriteWithIgnoreUnknownValues(true);
  }

  public void batchStorageWriteWithIgnoreUnknownValues(boolean withInputSchema) throws Exception {
    assumeTrue(!useStreaming);
    // Make sure that GroupIntoBatches does not buffer data.
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(1);

    BigQueryIO.Write.Method method =
        useStorageApi ? Method.STORAGE_WRITE_API : Method.STORAGE_API_AT_LEAST_ONCE;
    p.enableAbandonedNodeEnforcement(false);

    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset-id.table");
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("name").setType("STRING")));

    fakeDatasetService.createTable(new Table().setTableReference(tableRef).setSchema(tableSchema));

    List<TableRow> rows =
        Arrays.asList(
            new TableRow().set("number", "1").set("name", "a"),
            new TableRow().set("number", "2").set("name", "b").set("extra", "aaa"),
            new TableRow()
                .set("number", "3")
                .set("name", "c")
                .set("repeated", Arrays.asList("a", "a")),
            new TableRow().set("number", "4").set("name", "d").set("req", "req_a"),
            new TableRow()
                .set("number", "5")
                .set("name", "e")
                .set("repeated", Arrays.asList("a", "a"))
                .set("req", "req_a"));

    BigQueryIO.Write<TableRow> writeTransform =
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .ignoreUnknownValues()
            .withTestServices(fakeBqServices)
            .withoutValidation();
    if (withInputSchema) {
      writeTransform = writeTransform.withSchema(tableSchema);
    }
    WriteResult result = p.apply(Create.of(rows)).apply(writeTransform);
    // we ignore extra values instead of sending to DLQ. check that it's empty:
    PAssert.that(result.getFailedStorageApiInserts()).empty();
    p.run().waitUntilFinish();

    Iterable<TableRow> expectedDroppedValues =
        rows.subList(1, 5).stream()
            .map(tr -> filterUnknownValues(tr, tableSchema.getFields()))
            .collect(Collectors.toList());

    Iterable<TableRow> expectedFullValues = rows.subList(0, 1);

    assertThat(
        fakeDatasetService.getAllRows(
            tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.concat(expectedDroppedValues, expectedFullValues), TableRow.class)));
  }

  @Test
  public void testStreamingWriteValidateFailsWithoutTriggeringFrequency() {
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApiApproximate);
    p.enableAbandonedNodeEnforcement(false);
    Method method = useStorageApi ? Method.STORAGE_WRITE_API : Method.FILE_LOADS;

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("unbounded PCollection via FILE_LOADS or STORAGE_WRITE_API");
    thrown.expectMessage("triggering frequency must be specified");

    p.getOptions().as(BigQueryOptions.class).setStorageWriteApiTriggeringFrequencySec(null);
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply(
            BigQueryIO.<InputRecord>write()
                .withAvroFormatFunction(r -> new GenericData.Record(r.getSchema()))
                .to("dataset.table")
                .withMethod(method)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER));
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
    assumeTrue(!useStorageApi);
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Only one of withFormatFunction or withAvroFormatFunction/withAvroWriter maybe set, not"
            + " both.");
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
    assumeTrue(!useStorageApi);
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
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApi);
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Avro output is not supported when method == STREAMING_INSERTS");
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
    assumeTrue(!useStreaming);
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Auto-sharding is only applicable to an unbounded PCollection, but the input PCollection is BOUNDED.");
    p.apply(Create.empty(INPUT_RECORD_CODER))
        .apply(
            BigQueryIO.<InputRecord>write()
                .to("dataset.table")
                .withSchema(new TableSchema())
                .withAutoSharding()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }

  @Test
  public void testMaxRetryJobs() {
    assumeTrue(!useStorageApi);
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("dataset.table")
            .withSchema(new TableSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchemaUpdateOptions(
                EnumSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
            .withMaxRetryJobs(500);
    assertEquals(500, write.getMaxRetryJobs());
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

    DynamicDestinations<String, String> dynamicDestinations = new IdentityDynamicTables();

    fakeJobService.setNumFailuresExpected(3);
    WriteTables<String> writeTables =
        new WriteTables<>(
            true,
            fakeBqServices,
            jobIdTokenView,
            BigQueryIO.Write.WriteDisposition.WRITE_EMPTY,
            BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
            sideInputs,
            dynamicDestinations,
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
            .setCoder(KvCoder.of(StringUtf8Coder.of(), INSTANCE))
            .apply(
                ParDo.of(
                    new DoFn<
                        KV<String, WriteTables.Result>,
                        KV<TableDestination, WriteTables.Result>>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, WriteTables.Result> e,
                          OutputReceiver<KV<TableDestination, WriteTables.Result>> o) {
                        o.output(KV.of(dynamicDestinations.getTable(e.getKey()), e.getValue()));
                      }
                    }));

    PAssert.thatMultimap(writeTablesOutput)
        .satisfies(
            input -> {
              assertEquals(expectedTempTables.keySet(), input.keySet());
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
            null,
            jobIdTokenView);

    // Unfortunate hack to have create treat tempTablesElement as a single element, instead of as an
    // iterable
    p.apply(
            Create.of(
                    ImmutableList.of(
                        (Iterable<KV<TableDestination, WriteTables.Result>>) tempTablesElement))
                .withCoder(IterableCoder.of(KvCoder.of(TableDestinationCoder.of(), INSTANCE))))
        .apply(writeRename);

    p.run().waitUntilFinish();

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

    WriteRename.TempTableCleanupFn.removeTemporaryTables(datasetService, tableRefs);

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
  public void testStreamingInsertsExtendedErrorRetrieval() throws Exception {
    assumeTrue(!useStorageApi);
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
  public void testStorageApiErrorsWriteProto() throws Exception {
    assumeTrue(useStorageApi);
    final Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;

    final int failFrom = 10;

    Function<Integer, Proto3SchemaMessages.Primitive> getPrimitive =
        (Integer i) ->
            Proto3SchemaMessages.Primitive.newBuilder()
                .setPrimitiveDouble(i)
                .setPrimitiveFloat(i)
                .setPrimitiveInt32(i)
                .setPrimitiveInt64(i)
                .setPrimitiveUint32(i)
                .setPrimitiveUint64(i)
                .setPrimitiveSint32(i)
                .setPrimitiveSint64(i)
                .setPrimitiveFixed32(i)
                .setPrimitiveFixed64(i)
                .setPrimitiveBool(true)
                .setPrimitiveString(Integer.toString(i))
                .setPrimitiveBytes(
                    ByteString.copyFrom(Integer.toString(i).getBytes(StandardCharsets.UTF_8)))
                .build();
    List<Proto3SchemaMessages.Primitive> goodRows =
        IntStream.range(1, 20).mapToObj(getPrimitive::apply).collect(Collectors.toList());

    Function<Integer, TableRow> getPrimitiveRow =
        (Integer i) ->
            new TableRow()
                .set("primitive_double", Double.valueOf(i))
                .set("primitive_float", Float.valueOf(i).doubleValue())
                .set("primitive_int32", i.intValue())
                .set("primitive_int64", i.toString())
                .set("primitive_uint32", i.toString())
                .set("primitive_uint64", i.toString())
                .set("primitive_sint32", i.toString())
                .set("primitive_sint64", i.toString())
                .set("primitive_fixed32", i.toString())
                .set("primitive_fixed64", i.toString())
                .set("primitive_bool", true)
                .set("primitive_string", i.toString())
                .set(
                    "primitive_bytes",
                    BaseEncoding.base64()
                        .encode(
                            ByteString.copyFrom(i.toString().getBytes(StandardCharsets.UTF_8))
                                .toByteArray()));

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable)
            tr ->
                tr.containsKey("primitive_int32")
                    && (Integer) tr.get("primitive_int32") >= failFrom;
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    SerializableFunction<Proto3SchemaMessages.Primitive, TableRow> formatRecordOnFailureFunction =
        input -> {
          TableRow failedTableRow = new TableRow().set("testFailureFunctionField", "testValue");
          failedTableRow.set("originalValue", input.getPrimitiveFixed32());
          return failedTableRow;
        };

    WriteResult result =
        p.apply(Create.of(goodRows))
            .apply(
                BigQueryIO.writeProtos(Proto3SchemaMessages.Primitive.class)
                    .to("project-id:dataset-id.table")
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(method)
                    .withoutValidation()
                    .withFormatRecordOnFailureFunction(formatRecordOnFailureFunction)
                    .withPropagateSuccessfulStorageApiWrites(true)
                    .withTestServices(fakeBqServices));

    PCollection<TableRow> deadRows =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));

    List<TableRow> expectedFailedRows =
        goodRows.stream()
            .filter(primitive -> primitive.getPrimitiveFixed32() >= failFrom)
            .map(formatRecordOnFailureFunction::apply)
            .collect(Collectors.toList());
    PAssert.that(deadRows).containsInAnyOrder(expectedFailedRows);
    p.run();

    // Round trip through the coder to make sure the types match our expected types.
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table").stream()
            .map(
                tr -> {
                  try {
                    byte[] bytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), tr);
                    return CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), bytes);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList()),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(
                    goodRows.stream()
                        .map(primitive -> getPrimitiveRow.apply(primitive.getPrimitiveFixed32()))
                        .collect(Collectors.toList()),
                    r -> !shouldFailRow.apply(r)),
                TableRow.class)));
  }

  @Test
  public void testStorageApiErrorsWriteBeamRow() throws Exception {
    assumeTrue(useStorageApi);
    final Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;

    final int failFrom = 10;
    final String shouldFailName = "failme";

    List<SchemaPojo> goodRows =
        Lists.newArrayList(
            new SchemaPojo("a", 1),
            new SchemaPojo("b", 2),
            new SchemaPojo("c", 10),
            new SchemaPojo("d", 11),
            new SchemaPojo(shouldFailName, 1));

    String nameField = "name";
    String numberField = "number";
    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable)
            tr ->
                shouldFailName.equals(tr.get(nameField))
                    || (Integer.valueOf((String) tr.get(numberField)) >= failFrom);
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    SerializableFunction<SchemaPojo, TableRow> formatRecordOnFailureFunction =
        input -> {
          TableRow failedTableRow = new TableRow().set("testFailureFunctionField", "testValue");
          failedTableRow.set("originalName", input.name);
          failedTableRow.set("originalNumber", input.number);
          return failedTableRow;
        };

    WriteResult result =
        p.apply(Create.of(goodRows))
            .apply(
                BigQueryIO.<SchemaPojo>write()
                    .to("project-id:dataset-id.table")
                    .withMethod(method)
                    .useBeamSchema()
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withPropagateSuccessfulStorageApiWrites(true)
                    .withTestServices(fakeBqServices)
                    .withFormatRecordOnFailureFunction(formatRecordOnFailureFunction)
                    .withoutValidation());

    PCollection<TableRow> deadRows =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));
    PCollection<TableRow> successfulRows = result.getSuccessfulStorageApiInserts();

    List<TableRow> expectedFailedRows =
        goodRows.stream()
            .filter(pojo -> shouldFailName.equals(pojo.name) || pojo.number >= failFrom)
            .map(formatRecordOnFailureFunction::apply)
            .collect(Collectors.toList());
    PAssert.that(deadRows).containsInAnyOrder(expectedFailedRows);
    PAssert.that(successfulRows)
        .containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(
                    goodRows.stream()
                        .map(
                            pojo -> {
                              TableRow tableRow = new TableRow();
                              tableRow.set(nameField, pojo.name);
                              tableRow.set(numberField, String.valueOf(pojo.number));
                              return tableRow;
                            })
                        .collect(Collectors.toList()),
                    r -> !shouldFailRow.apply(r)),
                TableRow.class));
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table"),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(
                    goodRows.stream()
                        .map(
                            pojo -> {
                              TableRow tableRow = new TableRow();
                              tableRow.set(nameField, pojo.name);
                              tableRow.set(numberField, String.valueOf(pojo.number));
                              return tableRow;
                            })
                        .collect(Collectors.toList()),
                    r -> !shouldFailRow.apply(r)),
                TableRow.class)));
  }

  @Test
  public void testStorageApiErrorsWriteGenericRecord() throws Exception {
    assumeTrue(useStorageApi);
    final Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;

    final long failFrom = 10L;
    List<Long> goodRows = LongStream.range(0, 20).boxed().collect(Collectors.toList());

    String fieldName = "number";
    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable)
            tr -> (Long.valueOf((String) tr.get(fieldName))) >= failFrom;
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    SerializableFunction<Long, TableRow> formatRecordOnFailureFunction =
        input -> {
          TableRow failedTableRow = new TableRow().set("testFailureFunctionField", "testValue");
          failedTableRow.set("originalElement", input);
          return failedTableRow;
        };

    WriteResult result =
        p.apply(Create.of(goodRows))
            .apply(
                BigQueryIO.<Long>write()
                    .to("project-id:dataset-id.table")
                    .withMethod(method)
                    .withAvroFormatFunction(
                        (SerializableFunction<AvroWriteRequest<Long>, GenericRecord>)
                            input ->
                                new GenericRecordBuilder(avroSchema)
                                    .set(fieldName, input.getElement())
                                    .build())
                    .withSchema(
                        new TableSchema()
                            .setFields(
                                ImmutableList.of(
                                    new TableFieldSchema().setName(fieldName).setType("INTEGER"))))
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withPropagateSuccessfulStorageApiWrites(true)
                    .withTestServices(fakeBqServices)
                    .withFormatRecordOnFailureFunction(formatRecordOnFailureFunction)
                    .withoutValidation());

    PCollection<TableRow> deadRows =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));
    PCollection<TableRow> successfulRows = result.getSuccessfulStorageApiInserts();

    List<TableRow> expectedFailedRows =
        goodRows.stream()
            .filter(l -> l >= failFrom)
            .map(formatRecordOnFailureFunction::apply)
            .collect(Collectors.toList());
    PAssert.that(deadRows).containsInAnyOrder(expectedFailedRows);
    PAssert.that(successfulRows)
        .containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(
                    goodRows.stream()
                        .map(
                            l -> {
                              TableRow tableRow = new TableRow();
                              tableRow.set(fieldName, String.valueOf(l));
                              return tableRow;
                            })
                        .collect(Collectors.toList()),
                    r -> !shouldFailRow.apply(r)),
                TableRow.class));
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table"),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(
                    goodRows.stream()
                        .map(
                            l -> {
                              TableRow tableRow = new TableRow();
                              tableRow.set(fieldName, String.valueOf(l));
                              return tableRow;
                            })
                        .collect(Collectors.toList()),
                    r -> !shouldFailRow.apply(r)),
                TableRow.class)));
  }

  @Test
  public void testStorageApiErrorsWriteTableRows() throws Exception {
    assumeTrue(useStorageApi);
    final Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;

    TableSchema subSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(new TableFieldSchema().setName("number").setType("INTEGER")));

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema()
                        .setName("name")
                        .setType("STRING")
                        .setMode(Mode.REQUIRED.name()),
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema()
                        .setName("nested")
                        .setType("RECORD")
                        .setFields(subSchema.getFields())));

    TableRow goodNested = new TableRow().set("number", "42");
    TableRow badNested = new TableRow().set("number", "nAn");

    final String failValue = "failme";
    List<TableRow> goodRows =
        ImmutableList.of(
            new TableRow().set("name", "n1").set("number", "1"),
            new TableRow().set("name", failValue).set("number", "1"),
            new TableRow().set("name", "n2").set("number", "2"),
            new TableRow().set("name", failValue).set("number", "2"),
            new TableRow().set("name", "parent1").set("nested", goodNested),
            new TableRow().set("name", failValue).set("number", "1"));
    List<TableRow> badRows =
        ImmutableList.of(
            // Unknown field.
            new TableRow().set("name", "n3").set("number", "3").set("badField", "foo"),
            // Unknown field.
            new TableRow()
                .setF(
                    ImmutableList.of(
                        new TableCell().setV("n3"),
                        new TableCell().setV("3"),
                        new TableCell(),
                        new TableCell().setV("foo"))),
            // Wrong type.
            new TableRow().set("name", "n4").set("number", "baadvalue"),
            // Wrong type.
            new TableRow()
                .setF(
                    ImmutableList.of(
                        new TableCell().setV("n4"),
                        new TableCell().setV("baadvalue"),
                        new TableCell())),
            // Missing required field.
            new TableRow().set("number", "42"),
            // Invalid nested row
            new TableRow().set("name", "parent2").set("nested", badNested));

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable)
            tr -> tr.containsKey("name") && tr.get("name").equals(failValue);
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    SerializableFunction<TableRow, TableRow> formatRecordOnFailureFunction =
        input -> {
          TableRow failedTableRow = new TableRow().set("testFailureFunctionField", "testValue");
          if (input != null) {
            Object name = input.get("name");
            if (name != null) {
              failedTableRow.set("name", name);
            }
            Object number = input.get("number");
            if (number != null) {
              failedTableRow.set("number", number);
            }
          }
          return failedTableRow;
        };

    WriteResult result =
        p.apply(Create.of(Iterables.concat(goodRows, badRows)))
            .apply(
                BigQueryIO.writeTableRows()
                    .to("project-id:dataset-id.table")
                    .withMethod(method)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(tableSchema)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withPropagateSuccessfulStorageApiWrites(true)
                    .withTestServices(fakeBqServices)
                    .withFormatRecordOnFailureFunction(formatRecordOnFailureFunction)
                    .withoutValidation());

    PCollection<TableRow> deadRows =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));
    PCollection<TableRow> successfulRows = result.getSuccessfulStorageApiInserts();

    List<TableRow> expectedFailedRows =
        badRows.stream().map(formatRecordOnFailureFunction::apply).collect(Collectors.toList());
    expectedFailedRows.addAll(
        goodRows.stream()
            .filter(shouldFailRow::apply)
            .map(formatRecordOnFailureFunction::apply)
            .collect(Collectors.toList()));
    PAssert.that(deadRows).containsInAnyOrder(expectedFailedRows);
    PAssert.that(successfulRows)
        .containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(goodRows, r -> !shouldFailRow.apply(r)), TableRow.class));
    p.run();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table"),
        containsInAnyOrder(
            Iterables.toArray(
                Iterables.filter(goodRows, r -> !shouldFailRow.apply(r)), TableRow.class)));
  }

  @Test
  public void testStreamingInsertsWrongErrorConfigs() {
    assumeTrue(!useStorageApi);
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
                  + "does not use extended errors. Use getFailedInserts or getFailedStorageApiInserts instead"));
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
                  + "uses extended errors information. Use getFailedInsertsWithErr or getFailedStorageApiInserts instead"));
    }

    try {
      p.apply("Create3", Create.<TableRow>of(row1))
          .apply("Write3", bqIoWrite.withSuccessfulInsertsPropagation(false))
          .getSuccessfulInserts();
      fail();
    } catch (IllegalStateException e) {
      assertThat(
          e.getMessage(),
          is(
              "Retrieving successful inserts is only supported for streaming inserts. "
                  + "Make sure withSuccessfulInsertsPropagation is correctly configured for "
                  + "BigQueryIO.Write object."));
    }
  }

  void schemaUpdateOptionsTest(
      BigQueryIO.Write.Method insertMethod, Set<SchemaUpdateOption> schemaUpdateOptions)
      throws Exception {
    assumeTrue(!useStorageApi);
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
    assumeTrue(useStreaming);
    assumeTrue(!useStorageApi);
    Set<SchemaUpdateOption> options = EnumSet.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    p.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("SchemaUpdateOptions are not supported when method == STREAMING_INSERTS");
    schemaUpdateOptionsTest(BigQueryIO.Write.Method.STREAMING_INSERTS, options);
  }

  @Test
  public void testWriteWithStorageApiWithDefaultProject() throws Exception {
    assumeTrue(useStorageApi);
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to("dataset-id.table-id")
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(new TableFieldSchema().setName("name").setType("STRING"))))
            .withMethod(Method.STORAGE_WRITE_API)
            .withoutValidation()
            .withTestServices(fakeBqServices);

    p.apply(
            Create.of(new TableRow().set("name", "a"), new TableRow().set("name", "b"))
                .withCoder(TableRowJsonCoder.of()))
        .apply("WriteToBQ", write);
    p.run();
    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(new TableRow().set("name", "a"), new TableRow().set("name", "b")));
  }

  @Test
  public void testStorageWriteWithMultipleAppendsPerStream() throws Exception {
    assumeTrue(useStorageApi);

    // reduce threshold to trigger multiple stream appends
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(0);
    // limit parallelism to limit the number of write streams we have open
    p.getOptions().as(DirectOptions.class).setTargetParallelism(1);

    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("num").setType("INTEGER"),
                    new TableFieldSchema().setName("name").setType("STRING")));
    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(schema);
    fakeTable.setTableReference(ref);
    fakeDatasetService.createTable(fakeTable);

    List<TableRow> rows = new ArrayList<TableRow>(100);
    for (int i = 0; i < 100; i++) {
      rows.add(new TableRow().set("num", String.valueOf(i)).set("name", String.valueOf(i)));
    }
    PCollection<TableRow> tableRowPCollection;
    if (useStreaming) {
      TestStream<TableRow> testStream =
          TestStream.create(TableRowJsonCoder.of())
              .addElements(rows.get(0), Iterables.toArray(rows.subList(1, 25), TableRow.class))
              .advanceProcessingTime(Duration.standardMinutes(1))
              .addElements(rows.get(25), Iterables.toArray(rows.subList(26, 50), TableRow.class))
              .advanceProcessingTime(Duration.standardMinutes(1))
              .addElements(rows.get(50), Iterables.toArray(rows.subList(51, 75), TableRow.class))
              .addElements(rows.get(75), Iterables.toArray(rows.subList(76, 100), TableRow.class))
              .advanceWatermarkToInfinity();
      tableRowPCollection = p.apply(testStream);
    } else {
      tableRowPCollection = p.apply(Create.of(rows));
    }
    Method method =
        useStorageApiApproximate ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API;
    tableRowPCollection.apply(
        "Save Events To BigQuery",
        BigQueryIO.writeTableRows()
            .to(ref)
            .withMethod(method)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withTestServices(fakeBqServices));

    p.run().waitUntilFinish();

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(rows, TableRow.class)));
  }

  public static class ThrowingFakeDatasetServices extends FakeDatasetService {
    @Override
    public BigQueryServices.StreamAppendClient getStreamAppendClient(
        String streamName,
        DescriptorProtos.DescriptorProto descriptor,
        boolean useConnectionPool,
        AppendRowsRequest.MissingValueInterpretation missingValueInterpretation) {
      return new BigQueryServices.StreamAppendClient() {
        @Override
        public ApiFuture<AppendRowsResponse> appendRows(long offset, ProtoRows rows) {
          Map<Integer, String> errorMap = new HashMap<>();
          for (int i = 0; i < rows.getSerializedRowsCount(); i++) {
            errorMap.put(i, "some serialization error");
          }
          SettableApiFuture<AppendRowsResponse> appendResult = SettableApiFuture.create();
          appendResult.setException(
              new Exceptions.AppendSerializationError(
                  404, "some description", "some stream", errorMap));
          return appendResult;
        }

        @Override
        public com.google.cloud.bigquery.storage.v1.@Nullable TableSchema getUpdatedSchema() {
          return null;
        }

        @Override
        public void pin() {}

        @Override
        public void unpin() {}

        @Override
        public void close() {}
      };
    }
  }

  @Test
  public void testStorageWriteReturnsAppendSerializationError() throws Exception {
    assumeTrue(useStorageApi);
    assumeTrue(useStreaming);
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdRecordCount(5);

    TableSchema schema =
        new TableSchema()
            .setFields(Arrays.asList(new TableFieldSchema().setType("INTEGER").setName("long")));
    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(schema);
    fakeTable.setTableReference(ref);

    ThrowingFakeDatasetServices throwingService = new ThrowingFakeDatasetServices();
    throwingService.createTable(fakeTable);

    int numRows = 100;

    WriteResult res =
        p.apply(
                PeriodicImpulse.create()
                    .startAt(Instant.ofEpochMilli(0))
                    .stopAfter(Duration.millis(numRows - 1))
                    .withInterval(Duration.millis(1)))
            .apply(
                "Convert to longs",
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(instant -> new TableRow().set("long", instant.getMillis())))
            .apply(
                BigQueryIO.writeTableRows()
                    .to(ref)
                    .withSchema(schema)
                    .withTestServices(
                        new FakeBigQueryServices()
                            .withDatasetService(throwingService)
                            .withJobService(fakeJobService)));

    PCollection<Integer> numErrors =
        res.getFailedStorageApiInserts()
            .apply(
                "Count errors",
                MapElements.into(TypeDescriptors.integers())
                    .via(err -> err.getErrorMessage().equals("some serialization error") ? 1 : 0))
            .apply(
                Window.<Integer>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(Sum.integersGlobally());

    PAssert.that(numErrors).containsInAnyOrder(numRows);

    p.run().waitUntilFinish();
  }

  @Test
  public void testWriteProtos() throws Exception {
    BigQueryIO.Write.Method method =
        useStreaming
            ? (useStorageApi
                ? (useStorageApiApproximate
                    ? Method.STORAGE_API_AT_LEAST_ONCE
                    : Method.STORAGE_WRITE_API)
                : Method.STREAMING_INSERTS)
            : useStorageApi ? Method.STORAGE_WRITE_API : Method.FILE_LOADS;
    Function<Integer, Proto3SchemaMessages.Primitive> getPrimitive =
        (Integer i) ->
            Proto3SchemaMessages.Primitive.newBuilder()
                .setPrimitiveDouble(i)
                .setPrimitiveFloat(i)
                .setPrimitiveInt32(i)
                .setPrimitiveInt64(i)
                .setPrimitiveUint32(i)
                .setPrimitiveUint64(i)
                .setPrimitiveSint32(i)
                .setPrimitiveSint64(i)
                .setPrimitiveFixed32(i)
                .setPrimitiveFixed64(i)
                .setPrimitiveBool(true)
                .setPrimitiveString(Integer.toString(i))
                .setPrimitiveBytes(
                    ByteString.copyFrom(Integer.toString(i).getBytes(StandardCharsets.UTF_8)))
                .build();
    Function<Integer, TableRow> getPrimitiveRow =
        (Integer i) ->
            new TableRow()
                .set("primitive_double", Double.valueOf(i))
                .set("primitive_float", Float.valueOf(i).doubleValue())
                .set("primitive_int32", i.intValue())
                .set("primitive_int64", i.toString())
                .set("primitive_uint32", i.toString())
                .set("primitive_uint64", i.toString())
                .set("primitive_sint32", i.toString())
                .set("primitive_sint64", i.toString())
                .set("primitive_fixed32", i.toString())
                .set("primitive_fixed64", i.toString())
                .set("primitive_bool", true)
                .set("primitive_string", i.toString())
                .set(
                    "primitive_bytes",
                    BaseEncoding.base64()
                        .encode(
                            ByteString.copyFrom(i.toString().getBytes(StandardCharsets.UTF_8))
                                .toByteArray()));

    List<Proto3SchemaMessages.Primitive> nestedItems =
        Lists.newArrayList(getPrimitive.apply(1), getPrimitive.apply(2), getPrimitive.apply(3));

    Iterable<Proto3SchemaMessages.Nested> items =
        nestedItems.stream()
            .map(
                p ->
                    Proto3SchemaMessages.Nested.newBuilder()
                        .setNested(p)
                        .addAllNestedList(Lists.newArrayList(p, p, p))
                        .build())
            .collect(Collectors.toList());

    List<TableRow> expectedNestedTableRows =
        Lists.newArrayList(
            getPrimitiveRow.apply(1), getPrimitiveRow.apply(2), getPrimitiveRow.apply(3));
    Iterable<TableRow> expectedItems =
        expectedNestedTableRows.stream()
            .map(
                p ->
                    new TableRow().set("nested", p).set("nested_list", Lists.newArrayList(p, p, p)))
            .collect(Collectors.toList());

    BigQueryIO.Write<Proto3SchemaMessages.Nested> write =
        BigQueryIO.writeProtos(Proto3SchemaMessages.Nested.class)
            .to("dataset-id.table-id")
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withMethod(method)
            .withoutValidation()
            .withTestServices(fakeBqServices);

    p.apply(Create.of(items)).apply("WriteToBQ", write);
    p.run();

    // Round trip through the coder to make sure the types match our expected types.
    List<TableRow> allRows =
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id").stream()
            .map(
                tr -> {
                  try {
                    byte[] bytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), tr);
                    return CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), bytes);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    assertThat(allRows, containsInAnyOrder(Iterables.toArray(expectedItems, TableRow.class)));
  }

  @Test
  public void testUpsertAndDeleteTableRows() throws Exception {
    assumeTrue(useStorageApi);
    assumeTrue(useStorageApiApproximate);

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("key1").setType("STRING"),
                    new TableFieldSchema().setName("key2").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")));

    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(tableSchema);
    fakeTable.setTableReference(ref);
    fakeDatasetService.createTable(fakeTable);
    fakeDatasetService.setPrimaryKey(ref, Lists.newArrayList("key1", "key2"));

    List<RowMutation> items =
        Lists.newArrayList(
            RowMutation.of(
                new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(0L))),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(0L))),
            RowMutation.of(
                new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "2"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(1L))),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.DELETE, Long.toHexString(1L))),
            RowMutation.of(
                new TableRow().set("key1", "foo3").set("key2", "bar3").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(0L))),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "3"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(2L))),
            RowMutation.of(
                new TableRow().set("key1", "foo4").set("key2", "bar4").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, Long.toHexString(0L))),
            RowMutation.of(
                new TableRow().set("key1", "foo4").set("key2", "bar4").set("value", "1"),
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.DELETE, Long.toHexString(1L))));

    BigQueryIO.Write<RowMutation> write =
        BigQueryIO.applyRowMutations()
            .to("dataset-id.table-id")
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withSchema(tableSchema)
            .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)
            .withoutValidation()
            .withTestServices(fakeBqServices);

    p.apply(Create.of(items)).apply("WriteToBQ", write);
    p.run();

    List<TableRow> expected =
        Lists.newArrayList(
            new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "2"),
            new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "3"),
            new TableRow().set("key1", "foo3").set("key2", "bar3").set("value", "1"));

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
  }

  @Test
  public void testUpsertAndDeleteGenericRecords() throws Exception {
    assumeTrue(useStorageApi);
    assumeTrue(useStorageApiApproximate);

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("key1").setType("STRING"),
                    new TableFieldSchema().setName("key2").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING"),
                    new TableFieldSchema().setName("updateType").setType("STRING"),
                    new TableFieldSchema().setName("sqn").setType("STRING")));

    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(tableSchema);
    fakeTable.setTableReference(ref);
    fakeDatasetService.createTable(fakeTable);
    fakeDatasetService.setPrimaryKey(ref, Lists.newArrayList("key1", "key2"));

    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .optionalString("key1")
            .optionalString("key2")
            .optionalString("value")
            .optionalString("updateType")
            .requiredString("sqn")
            .endRecord();

    List<GenericRecord> items =
        Lists.newArrayList(
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo0")
                .set("key2", "bar0")
                .set("value", "1")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(0L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo1")
                .set("key2", "bar1")
                .set("value", "1")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(0L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo0")
                .set("key2", "bar0")
                .set("value", "2")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(1L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo1")
                .set("key2", "bar1")
                .set("value", "1")
                .set("updateType", "DELETE")
                .set("sqn", Long.toHexString(1L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo3")
                .set("key2", "bar3")
                .set("value", "1")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(0L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo1")
                .set("key2", "bar1")
                .set("value", "3")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(2L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo4")
                .set("key2", "bar4")
                .set("value", "1")
                .set("updateType", "UPSERT")
                .set("sqn", Long.toHexString(0L))
                .build(),
            new GenericRecordBuilder(avroSchema)
                .set("key1", "foo4")
                .set("key2", "bar4")
                .set("value", "1")
                .set("updateType", "DELETE")
                .set("sqn", Long.toHexString(1L))
                .build());

    BigQueryIO.Write<GenericRecord> write =
        BigQueryIO.writeGenericRecords()
            .to("dataset-id.table-id")
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withSchema(tableSchema)
            .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)
            .withRowMutationInformationFn(
                r -> {
                  RowMutationInformation.MutationType mutationType =
                      RowMutationInformation.MutationType.valueOf(r.get("updateType").toString());
                  String sqn = r.get("sqn").toString();
                  return RowMutationInformation.of(mutationType, sqn);
                })
            .withoutValidation()
            .withTestServices(fakeBqServices);

    p.apply(Create.of(items).withCoder(AvroGenericCoder.of(avroSchema))).apply("WriteToBQ", write);
    p.run();

    List<TableRow> expected =
        Lists.newArrayList(
            new TableRow()
                .set("key1", "foo0")
                .set("key2", "bar0")
                .set("value", "2")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(1)),
            new TableRow()
                .set("key1", "foo1")
                .set("key2", "bar1")
                .set("value", "3")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(2)),
            new TableRow()
                .set("key1", "foo3")
                .set("key2", "bar3")
                .set("value", "1")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(0)));

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
  }

  @Test
  public void testUpsertAndDeleteBeamRows() throws Exception {
    assumeTrue(useStorageApi);
    assumeTrue(useStorageApiApproximate);

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("key1").setType("STRING"),
                    new TableFieldSchema().setName("key2").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING"),
                    new TableFieldSchema().setName("updateType").setType("STRING"),
                    new TableFieldSchema().setName("sqn").setType("STRING")));

    Table fakeTable = new Table();
    TableReference ref =
        new TableReference()
            .setProjectId("project-id")
            .setDatasetId("dataset-id")
            .setTableId("table-id");
    fakeTable.setSchema(tableSchema);
    fakeTable.setTableReference(ref);
    fakeDatasetService.createTable(fakeTable);
    fakeDatasetService.setPrimaryKey(ref, Lists.newArrayList("key1", "key2"));

    Schema beamSchema =
        Schema.builder()
            .addNullableStringField("key1")
            .addNullableStringField("key2")
            .addNullableStringField("value")
            .addNullableStringField("updateType")
            .addNullableStringField("sqn")
            .build();

    List<Row> items =
        Lists.newArrayList(
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo0")
                .withFieldValue("key2", "bar0")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(0L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo1")
                .withFieldValue("key2", "bar1")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(0L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo0")
                .withFieldValue("key2", "bar0")
                .withFieldValue("value", "2")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(1L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo1")
                .withFieldValue("key2", "bar1")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "DELETE")
                .withFieldValue("sqn", Long.toHexString(1L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo3")
                .withFieldValue("key2", "bar3")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(0L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo1")
                .withFieldValue("key2", "bar1")
                .withFieldValue("value", "3")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(2L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo4")
                .withFieldValue("key2", "bar4")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "UPSERT")
                .withFieldValue("sqn", Long.toHexString(0L))
                .build(),
            Row.withSchema(beamSchema)
                .withFieldValue("key1", "foo4")
                .withFieldValue("key2", "bar4")
                .withFieldValue("value", "1")
                .withFieldValue("updateType", "DELETE")
                .withFieldValue("sqn", Long.toHexString(1L))
                .build());

    BigQueryIO.Write<Row> write =
        BigQueryIO.<Row>write()
            .to("dataset-id.table-id")
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withSchema(tableSchema)
            .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)
            .useBeamSchema()
            .withRowMutationInformationFn(
                r ->
                    RowMutationInformation.of(
                        RowMutationInformation.MutationType.valueOf(r.getString("updateType")),
                        r.getString("sqn")))
            .withoutValidation()
            .withTestServices(fakeBqServices);

    p.apply(Create.of(items).withRowSchema(beamSchema)).apply("WriteToBQ", write);
    p.run();

    List<TableRow> expected =
        Lists.newArrayList(
            new TableRow()
                .set("key1", "foo0")
                .set("key2", "bar0")
                .set("value", "2")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(1)),
            new TableRow()
                .set("key1", "foo1")
                .set("key2", "bar1")
                .set("value", "3")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(2)),
            new TableRow()
                .set("key1", "foo3")
                .set("key2", "bar3")
                .set("value", "1")
                .set("updatetype", "UPSERT")
                .set("sqn", Long.toHexString(0)));

    assertThat(
        fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id"),
        containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
  }
}
