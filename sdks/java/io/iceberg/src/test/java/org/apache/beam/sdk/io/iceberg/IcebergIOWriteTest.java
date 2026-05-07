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
package org.apache.beam.sdk.io.iceberg;

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamRowToIcebergRecord;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class IcebergIOWriteTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOWriteTest.class);

  private static final String NONE = "none";
  private static final String HASH = "hash";
  private static final String HASH_WITH_AUTOSHARDING = "hashWithAutoSharding";

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return asList(new Object[][] {{NONE}, {HASH}, {HASH_WITH_AUTOSHARDING}});
  }

  @Parameterized.Parameter(0)
  public String distributionMode;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private IcebergIO.WriteRows writeTransform(
      IcebergCatalogConfig catalog, TableIdentifier tableId) {
    IcebergIO.WriteRows write = IcebergIO.writeRows(catalog).to(tableId);
    return applyDistribution(write);
  }

  private IcebergIO.WriteRows writeTransform(
      IcebergCatalogConfig catalog, DynamicDestinations dynamicDestinations) {
    IcebergIO.WriteRows write = IcebergIO.writeRows(catalog).to(dynamicDestinations);
    return applyDistribution(write);
  }

  private IcebergIO.WriteRows applyDistribution(IcebergIO.WriteRows write) {
    if (distributionMode.contains(HASH)) {
      write = write.withDistributionMode(DistributionMode.HASH);
    }
    if (distributionMode.equals(HASH_WITH_AUTOSHARDING)) {
      write = write.withAutosharding();
    }
    return write;
  }

  @Test
  public void testSimpleAppend() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", writeTransform(catalog, tableId));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    Table table = warehouse.loadTable(tableId);
    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());

    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testCreateNamespaceAndTable() {
    Namespace newNamespace = Namespace.of("new_namespace");
    TableIdentifier tableId =
        TableIdentifier.of(newNamespace, "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder().setCatalogProperties(catalogProps).build();

    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", writeTransform(catalog, tableId));

    assertFalse(((SupportsNamespaces) catalog.catalog()).namespaceExists(newNamespace));
    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    assertTrue(((SupportsNamespaces) catalog.catalog()).namespaceExists(newNamespace));
    LOG.info("Done running pipeline");

    Table table = warehouse.loadTable(tableId);
    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());

    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  /** Tests that a small write to three different tables with dynamic destinations works. */
  @Test
  public void testDynamicDestinationsWithoutSpillover() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);
    final TableIdentifier table1Id = TableIdentifier.of("default", "table1-" + salt);
    final TableIdentifier table2Id = TableIdentifier.of("default", "table2-" + salt);
    final TableIdentifier table3Id = TableIdentifier.of("default", "table3-" + salt);

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          @Override
          public Schema getDataSchema() {
            return IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);
          }

          @Override
          public Row getData(Row element) {
            return element;
          }

          @Override
          public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
            long tableNumber = element.getValue().getInt64("id") / 3 + 1;
            return String.format("default.table%s-%s", tableNumber, salt);
          }

          @Override
          public IcebergDestination instantiateDestination(String dest) {
            return IcebergDestination.builder()
                .setTableIdentifier(TableIdentifier.parse(dest))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }
        };

    testPipeline
        .apply(
            "Records To Add",
            Create.of(
                TestFixtures.asRows(
                    Iterables.concat(
                        TestFixtures.FILE1SNAPSHOT1,
                        TestFixtures.FILE1SNAPSHOT2,
                        TestFixtures.FILE1SNAPSHOT3))))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", writeTransform(catalog, dynamicDestinations));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    Table table1 = warehouse.loadTable(table1Id);
    Table table2 = warehouse.loadTable(table2Id);
    Table table3 = warehouse.loadTable(table3Id);

    List<Record> writtenRecords1 = ImmutableList.copyOf(IcebergGenerics.read(table1).build());
    List<Record> writtenRecords2 = ImmutableList.copyOf(IcebergGenerics.read(table2).build());
    List<Record> writtenRecords3 = ImmutableList.copyOf(IcebergGenerics.read(table3).build());

    assertThat(writtenRecords1, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
    assertThat(writtenRecords2, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT2.toArray()));
    assertThat(writtenRecords3, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT3.toArray()));
  }

  /**
   * Tests writing to enough destinations to spill over to the "slow" write path.
   *
   * <p>Note that we could have added a config to lower the spill number but exceeding the default
   * is fine
   */
  @Test
  public void testDynamicDestinationsWithSpillover() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);

    // Create far more tables than the max writers per bundle
    int numDestinations = 5 * WriteUngroupedRowsToFiles.DEFAULT_MAX_WRITERS_PER_BUNDLE;
    List<TableIdentifier> tableIdentifiers = Lists.newArrayList();
    List<Table> tables = Lists.newArrayList();
    for (int i = 0; i < numDestinations; ++i) {
      TableIdentifier id = TableIdentifier.of("default", "table" + i + "-" + salt);
      tableIdentifiers.add(id);
      tables.add(warehouse.createTable(id, TestFixtures.SCHEMA));
    }

    // Create plenty of data to hit each table
    int numElements = 10 * numDestinations;
    List<Record> elements = Lists.newArrayList();
    final Record genericRecord = GenericRecord.create(TestFixtures.SCHEMA);
    Map<TableIdentifier, List<Record>> elementsPerTable = Maps.newHashMap();
    for (int i = 0; i < numElements; ++i) {
      Record element = genericRecord.copy(ImmutableMap.of("id", (long) i, "data", "data for " + i));
      TableIdentifier tableId = tableIdentifiers.get(i % numDestinations);
      elements.add(element);
      elementsPerTable.computeIfAbsent(tableId, ignored -> Lists.newArrayList()).add(element);
    }

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          @Override
          public Schema getDataSchema() {
            return IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);
          }

          @Override
          public Row getData(Row element) {
            return element;
          }

          @Override
          public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
            long tableNumber = element.getValue().getInt64("id") % numDestinations;
            return String.format("default.table%s-%s", tableNumber, salt);
          }

          @Override
          public IcebergDestination instantiateDestination(String dest) {
            return IcebergDestination.builder()
                .setTableIdentifier(TableIdentifier.parse(dest))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }
        };

    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(elements)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", writeTransform(catalog, dynamicDestinations));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    for (int i = 0; i < numDestinations; ++i) {
      TableIdentifier tableId = tableIdentifiers.get(i);
      Table table = tables.get(i);
      List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
      assertThat(
          writtenRecords, Matchers.containsInAnyOrder(elementsPerTable.get(tableId).toArray()));
    }
  }

  /**
   * A test of our assumptions about how two commits of the same file work in iceberg's Java API.
   */
  @Test
  public void testIdempotentCommit() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    // Create a table and add records to it.
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    Record record =
        beamRowToIcebergRecord(
            table.schema(),
            Row.withSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
                .addValues(42L, "bizzle")
                .build());

    OutputFile outputFile = table.io().newOutputFile(TEMPORARY_FOLDER.newFile().toString());
    DataWriter<Record> icebergDataWriter =
        Parquet.writeData(outputFile)
            .createWriterFunc(GenericParquetWriter::create)
            .schema(table.schema())
            .withSpec(table.spec())
            .overwrite()
            .build();

    icebergDataWriter.write(record);
    icebergDataWriter.close();
    DataFile dataFile = icebergDataWriter.toDataFile();

    AppendFiles update = table.newAppend();
    update.appendFile(dataFile);
    update.commit();

    AppendFiles secondUpdate = table.newAppend();
    secondUpdate.appendFile(dataFile);
    secondUpdate.commit();
  }

  @Test
  public void testStreamingWrite() {
    TableIdentifier tableId =
        TableIdentifier.of(
            "default", "streaming_" + Long.toString(UUID.randomUUID().hashCode(), 16));

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    List<Row> inputRows = TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1);
    TestStream<Row> stream =
        TestStream.create(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            // the first two rows are written within the same triggering interval,
            // so they should both be in the first snapshot
            .addElements(inputRows.get(0))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(inputRows.get(1))
            .advanceProcessingTime(Duration.standardSeconds(5))
            // the third row is written in a new triggering interval,
            // so we create a new snapshot for it.
            .addElements(inputRows.get(2))
            .advanceProcessingTime(Duration.standardSeconds(5))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, SnapshotInfo>> output =
        testPipeline
            .apply("Stream Records", stream)
            .apply(
                "Append To Table",
                writeTransform(catalog, tableId)
                    .withTriggeringFrequency(Duration.standardSeconds(3)))
            .getSnapshots();
    // verify that 2 snapshots are created (one per triggering interval)
    PCollection<Long> snapshots = output.apply(Count.globally());
    PAssert.that(snapshots).containsInAnyOrder(2L);
    testPipeline.run().waitUntilFinish();

    Table table = warehouse.loadTable(tableId);

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testHashDistribution() {
    assumeTrue(distributionMode.equals(HASH_WITH_AUTOSHARDING));
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    TableIdentifier tableId =
        TableIdentifier.of("default", "hash_" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();
    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    // create table with two partitions
    catalog
        .catalog()
        .createTable(
            tableId,
            IcebergUtils.beamSchemaToIcebergSchema(schema),
            PartitionSpec.builderFor(IcebergUtils.beamSchemaToIcebergSchema(schema))
                .bucket("id", 2)
                .build());

    // Prepare 100 rows and split them up into separate keys.
    // The "none" distribution will process each key in a separate writer DoFn,
    // essentially creating one file per parallel thread. This means one file per
    // record since each record is in its own key.
    // The "hash" distribution will group records by partition key first, resulting
    // in a much smaller number of files created.
    PCollection<Row> rows =
        testPipeline
            .apply(GenerateSequence.from(0).to(100))
            .apply(
                "Make rows",
                MapElements.into(TypeDescriptors.rows())
                    .via(i -> Row.withSchema(schema).addValues(i, "name_" + i).build()))
            .setRowSchema(schema)
            .apply(WithKeys.of(1L))
            .setCoder(KvCoder.of(VarLongCoder.of(), SchemaCoder.of(schema)))
            .apply(Redistribute.byKey())
            .apply(Values.create());

    Function<String, MapElements<KV<String, SnapshotInfo>, KV<String, Integer>>> getAddedFilesFunc =
        (distribution) ->
            MapElements.into(kvs(strings(), integers()))
                .via(
                    snapshot ->
                        KV.of(
                            distribution,
                            Integer.parseInt(
                                checkStateNotNull(snapshot.getValue().getSummary())
                                    .get("added-data-files"))));

    // 1. Write files without any additional config
    PCollection<KV<String, Integer>> noneDistributionAddedFiles =
        rows.apply(
                "none distribution write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withDistributionMode(DistributionMode.NONE))
            .getSnapshots()
            .apply("Get none files", getAddedFilesFunc.apply(NONE));
    // 2. Write files with hash distribution
    PCollection<KV<String, Integer>> hashDistributionAddedFiles =
        rows.apply(
                "hash distribution write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withDistributionMode(DistributionMode.HASH))
            .getSnapshots()
            .apply("Get hash files", getAddedFilesFunc.apply(HASH));
    // 3. Write files with hash distribution AND auto-sharding
    PCollection<KV<String, Integer>> hashAutoShardingDistributionAddedFiles =
        rows.apply(
                "hash distribution + autosharding write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withDistributionMode(DistributionMode.HASH)
                    .withAutosharding())
            .getSnapshots()
            .apply("Get hash autosharded files", getAddedFilesFunc.apply(HASH_WITH_AUTOSHARDING));

    PCollectionList.of(
            Arrays.asList(
                hashDistributionAddedFiles,
                noneDistributionAddedFiles,
                hashAutoShardingDistributionAddedFiles))
        .apply(Flatten.pCollections())
        .apply("add dummy key", WithKeys.of(1))
        .apply("group together", GroupByKey.create())
        .apply("unwrap values", Values.create())
        .apply(
            "validate num files",
            ParDo.of(
                new DoFn<Iterable<KV<String, Integer>>, Void>() {
                  @ProcessElement
                  public void process(@Element Iterable<KV<String, Integer>> sums) {
                    List<KV<String, Integer>> sumList = Lists.newArrayList(sums.iterator());
                    assertEquals(3, sumList.size());

                    int numFilesAddedNoneDist =
                        Iterables.getOnlyElement(
                            sumList.stream()
                                .filter(kv -> kv.getKey().equals(NONE))
                                .map(KV::getValue)
                                .collect(Collectors.toList()));

                    int numFilesAddedHashDist =
                        Iterables.getOnlyElement(
                            sumList.stream()
                                .filter(kv -> kv.getKey().equals(HASH))
                                .map(KV::getValue)
                                .collect(Collectors.toList()));

                    int numFilesAddedHashAutoShardingDist =
                        Iterables.getOnlyElement(
                            sumList.stream()
                                .filter(kv -> kv.getKey().equals(HASH_WITH_AUTOSHARDING))
                                .map(KV::getValue)
                                .collect(Collectors.toList()));

                    System.out.println("none: " + numFilesAddedNoneDist);
                    System.out.println("hash: " + numFilesAddedHashDist);
                    System.out.println(
                        "hash with autosharding: " + numFilesAddedHashAutoShardingDist);
                    // plain hash distribution should have exactly the same number of partitions
                    assertEquals(2, numFilesAddedHashDist);
                    // hash with autosharding may create sub-shards and lead to more than just 2
                    // files.
                    // should still be less than 'none' distribution though
                    assertTrue(numFilesAddedHashDist < numFilesAddedNoneDist);
                  }
                }));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testHashDistributionStreaming() {
    assumeTrue(distributionMode.equals(HASH_WITH_AUTOSHARDING));
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();

    TableIdentifier tableId =
        TableIdentifier.of(
            "default", "hash_streaming" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();
    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    // create table with two partitions
    catalog
        .catalog()
        .createTable(
            tableId,
            IcebergUtils.beamSchemaToIcebergSchema(schema),
            PartitionSpec.builderFor(IcebergUtils.beamSchemaToIcebergSchema(schema))
                .bucket("id", 2)
                .build());

    // Prepare 100 rows and split them up into separate keys.
    // The "none" distribution will process each key in a separate writer DoFn,
    // essentially creating one file per parallel thread. This means one file per
    // record since each record is in its own key.
    // The "hash" distribution will group records by partition key first, resulting
    // in a much smaller number of files created.
    PCollection<Row> rows =
        testPipeline
            .apply(
                TestStream.create(VarLongCoder.of())
                    .addElements(0L, LongStream.range(1, 10).boxed().toArray(Long[]::new))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .addElements(10L, LongStream.range(11, 20).boxed().toArray(Long[]::new))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .addElements(20L, LongStream.range(21, 30).boxed().toArray(Long[]::new))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .addElements(30L, LongStream.range(31, 40).boxed().toArray(Long[]::new))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .addElements(40L, LongStream.range(41, 50).boxed().toArray(Long[]::new))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .advanceProcessingTime(Duration.standardSeconds(10))
                    .advanceWatermarkToInfinity())
            .apply(
                "Make rows",
                MapElements.into(TypeDescriptors.rows())
                    .via(i -> Row.withSchema(schema).addValues(i, "name_" + i).build()))
            .setRowSchema(schema)
            .apply(WithKeys.of(r -> r.getInt64("id")))
            .setCoder(KvCoder.of(VarLongCoder.of(), SchemaCoder.of(schema)))
            .apply(Redistribute.byKey())
            .apply(Values.create());

    Function<String, MapElements<KV<String, SnapshotInfo>, KV<String, Integer>>> getAddedFilesFunc =
        (distribution) ->
            MapElements.into(kvs(strings(), integers()))
                .via(
                    snapshot ->
                        KV.of(
                            distribution,
                            Integer.parseInt(
                                checkStateNotNull(snapshot.getValue().getSummary())
                                    .get("added-data-files"))));

    // 1. Write files without any additional config
    PCollection<KV<String, Integer>> noneDistributionAddedFiles =
        rows.apply(
                "none distribution write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withTriggeringFrequency(Duration.standardSeconds(5))
                    .withDistributionMode(DistributionMode.NONE))
            .getSnapshots()
            .apply("Get none files", getAddedFilesFunc.apply(NONE));
    // 2. Write files with hash distribution
    PCollection<KV<String, Integer>> hashDistributionAddedFiles =
        rows.apply(
                "hash distribution write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withTriggeringFrequency(Duration.standardSeconds(5))
                    .withDistributionMode(DistributionMode.HASH))
            .getSnapshots()
            .apply("Get hash files", getAddedFilesFunc.apply(HASH));
    // 3. Write files with hash distribution AND auto-sharding
    PCollection<KV<String, Integer>> hashAutoShardingDistributionAddedFiles =
        rows.apply(
                "hash distribution + autosharding write",
                IcebergIO.writeRows(catalog)
                    .to(tableId)
                    .withTriggeringFrequency(Duration.standardSeconds(5))
                    .withDistributionMode(DistributionMode.HASH)
                    .withAutosharding())
            .getSnapshots()
            .apply("Get hash autosharded files", getAddedFilesFunc.apply(HASH_WITH_AUTOSHARDING));

    PCollectionList.of(
            Arrays.asList(
                hashDistributionAddedFiles,
                noneDistributionAddedFiles,
                hashAutoShardingDistributionAddedFiles))
        .apply(Flatten.pCollections())
        .apply("add dummy key", WithKeys.of(1))
        .apply("group together", GroupByKey.create())
        .apply(
            "validate num files",
            ParDo.of(
                new DoFn<KV<Integer, Iterable<KV<String, Integer>>>, Void>() {
                  private final Counter numWaves =
                      Metrics.counter(IcebergIOWriteTest.class, "numWaves");

                  @ProcessElement
                  public void process(@Element KV<Integer, Iterable<KV<String, Integer>>> sums) {
                    List<KV<String, Integer>> sumList =
                        Lists.newArrayList(sums.getValue().iterator());
                    // each wave should have one snapshot per write branch
                    System.out.println("list: " + sumList);
                    assertEquals(3, sumList.size());

                    // get the number of files written by each branch
                    int numFilesAddedHashDist =
                        Iterables.getOnlyElement(
                            sumList.stream()
                                .filter(kv -> kv.getKey().equals(HASH))
                                .map(KV::getValue)
                                .collect(Collectors.toList()));

                    // plain hash distribution should have exactly the same number of partitions
                    assertEquals(2, numFilesAddedHashDist);
                    // hash with autosharding may create sub-shards and lead to more than just 2
                    // files.
                    // In a production runner like Dataflow, hash + autosharding would still
                    // make less files than 'none' distribution.
                    // We're testing with DirectRunner though, which doesn't have a smart
                    // autosharding implementation, so it may sometimes make more files
                    // than even 'none' distribution.
                    // assertTrue(numFilesAddedHashAutoShardingDist < numFilesAddedNoneDist);
                    numWaves.inc();
                  }
                }));

    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    // verify total number of snapshot commit waves
    long numWaves =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(IcebergIOWriteTest.class, "numWaves"))
                    .build())
            .getCounters()
            .iterator()
            .next()
            .getCommitted();
    assertEquals(5L, numWaves);
  }
}
