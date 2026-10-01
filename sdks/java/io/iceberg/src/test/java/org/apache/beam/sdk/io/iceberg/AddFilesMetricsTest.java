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

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.FIELD_ID_ERROR;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.UNKNOWN_PARTITION_ERROR;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.EPOCH_SECONDS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.FLAG;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.FULL_METRICS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.ID;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.NAME;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_INT96;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_MICROS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_MILLIS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_NANOS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.UNSIGNED;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.column;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Metrics-based partition inference, end to end, for files registered without a location prefix: a
 * file lands in the partition readers find its rows in, or goes to the error output when that
 * partition cannot be known. The bounds it reads are covered by {@link BoundAdjustmentTest}, and
 * which table column each file column is by {@link ParquetFieldIdsTest}.
 */
@RunWith(JUnit4.class)
public class AddFilesMetricsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  /** Days from the epoch to 2024-01-01. */
  private static final int EPOCH_DAY = 19723;

  private static final Schema ID_FLAG =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "flag", Types.BooleanType.get()));
  private static final Schema ID_NAME =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));
  private static final Schema ID_TS =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()));
  private static final Schema ID_TS_NS =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withZone()));
  private static final Schema ID_NAME_AGE =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get()));
  private static final Schema UNSIGNED_ONLY =
      new Schema(Types.NestedField.optional(1, "u", Types.LongType.get()));
  private static final Schema ID_U_INT =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "u", Types.IntegerType.get()));
  private static final Schema ID_FLAG_UNSIGNED =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "flag", Types.BooleanType.get()),
          Types.NestedField.optional(3, "u", Types.LongType.get()));

  private HadoopCatalog catalog;
  private TableIdentifier tableId;
  private IcebergCatalogConfig catalogConfig;
  private ParquetTestFiles files;

  @Before
  public void setup() {
    files = new ParquetTestFiles(temp.getRoot());
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    tableId = TableIdentifier.of("default", testName.getMethodName());
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
  }

  /** One Avro row {@code {id: 1}}; Avro files carry no column metrics. */
  private String writeAvro(String name) throws IOException {
    File file = new File(temp.getRoot(), name);
    org.apache.avro.Schema avro =
        org.apache.avro.SchemaBuilder.record("r").fields().requiredInt("id").endRecord();
    try (org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer =
        new org.apache.avro.file.DataFileWriter<>(
            new org.apache.avro.generic.GenericDatumWriter<>(avro))) {
      writer.create(avro, file);
      org.apache.avro.generic.GenericData.Record record =
          new org.apache.avro.generic.GenericData.Record(avro);
      record.put("id", 1);
      writer.append(record);
    }
    return file.getAbsolutePath();
  }

  // ---- metrics-based partition inference, end to end

  private PCollectionRowTuple register(String... files) {
    return pipeline
        .apply("Create Input", Create.of(Arrays.asList(files)))
        .apply(new AddFiles(catalogConfig, tableId.toString(), null, null, null, null, null, null));
  }

  private static void expectNoErrors(PCollectionRowTuple output) {
    PAssert.that(output.get("errors")).empty();
  }

  /** The only error row is an unknown partition for {@code file} that names {@code column}. */
  private static void expectUnknownPartition(
      PCollectionRowTuple output, String file, String column) {
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row error = Iterables.getOnlyElement(rows);
              String message = String.valueOf(error.getString("error"));
              assertEquals(file, error.getString("file"));
              assertTrue(message, message.startsWith(UNKNOWN_PARTITION_ERROR));
              assertTrue(message, message.contains(column));
              return null;
            });
  }

  private List<DataFile> registeredFiles() {
    Table table = catalog.loadTable(tableId);
    if (table.currentSnapshot() == null) {
      return new ArrayList<>();
    }
    return Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
  }

  private @Nullable Object onlyPartitionValue() {
    DataFile file = Iterables.getOnlyElement(registeredFiles());
    return file.partition().get(0, Object.class);
  }

  /** The only error row is for {@code file}, and its message contains {@code fragment}. */
  private static void expectError(PCollectionRowTuple output, String file, String fragment) {
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row error = Iterables.getOnlyElement(rows);
              String message = String.valueOf(error.getString("error"));
              assertEquals(file, error.getString("file"));
              assertTrue(message, message.contains(fragment));
              return null;
            });
  }

  /**
   * The registered file's rows as {@code column=value} strings, read the way an engine reads them:
   * through the table's stored name mapping. IcebergGenerics passes no mapping at all.
   */
  private List<String> readBack(String... columns) throws IOException {
    Table table = catalog.loadTable(tableId);
    Schema schema = table.schema();
    DataFile file = Iterables.getOnlyElement(registeredFiles());
    List<String> rows = new ArrayList<>();
    try (CloseableIterable<Record> records =
        Parquet.read(table.io().newInputFile(file.location()))
            .project(schema)
            .withNameMapping(
                NameMappingParser.fromJson(
                    table.properties().get(TableProperties.DEFAULT_NAME_MAPPING)))
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record record : records) {
        List<String> values = new ArrayList<>();
        for (String column : columns) {
          values.add(column + "=" + record.getField(column));
        }
        rows.add(String.join(" ", values));
      }
    }
    return rows;
  }

  @Test
  public void testAllNullBooleanColumnRegistersUnderTheNullPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file =
        files.write("nulls.parquet", true, Arrays.asList(ID, FLAG), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  @Test
  public void testAllNullStringColumnRegistersUnderTheNullPartition() throws IOException {
    catalog.createTable(
        tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build(), FULL_METRICS);
    String file =
        files.write("nulls.parquet", true, Arrays.asList(ID, NAME), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  @Test
  public void testAllNullTimestampColumnRegistersUnderTheNullDayPartition() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).day("ts").build(), FULL_METRICS);
    String file =
        files.write(
            "nulls.parquet", true, Arrays.asList(ID, TS_MICROS), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  /** A file written before the partition column existed reads as null for every row. */
  @Test
  public void testFileLackingThePartitionColumnRegistersUnderTheNullPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file = files.write("older.parquet", true, Arrays.asList(ID), row(1), row(2));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  /** Null rows belong to the null partition, so such a file spans two partitions. */
  @Test
  public void testPartitionColumnWithNullsAndValuesIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file =
        files.write("mixed.parquet", true, Arrays.asList(ID, FLAG), row(1, true), row(2, null));

    expectUnknownPartition(register(file), file, "flag");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  @Test
  public void testPartitionColumnWithoutStatisticsIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file =
        files.write("nostats.parquet", false, Arrays.asList(ID, FLAG), row(1, true), row(2, true));

    expectUnknownPartition(register(file), file, "flag");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /** Iceberg collects no bounds for INT96; such a file must not land in the null partition. */
  @Test
  public void testInt96PartitionColumnIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).day("ts").build(), FULL_METRICS);
    String file =
        files.write(
            "int96.parquet", true, Arrays.asList(ID, TS_INT96), row(1, new NanoTime(2460311, 0L)));

    expectUnknownPartition(register(file), file, "ts");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  @Test
  public void testAvroFileOnAPartitionedTableIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String parquet = files.write("good.parquet", true, Arrays.asList(ID, FLAG), row(1, true));
    String avro = writeAvro("rows.avro");

    expectUnknownPartition(register(parquet, avro), avro, "flag");
    pipeline.run().waitUntilFinish();

    DataFile registered = Iterables.getOnlyElement(registeredFiles());
    assertEquals(parquet, registered.location());
    assertEquals(true, registered.partition().get(0, Object.class));
  }

  @Test
  public void testMillisTimestampFileLandsInItsDayPartition() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).day("ts").build(), FULL_METRICS);
    String file =
        files.write(
            "millis.parquet",
            true,
            Arrays.asList(ID, TS_MILLIS),
            row(1, EPOCH_SECONDS * 1000L),
            row(2, EPOCH_SECONDS * 1000L + 1));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(EPOCH_DAY, onlyPartitionValue());
  }

  @Test
  public void testNanosFileLandsInItsDayPartitionUnderANanosColumn() throws IOException {
    catalog.createTable(
        tableId,
        ID_TS_NS,
        PartitionSpec.builderFor(ID_TS_NS).day("ts").build(),
        ImmutableMap.of("format-version", "3", "write.metadata.metrics.default", "full"));
    String file =
        files.write(
            "nanos.parquet",
            true,
            Arrays.asList(ID, TS_NANOS),
            row(1, EPOCH_SECONDS * 1_000_000_000L + 1),
            row(2, EPOCH_SECONDS * 1_000_000_000L + 1_500));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(EPOCH_DAY, onlyPartitionValue());
  }

  @Test
  public void testUnsigned32FileWithValuesFrom2To31GoesToTheErrorOutput() throws IOException {
    catalog.createTable(tableId, UNSIGNED_ONLY, PartitionSpec.unpartitioned(), FULL_METRICS);
    String file =
        files.write(
            "unsigned.parquet",
            true,
            2,
            Arrays.asList(UNSIGNED),
            row(5),
            row(10),
            row((int) 3_000_000_000L),
            row((int) 3_000_000_005L));

    expectError(register(file), file, BoundAdjustment.UNSIGNED_RANGE_ERROR);
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /**
   * Iceberg's default metrics mode, truncate(16), stores a 23-character value as two different
   * bounds, which are a range for pruning but cannot name the partition.
   */
  @Test
  public void testLongStringUnderDefaultMetricsLandsInItsIdentityPartition() throws IOException {
    catalog.createTable(
        tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build());
    String value = "customer-00000000000001";
    String file =
        files.write("long.parquet", true, Arrays.asList(ID, NAME), row(1, value), row(2, value));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(value, String.valueOf(onlyPartitionValue()));
  }

  /** The partition comes from full bounds; the registered file keeps the table's own metrics. */
  @Test
  public void testTruncatedTableMetricsStillInferTheIdentityPartition() throws IOException {
    catalog.createTable(
        tableId,
        ID_NAME,
        PartitionSpec.builderFor(ID_NAME).identity("name").build(),
        ImmutableMap.of("write.metadata.metrics.default", "truncate(4)"));
    String file =
        files.write(
            "names.parquet", true, Arrays.asList(ID, NAME), row(1, "abcdefgh"), row(2, "abcdefgh"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    DataFile registered = Iterables.getOnlyElement(registeredFiles());
    assertEquals("abcdefgh", String.valueOf(registered.partition().get(0, Object.class)));
    assertEquals(
        "abcd",
        Conversions.fromByteBuffer(Types.StringType.get(), registered.lowerBounds().get(2))
            .toString());
  }

  // ---- files that carry field ids

  /**
   * Written for another table: its email column carries id 3, the table's age. Trusting that id put
   * the file under age=779632737, the first four bytes of "a@x.com" read as an int. The refusal
   * reaches the error output.
   */
  @Test
  public void testForeignFieldIdsDoNotPlaceAFileInAPartition() throws IOException {
    catalog.createTable(
        tableId,
        ID_NAME_AGE,
        PartitionSpec.builderFor(ID_NAME_AGE).identity("age").build(),
        FULL_METRICS);
    PrimitiveType email =
        column("email", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()).withId(3);
    String file =
        files.write(
            "foreign.parquet",
            true,
            Arrays.asList(NAME.withId(1), ID.withId(2), email),
            row("alice", 1, "a@x.com"),
            row("bob", 2, "a@x.org"));

    expectError(register(file), file, FIELD_ID_ERROR);
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /**
   * Readers resolve a file without field ids through the table's stored name mapping, which keeps
   * the old name of a renamed column, so the partition is inferred through that mapping too.
   */
  @Test
  public void testFileWithoutIdsWrittenBeforeARenameLandsInItsPartition() throws IOException {
    Table table =
        catalog.createTable(
            tableId,
            ID_NAME,
            PartitionSpec.builderFor(ID_NAME).identity("name").build(),
            FULL_METRICS);
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(table.schema())))
        .commit();
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        files.write(
            "older.parquet", true, Arrays.asList(ID, NAME), row(1, "alice"), row(2, "alice"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals("alice", String.valueOf(onlyPartitionValue()));
    assertEquals(
        Arrays.asList("id=1 full_name=alice", "id=2 full_name=alice"), readBack("id", "full_name"));
  }

  /**
   * With metrics switched off for the table, the partition is inferred from metrics collected for
   * the partition columns alone; an unrelated column must not be able to fail that.
   */
  @Test
  public void testUnrelatedColumnDoesNotBreakInferenceUnderMetricsModeNone() throws IOException {
    catalog.createTable(
        tableId,
        ID_FLAG_UNSIGNED,
        PartitionSpec.builderFor(ID_FLAG_UNSIGNED).identity("flag").build(),
        ImmutableMap.of("write.metadata.metrics.default", "none"));
    String file =
        files.write(
            "unsigned.parquet",
            true,
            Arrays.asList(ID, FLAG, UNSIGNED),
            row(1, true, 0),
            row(2, true, 7));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(true, onlyPartitionValue());
  }

  // ---- a partition column's name, type or unit in the file differs from the table's

  /**
   * Iceberg looks metrics modes up by the file's column name, and a file written before a rename
   * still uses the old one, so the recollection must ask for full metrics under that name.
   */
  @Test
  public void testFileWithoutIdsWrittenBeforeARenameLandsInItsPartitionUnderDefaultMetrics()
      throws IOException {
    Table table =
        catalog.createTable(
            tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build());
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(table.schema())))
        .commit();
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        files.write(
            "older.parquet", true, Arrays.asList(ID, NAME), row(1, "alice"), row(2, "alice"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals("alice", String.valueOf(onlyPartitionValue()));
  }

  @Test
  public void testFileWithIdsWrittenBeforeARenameLandsInItsPartitionUnderDefaultMetrics()
      throws IOException {
    Table table =
        catalog.createTable(
            tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build());
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        files.write(
            "older.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(2)),
            row(1, "alice"),
            row(2, "alice"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals("alice", String.valueOf(onlyPartitionValue()));
  }

  /**
   * Full mode set under the column's new name does not reach a file written before the rename:
   * Iceberg stores that file's bounds truncated, so they must not be reused for the partition.
   */
  @Test
  public void testTruncatedBoundsOfARenamedColumnAreNotReusedForItsPartition() throws IOException {
    Table table =
        catalog.createTable(
            tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build());
    table.updateSchema().renameColumn("name", "full_name").commit();
    table.updateProperties().set("write.metadata.metrics.column.full_name", "full").commit();
    String value = "customer-00000000000001";
    String file =
        files.write(
            "older.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(2)),
            row(1, value),
            row(2, value));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(value, String.valueOf(onlyPartitionValue()));
  }

  /**
   * With metrics off for the table, the partition column is only read by the recollection; a uint32
   * file column under an int table column used to throw there and fail the pipeline.
   */
  @Test
  public void testUnsigned32UnderAnIntPartitionColumnRegistersUnderMetricsModeNone()
      throws IOException {
    catalog.createTable(
        tableId,
        ID_U_INT,
        PartitionSpec.builderFor(ID_U_INT).identity("u").build(),
        ImmutableMap.of("write.metadata.metrics.default", "none"));
    String file = files.write("u.parquet", true, Arrays.asList(ID, UNSIGNED), row(1, 7), row(2, 7));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(7, onlyPartitionValue());
  }

  /**
   * Rounding the upper bound up, as pruning needs, would put 23:59:59.999999999 in the next day.
   */
  @Test
  public void testNanosFileEndingInTheLastNanoOfADayLandsInThatDay() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).day("ts").build(), FULL_METRICS);
    long dayStartNanos = EPOCH_SECONDS * 1_000_000_000L;
    String file =
        files.write(
            "nanos.parquet",
            true,
            Arrays.asList(ID, TS_NANOS),
            row(1, dayStartNanos + 1_000_000_000L),
            row(2, dayStartNanos + 86_400_000_000_000L - 1));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(EPOCH_DAY, onlyPartitionValue());
  }

  /** A micros column cannot hold the extra nanos: the partition is the value's micros, floored. */
  @Test
  public void testSubMicroNanosValueLandsInTheIdentityPartitionOfItsMicros() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).identity("ts").build(), FULL_METRICS);
    String file =
        files.write(
            "nanos.parquet",
            true,
            Arrays.asList(ID, TS_NANOS),
            row(1, EPOCH_SECONDS * 1_000_000_000L + 1_500));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(EPOCH_SECONDS * 1_000_000L + 1, onlyPartitionValue());
  }
}
