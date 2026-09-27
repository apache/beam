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

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.UNKNOWN_PARTITION_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
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
 * Column bounds and metrics-based partition inference for files that are registered without a
 * location prefix. Files without field ids resolve through the table's name mapping, so the bounds
 * Iceberg collects must be in the unit of the table column's type, not the file's. Files with field
 * ids are read by those ids, so the ids must agree with the table.
 */
@RunWith(JUnit4.class)
public class AddFilesMetricsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  /** 2024-01-01T00:00:00Z. */
  private static final long EPOCH_SECONDS = 1704067200L;

  /** Days from the epoch to 2024-01-01. */
  private static final int EPOCH_DAY = 19723;

  private static final Map<String, String> FULL_METRICS =
      ImmutableMap.of("write.metadata.metrics.default", "full");

  private static final PrimitiveType ID = column("id", PrimitiveTypeName.INT32, null);
  private static final PrimitiveType FLAG = column("flag", PrimitiveTypeName.BOOLEAN, null);
  private static final PrimitiveType NAME =
      column("name", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
  private static final PrimitiveType TS_MICROS =
      column(
          "ts",
          PrimitiveTypeName.INT64,
          LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
  private static final PrimitiveType TS_MILLIS =
      column(
          "ts",
          PrimitiveTypeName.INT64,
          LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
  private static final PrimitiveType TS_NANOS =
      column(
          "ts", PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
  private static final PrimitiveType TS_INT96 = column("ts", PrimitiveTypeName.INT96, null);
  private static final PrimitiveType UNSIGNED =
      column("u", PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false));

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
  private static final Schema TS_NS =
      new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withZone()));
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
  private static final Schema ID_FLAG_UNSIGNED =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "flag", Types.BooleanType.get()),
          Types.NestedField.optional(3, "u", Types.LongType.get()));

  private HadoopCatalog catalog;
  private TableIdentifier tableId;
  private IcebergCatalogConfig catalogConfig;

  @Before
  public void setup() {
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    tableId = TableIdentifier.of("default", testName.getMethodName());
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
  }

  private static PrimitiveType column(
      String name, PrimitiveTypeName physical, @Nullable LogicalTypeAnnotation annotation) {
    org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> builder =
        org.apache.parquet.schema.Types.primitive(physical, Repetition.OPTIONAL);
    if (annotation != null) {
      builder = builder.as(annotation);
    }
    return builder.named(name);
  }

  private static Object[] row(@Nullable Object... values) {
    return values;
  }

  /** One row per array, one value per column; a null value leaves the column unset. */
  private String write(
      String name, boolean statistics, List<PrimitiveType> columns, Object[]... rows)
      throws IOException {
    return write(name, statistics, 0, columns, rows);
  }

  /** Cuts a row group every {@code rowsPerGroup} rows; 0 writes a single row group. */
  private String write(
      String name,
      boolean statistics,
      int rowsPerGroup,
      List<PrimitiveType> columns,
      Object[]... rows)
      throws IOException {
    MessageType type = new MessageType("root", new ArrayList<>(columns));
    File file = new File(temp.getRoot(), name);
    SimpleGroupFactory factory = new SimpleGroupFactory(type);
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
            .withType(type)
            .withStatisticsEnabled(statistics);
    if (rowsPerGroup > 0) {
      builder =
          builder
              .withRowGroupSize(1L)
              .withMinRowCountForPageSizeCheck(rowsPerGroup)
              .withMaxRowCountForPageSizeCheck(rowsPerGroup);
    }
    try (ParquetWriter<Group> writer = builder.build()) {
      for (Object[] values : rows) {
        Group group = factory.newGroup();
        for (int i = 0; i < columns.size(); i++) {
          add(group, columns.get(i).getName(), values[i]);
        }
        writer.write(group);
      }
    }
    return file.getAbsolutePath();
  }

  private static void add(Group group, String column, @Nullable Object value) {
    if (value instanceof Long) {
      group.add(column, (Long) value);
    } else if (value instanceof Integer) {
      group.add(column, (Integer) value);
    } else if (value instanceof Boolean) {
      group.add(column, (Boolean) value);
    } else if (value instanceof String) {
      group.add(column, (String) value);
    } else if (value instanceof NanoTime) {
      group.add(column, (NanoTime) value);
    }
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

  // ---- bounds in the unit of the Iceberg type

  private Metrics metricsOf(String file, Schema tableSchema) throws IOException {
    ParquetMetadata footer = ParquetFooters.read(file);
    return AddFiles.getFileMetrics(
        HadoopInputFile.fromLocation(file, new Configuration()),
        FileFormat.PARQUET,
        MetricsConfig.fromProperties(FULL_METRICS),
        MappingUtil.create(tableSchema),
        tableSchema,
        ParquetFieldIds.resolve(footer, tableSchema));
  }

  private static Schema convertedSchema(String file) throws IOException {
    return ParquetSchemaUtil.convert(ParquetFooters.read(file).getFileMetaData().getSchema());
  }

  private static @Nullable Object lower(Metrics metrics, Schema schema) {
    return decode(metrics.lowerBounds(), schema);
  }

  private static @Nullable Object upper(Metrics metrics, Schema schema) {
    return decode(metrics.upperBounds(), schema);
  }

  private static @Nullable Object decode(@Nullable Map<Integer, ByteBuffer> bounds, Schema schema) {
    Types.NestedField field = schema.columns().get(0);
    ByteBuffer bytes = bounds == null ? null : bounds.get(field.fieldId());
    return bytes == null ? null : Conversions.fromByteBuffer(field.type(), bytes);
  }

  @Test
  public void testMillisTimestampBoundsAreMicros() throws IOException {
    String file =
        write(
            "millis.parquet",
            true,
            Arrays.asList(TS_MILLIS),
            row(EPOCH_SECONDS * 1000L),
            row(EPOCH_SECONDS * 1000L + 1));
    Schema schema = convertedSchema(file);
    assertEquals(Types.TimestampType.withZone(), schema.columns().get(0).type());

    Metrics metrics = metricsOf(file, schema);

    assertEquals(EPOCH_SECONDS * 1_000_000L, lower(metrics, schema));
    assertEquals(EPOCH_SECONDS * 1_000_000L + 1_000L, upper(metrics, schema));
  }

  @Test
  public void testNanosTimestampBoundsAreMicrosAndConservative() throws IOException {
    PrimitiveType nanos =
        column(
            "ts",
            PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS));
    String file =
        write(
            "nanos.parquet",
            true,
            Arrays.asList(nanos),
            row(EPOCH_SECONDS * 1_000_000_000L + 1),
            row(EPOCH_SECONDS * 1_000_000_000L + 1_500));
    Schema schema = convertedSchema(file);
    assertEquals(Types.TimestampType.withoutZone(), schema.columns().get(0).type());

    Metrics metrics = metricsOf(file, schema);

    // lower rounds down, upper rounds up, so the bounds still contain every value
    assertEquals(EPOCH_SECONDS * 1_000_000L, lower(metrics, schema));
    assertEquals(EPOCH_SECONDS * 1_000_000L + 2, upper(metrics, schema));
  }

  @Test
  public void testMillisTimeBoundsAreMicros() throws IOException {
    PrimitiveType millisTime =
        column("t", PrimitiveTypeName.INT32, LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS));
    String file =
        write("time.parquet", true, Arrays.asList(millisTime), row(3_600_000), row(3_600_001));
    Schema schema = convertedSchema(file);
    assertEquals(Types.TimeType.get(), schema.columns().get(0).type());

    Metrics metrics = metricsOf(file, schema);

    assertEquals(3_600_000_000L, lower(metrics, schema));
    assertEquals(3_600_001_000L, upper(metrics, schema));
  }

  @Test
  public void testNanosTimeBoundsAreMicros() throws IOException {
    PrimitiveType nanosTime =
        column("t", PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(true, TimeUnit.NANOS));
    String file =
        write(
            "nanotime.parquet",
            true,
            Arrays.asList(nanosTime),
            row(3_600_000_000_001L),
            row(3_600_000_000_999L));
    Schema schema = convertedSchema(file);

    Metrics metrics = metricsOf(file, schema);

    assertEquals(3_600_000_000L, lower(metrics, schema));
    assertEquals(3_600_000_001L, upper(metrics, schema));
  }

  @Test
  public void testUnsigned32BoundsAreLongs() throws IOException {
    String file =
        write("unsigned.parquet", true, Arrays.asList(UNSIGNED), row(0), row(Integer.MAX_VALUE));
    Schema schema = convertedSchema(file);
    assertEquals(Types.LongType.get(), schema.columns().get(0).type());

    Metrics metrics = metricsOf(file, schema);

    assertEquals(0L, lower(metrics, schema));
    assertEquals((long) Integer.MAX_VALUE, upper(metrics, schema));
  }

  /** Below 2^31 signed and unsigned order agree, so folding the row groups is safe. */
  @Test
  public void testUnsigned32BoundsSpanRowGroupsBelow2To31() throws IOException {
    String file =
        write(
            "unsigned.parquet",
            true,
            2,
            Arrays.asList(UNSIGNED),
            row(5),
            row(10),
            row(2_000_000_000),
            row(2_000_000_005));
    assertEquals(2, ParquetFooters.read(file).getBlocks().size());
    Schema schema = convertedSchema(file);

    Metrics metrics = metricsOf(file, schema);

    assertEquals(5L, lower(metrics, schema));
    assertEquals(2_000_000_005L, upper(metrics, schema));
  }

  /**
   * Iceberg readers widen a uint32 value to a long as signed, so a value of 2^31 or more reads back
   * negative and no bounds can describe what they return.
   */
  @Test
  public void testUnsigned32ValueFrom2To31IsRefused() throws IOException {
    String file = write("unsigned.parquet", true, Arrays.asList(UNSIGNED), row(0), row(-1));
    Schema schema = convertedSchema(file);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> metricsOf(file, schema));
    assertTrue(e.getMessage(), e.getMessage().contains("2^31"));
  }

  /**
   * Parquet orders each row group's statistics unsigned, but Iceberg folds the row groups with a
   * signed comparison: 3000000000 reads as -1294967296, so it wins the lower bound and 10 the upper
   * one.
   */
  @Test
  public void testUnsigned32ValuesFrom2To31AcrossRowGroupsAreRefused() throws IOException {
    String file =
        write(
            "unsigned.parquet",
            true,
            2,
            Arrays.asList(UNSIGNED),
            row(5),
            row(10),
            row((int) 3_000_000_000L),
            row((int) 3_000_000_005L));
    assertEquals(2, ParquetFooters.read(file).getBlocks().size());
    Schema schema = convertedSchema(file);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> metricsOf(file, schema));
    assertTrue(e.getMessage(), e.getMessage().contains("2^31"));
  }

  @Test
  public void testMicrosTimestampBoundsAreUnchanged() throws IOException {
    String file =
        write(
            "micros.parquet",
            true,
            Arrays.asList(TS_MICROS),
            row(EPOCH_SECONDS * 1_000_000L),
            row(EPOCH_SECONDS * 1_000_000L + 7));
    Schema schema = convertedSchema(file);

    Metrics metrics = metricsOf(file, schema);

    assertEquals(EPOCH_SECONDS * 1_000_000L, lower(metrics, schema));
    assertEquals(EPOCH_SECONDS * 1_000_000L + 7, upper(metrics, schema));
  }

  @Test
  public void testNanosTimestampBoundsAreUnchangedUnderANanosColumn() throws IOException {
    String file =
        write(
            "nanos.parquet",
            true,
            Arrays.asList(TS_NANOS),
            row(EPOCH_SECONDS * 1_000_000_000L + 1),
            row(EPOCH_SECONDS * 1_000_000_000L + 1_500));

    Metrics metrics = metricsOf(file, TS_NS);

    assertEquals(EPOCH_SECONDS * 1_000_000_000L + 1, lower(metrics, TS_NS));
    assertEquals(EPOCH_SECONDS * 1_000_000_000L + 1_500, upper(metrics, TS_NS));
  }

  @Test
  public void testMillisTimestampBoundsAreNanosUnderANanosColumn() throws IOException {
    String file =
        write(
            "millis.parquet",
            true,
            Arrays.asList(TS_MILLIS),
            row(EPOCH_SECONDS * 1000L),
            row(EPOCH_SECONDS * 1000L + 1));

    Metrics metrics = metricsOf(file, TS_NS);

    assertEquals(EPOCH_SECONDS * 1_000_000_000L, lower(metrics, TS_NS));
    assertEquals(EPOCH_SECONDS * 1_000_000_000L + 1_000_000L, upper(metrics, TS_NS));
  }

  @Test
  public void testMicrosTimestampBoundsAreNanosUnderANanosColumn() throws IOException {
    String file =
        write(
            "micros.parquet",
            true,
            Arrays.asList(TS_MICROS),
            row(EPOCH_SECONDS * 1_000_000L),
            row(EPOCH_SECONDS * 1_000_000L + 7));

    Metrics metrics = metricsOf(file, TS_NS);

    assertEquals(EPOCH_SECONDS * 1_000_000_000L, lower(metrics, TS_NS));
    assertEquals(EPOCH_SECONDS * 1_000_000_000L + 7_000L, upper(metrics, TS_NS));
  }

  @Test
  public void testBoundBeyondTheNanosRangeIsDropped() throws IOException {
    // 9999-12-31T23:59:59Z; nanos since 1970 overflow a long after 2262
    long lastSecondMillis = 253402300799000L;
    String file =
        write(
            "far.parquet",
            true,
            Arrays.asList(TS_MILLIS),
            row(EPOCH_SECONDS * 1000L),
            row(lastSecondMillis));

    Metrics metrics = metricsOf(file, TS_NS);

    assertEquals(EPOCH_SECONDS * 1_000_000_000L, lower(metrics, TS_NS));
    assertNull(upper(metrics, TS_NS));
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
   * The registered file's rows as {@code column=value} strings, read the way an engine that honors
   * the table's name mapping reads them; IcebergGenerics passes no mapping at all.
   */
  private List<String> readBack(String... columns) throws IOException {
    Table table = catalog.loadTable(tableId);
    Schema schema = table.schema();
    DataFile file = Iterables.getOnlyElement(registeredFiles());
    List<String> rows = new ArrayList<>();
    try (CloseableIterable<Record> records =
        Parquet.read(table.io().newInputFile(file.location()))
            .project(schema)
            .withNameMapping(MappingUtil.create(schema))
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
    String file = write("nulls.parquet", true, Arrays.asList(ID, FLAG), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  @Test
  public void testAllNullStringColumnRegistersUnderTheNullPartition() throws IOException {
    catalog.createTable(
        tableId, ID_NAME, PartitionSpec.builderFor(ID_NAME).identity("name").build(), FULL_METRICS);
    String file = write("nulls.parquet", true, Arrays.asList(ID, NAME), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  @Test
  public void testAllNullTimestampColumnRegistersUnderTheNullDayPartition() throws IOException {
    catalog.createTable(
        tableId, ID_TS, PartitionSpec.builderFor(ID_TS).day("ts").build(), FULL_METRICS);
    String file =
        write("nulls.parquet", true, Arrays.asList(ID, TS_MICROS), row(1, null), row(2, null));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  /** A file written before the partition column existed reads as null for every row. */
  @Test
  public void testFileLackingThePartitionColumnRegistersUnderTheNullPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file = write("older.parquet", true, Arrays.asList(ID), row(1), row(2));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertNull(onlyPartitionValue());
  }

  /** Null rows belong to the null partition, so such a file spans two partitions. */
  @Test
  public void testPartitionColumnWithNullsAndValuesIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file = write("mixed.parquet", true, Arrays.asList(ID, FLAG), row(1, true), row(2, null));

    expectUnknownPartition(register(file), file, "flag");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  @Test
  public void testPartitionColumnWithoutStatisticsIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String file =
        write("nostats.parquet", false, Arrays.asList(ID, FLAG), row(1, true), row(2, true));

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
        write(
            "int96.parquet", true, Arrays.asList(ID, TS_INT96), row(1, new NanoTime(2460311, 0L)));

    expectUnknownPartition(register(file), file, "ts");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  @Test
  public void testAvroFileOnAPartitionedTableIsAnUnknownPartition() throws IOException {
    catalog.createTable(
        tableId, ID_FLAG, PartitionSpec.builderFor(ID_FLAG).identity("flag").build(), FULL_METRICS);
    String parquet = write("good.parquet", true, Arrays.asList(ID, FLAG), row(1, true));
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
        write(
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
        write(
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
        write(
            "unsigned.parquet",
            true,
            2,
            Arrays.asList(UNSIGNED),
            row(5),
            row(10),
            row((int) 3_000_000_000L),
            row((int) 3_000_000_005L));

    expectError(register(file), file, "2^31");
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
        write("long.parquet", true, Arrays.asList(ID, NAME), row(1, value), row(2, value));

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
        write(
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

  /** Ids that agree with the table are what Iceberg's own writers produce for it. */
  @Test
  public void testFileWithTheTablesFieldIdsRegisters() throws IOException {
    catalog.createTable(tableId, ID_NAME, PartitionSpec.unpartitioned(), FULL_METRICS);
    PrimitiveType extra =
        column("email", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()).withId(50);
    String file =
        write(
            "own.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(2), extra),
            row(1, "a", "a@x.com"),
            row(2, "b", "b@x.com"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    DataFile registered = Iterables.getOnlyElement(registeredFiles());
    assertEquals(
        1,
        (int) Conversions.fromByteBuffer(Types.IntegerType.get(), registered.lowerBounds().get(1)));
    assertEquals(
        "a",
        Conversions.fromByteBuffer(Types.StringType.get(), registered.lowerBounds().get(2))
            .toString());
    assertEquals(Arrays.asList("id=1 name=a", "id=2 name=b"), readBack("id", "name"));
  }

  /** Readers use a file's own ids, so a name mapping cannot correct swapped ones. */
  @Test
  public void testFileWithSwappedFieldIdsIsRefused() throws IOException {
    catalog.createTable(tableId, ID_NAME, PartitionSpec.unpartitioned(), FULL_METRICS);
    String file =
        write(
            "swapped.parquet",
            true,
            Arrays.asList(NAME.withId(1), ID.withId(2)),
            row("a", 1),
            row("b", 2));

    expectError(register(file), file, "field id");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /** A file naming a table column under another id would read that column as null. */
  @Test
  public void testFileNamingATableColumnWithAnotherFieldIdIsRefused() throws IOException {
    catalog.createTable(tableId, ID_NAME, PartitionSpec.unpartitioned(), FULL_METRICS);
    String file =
        write(
            "renumbered.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(9)),
            row(1, "a"),
            row(2, "b"));

    expectError(register(file), file, "field id");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /**
   * Written for another table: its email column carries id 3, the table's age. Trusting that id put
   * the file under age=779632737, the first four bytes of "a@x.com" read as an int.
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
        write(
            "foreign.parquet",
            true,
            Arrays.asList(NAME.withId(1), ID.withId(2), email),
            row("alice", 1, "a@x.com"),
            row("bob", 2, "a@x.org"));

    expectError(register(file), file, "field id");
    pipeline.run().waitUntilFinish();

    assertEquals(Collections.emptyList(), registeredFiles());
  }

  /** A rename keeps the column's id, so files written before it stay valid. */
  @Test
  public void testFileWrittenBeforeARenameRegisters() throws IOException {
    Table table =
        catalog.createTable(tableId, ID_NAME, PartitionSpec.unpartitioned(), FULL_METRICS);
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        write(
            "older.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(2)),
            row(1, "a"),
            row(2, "b"));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(
        Arrays.asList("id=1 full_name=a", "id=2 full_name=b"), readBack("id", "full_name"));
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
        write(
            "unsigned.parquet",
            true,
            Arrays.asList(ID, FLAG, UNSIGNED),
            row(1, true, 0),
            row(2, true, 7));

    expectNoErrors(register(file));
    pipeline.run().waitUntilFinish();

    assertEquals(true, onlyPartitionValue());
  }
}
