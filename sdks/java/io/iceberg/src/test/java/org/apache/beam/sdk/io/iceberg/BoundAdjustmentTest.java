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

import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.EPOCH_SECONDS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.FULL_METRICS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_MICROS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_MILLIS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.TS_NANOS;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.UNSIGNED;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.column;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Parquet stores a column's bounds in the file's unit; readers and partition inference decode them
 * with the table column's type. Each test writes a file and checks the bounds Iceberg would store.
 */
@RunWith(JUnit4.class)
public class BoundAdjustmentTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema TS_NS =
      new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withZone()));

  private ParquetTestFiles files;

  @Before
  public void setup() {
    files = new ParquetTestFiles(temp.getRoot());
  }

  private static Metrics metricsOf(String file, Schema tableSchema) throws IOException {
    return BoundAdjustment.footerMetrics(
        ParquetFieldIds.resolve(ParquetFooters.read(file), tableSchema),
        tableSchema,
        MetricsConfig.fromProperties(FULL_METRICS),
        MappingUtil.create(tableSchema));
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
        files.write(
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
        files.write(
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
        files.write(
            "time.parquet", true, Arrays.asList(millisTime), row(3_600_000), row(3_600_001));
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
        files.write(
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
        files.write(
            "unsigned.parquet", true, Arrays.asList(UNSIGNED), row(0), row(Integer.MAX_VALUE));
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
        files.write(
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
   * The raw int -1 is 4294967295 as a uint32, and Iceberg readers would return it as -1, so the
   * file is refused.
   */
  @Test
  public void testUnsigned32ValueFrom2To31IsRefused() throws IOException {
    String file = files.write("unsigned.parquet", true, Arrays.asList(UNSIGNED), row(0), row(-1));
    Schema schema = convertedSchema(file);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> metricsOf(file, schema));
    assertTrue(e.getMessage(), e.getMessage().startsWith(BoundAdjustment.UNSIGNED_RANGE_ERROR));
  }

  /** Every row group is checked: only the second one holds values of 2^31 or more. */
  @Test
  public void testUnsigned32ValuesFrom2To31AcrossRowGroupsAreRefused() throws IOException {
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
    assertEquals(2, ParquetFooters.read(file).getBlocks().size());
    Schema schema = convertedSchema(file);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> metricsOf(file, schema));
    assertTrue(e.getMessage(), e.getMessage().startsWith(BoundAdjustment.UNSIGNED_RANGE_ERROR));
  }

  @Test
  public void testMicrosTimestampBoundsAreUnchanged() throws IOException {
    String file =
        files.write(
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
        files.write(
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
        files.write(
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
        files.write(
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
        files.write(
            "far.parquet",
            true,
            Arrays.asList(TS_MILLIS),
            row(EPOCH_SECONDS * 1000L),
            row(lastSecondMillis));

    Metrics metrics = metricsOf(file, TS_NS);

    assertEquals(EPOCH_SECONDS * 1_000_000_000L, lower(metrics, TS_NS));
    assertNull(upper(metrics, TS_NS));
  }
}
