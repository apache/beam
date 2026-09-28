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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Iceberg collects bounds in the file column's unit and width, but readers decode them with the
 * table column's type: a millis or nanos timestamp under a micros column, or a millis or micros one
 * under a nanos column, would be off by a factor of 1000 or more, and an unsigned 32-bit int under
 * a long column throws when the int is cast to a long. Bounds are what partition inference and
 * query pruning read, so they are rewritten in the table column's unit: the affected INT32 columns
 * are presented to Iceberg without their annotation (so it computes plain int bounds instead of
 * throwing) and every affected bound is converted afterwards, or dropped when it does not fit the
 * table's unit. Value counts and null counts are unaffected. A uint32 column must stay below 2^31:
 * see {@link #checkUnsignedRange}.
 */
enum BoundAdjustment {
  /** Millis stored under a micros type: times 1000. */
  MILLIS_TO_MICROS,
  /** Millis stored under a nanos type: times 1,000,000. */
  MILLIS_TO_NANOS,
  /** Micros stored under a nanos type: times 1000. */
  MICROS_TO_NANOS,
  /** Nanos stored under a micros type: divided by 1000, lower rounded down, upper rounded up. */
  NANOS_TO_MICROS,
  /** Unsigned 32-bit int stored under a long. */
  UINT32_TO_LONG;

  static final String UNSIGNED_RANGE_ERROR =
      "Iceberg readers return unsigned 32-bit values of 2^31 or more as negative numbers, and"
          + " this column holds one: ";

  /**
   * Iceberg's metrics for the file, with every bound in the table column's unit. Throws for a
   * uint32 column that holds a value of 2^31 or more.
   */
  static Metrics footerMetrics(
      ParquetFieldIds.Resolved resolved,
      Schema tableSchema,
      MetricsConfig config,
      NameMapping mapping) {
    ParquetMetadata footer = resolved.footer();
    Map<Integer, BoundAdjustment> adjustments =
        forSchema(footer.getFileMetaData().getSchema(), tableSchema);
    checkUnsignedRange(footer, adjustments);
    if (adjustments.isEmpty()) {
      return ParquetUtil.footerMetrics(footer, Stream.empty(), config, mapping);
    }
    Metrics raw =
        ParquetUtil.footerMetrics(
            withNeutralTypes(footer, adjustments), Stream.empty(), config, mapping);
    return apply(raw, adjustments);
  }

  static Map<Integer, BoundAdjustment> forSchema(MessageType fileSchema, Schema tableSchema) {
    Map<Integer, BoundAdjustment> adjustments = new HashMap<>();
    for (ColumnDescriptor column : fileSchema.getColumns()) {
      PrimitiveType primitive = column.getPrimitiveType();
      Type.ID id = primitive.getId();
      if (id == null) {
        continue;
      }
      org.apache.iceberg.types.@Nullable Type tableType = tableSchema.findType(id.intValue());
      if (tableType == null) {
        continue;
      }
      @Nullable BoundAdjustment adjustment = forPrimitive(primitive, tableType.typeId());
      if (adjustment != null) {
        adjustments.put(id.intValue(), adjustment);
      }
    }
    return adjustments;
  }

  /**
   * The conversion that puts this file column's bounds into the table column's unit. There are
   * three cases:
   *
   * <ul>
   *   <li>a millis or nanos timestamp under a micros timestamp column, or a millis or micros one
   *       under a nanos column;
   *   <li>a millis or nanos time under a time column;
   *   <li>an unsigned 32-bit int under a long column.
   * </ul>
   *
   * <p>Null for anything else, including units that already match: those bounds stay as Iceberg
   * computes them.
   */
  private static @Nullable BoundAdjustment forPrimitive(PrimitiveType primitive, TypeID tableType) {
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation instanceof TimestampLogicalTypeAnnotation) {
      TimeUnit fileUnit = ((TimestampLogicalTypeAnnotation) annotation).getUnit();
      if (tableType == TypeID.TIMESTAMP) {
        return toMicros(fileUnit);
      }
      if (tableType == TypeID.TIMESTAMP_NANO) {
        return toNanos(fileUnit);
      }
      return null;
    }
    if (annotation instanceof TimeLogicalTypeAnnotation) {
      if (tableType != TypeID.TIME) {
        return null;
      }
      return toMicros(((TimeLogicalTypeAnnotation) annotation).getUnit());
    }
    if (annotation instanceof IntLogicalTypeAnnotation) {
      IntLogicalTypeAnnotation intType = (IntLogicalTypeAnnotation) annotation;
      if (intType.getBitWidth() == 32 && !intType.isSigned() && tableType == TypeID.LONG) {
        return UINT32_TO_LONG;
      }
    }
    return null;
  }

  private static @Nullable BoundAdjustment toMicros(TimeUnit fileUnit) {
    switch (fileUnit) {
      case MILLIS:
        return MILLIS_TO_MICROS;
      case NANOS:
        return NANOS_TO_MICROS;
      default:
        return null;
    }
  }

  private static @Nullable BoundAdjustment toNanos(TimeUnit fileUnit) {
    switch (fileUnit) {
      case MILLIS:
        return MILLIS_TO_NANOS;
      case MICROS:
        return MICROS_TO_NANOS;
      default:
        return null;
    }
  }

  /**
   * Refuses a file whose uint32 column holds a value of 2^31 or more. Iceberg readers return such a
   * value as a negative number (3000000000 reads back as -1294967296), so the file would read back
   * wrong whatever bounds are stored for it. Below 2^31 a value reads back unchanged, and the
   * bounds Iceberg computes by comparing values as signed ints are correct.
   */
  static void checkUnsignedRange(
      ParquetMetadata footer, Map<Integer, BoundAdjustment> adjustments) {
    Set<ColumnPath> unsigned = new HashSet<>();
    for (ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
      Type.ID id = column.getPrimitiveType().getId();
      if (id != null && adjustments.get(id.intValue()) == UINT32_TO_LONG) {
        unsigned.add(ColumnPath.get(column.getPath()));
      }
    }
    if (unsigned.isEmpty()) {
      return;
    }
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData chunk : block.getColumns()) {
        if (unsigned.contains(chunk.getPath()) && maxIsNegative(chunk.getStatistics())) {
          throw new IllegalArgumentException(UNSIGNED_RANGE_ERROR + chunk.getPath().toDotString());
        }
      }
    }
  }

  /** A uint32 max that is negative as a signed int is 2^31 or more. */
  private static boolean maxIsNegative(@Nullable Statistics<?> stats) {
    if (stats == null || !stats.hasNonNullValue()) {
      return false;
    }
    Object max = stats.genericGetMax();
    return max instanceof Integer && (Integer) max < 0;
  }

  /** The footer with annotations removed from adjusted INT32 columns. */
  static ParquetMetadata withNeutralTypes(
      ParquetMetadata footer, Map<Integer, BoundAdjustment> adjustments) {
    MessageType neutral =
        (MessageType)
            footer.getFileMetaData().getSchema().convertWith(new WithoutAnnotations(adjustments));
    FileMetaData meta = footer.getFileMetaData();
    return new ParquetMetadata(
        new FileMetaData(neutral, meta.getKeyValueMetaData(), meta.getCreatedBy()),
        footer.getBlocks());
  }

  /** Rebuilds a schema with the annotation removed from each adjusted INT32 column. */
  private static final class WithoutAnnotations implements TypeConverter<Type> {
    private final Map<Integer, BoundAdjustment> adjustments;

    WithoutAnnotations(Map<Integer, BoundAdjustment> adjustments) {
      this.adjustments = adjustments;
    }

    @Override
    public Type convertPrimitiveType(List<GroupType> path, PrimitiveType primitive) {
      Type.ID id = primitive.getId();
      if (id == null
          || !adjustments.containsKey(id.intValue())
          || primitive.getPrimitiveTypeName() != PrimitiveTypeName.INT32) {
        return primitive;
      }
      return org.apache.parquet.schema.Types.primitive(
              primitive.getPrimitiveTypeName(), primitive.getRepetition())
          .id(id.intValue())
          .named(primitive.getName());
    }

    @Override
    public Type convertGroupType(List<GroupType> path, GroupType group, List<Type> children) {
      return group.withNewFields(children);
    }

    @Override
    public Type convertMessageType(MessageType message, List<Type> children) {
      return new MessageType(message.getName(), children);
    }
  }

  static Metrics apply(Metrics metrics, Map<Integer, BoundAdjustment> adjustments) {
    Map<Integer, ByteBuffer> lower = metrics.lowerBounds();
    Map<Integer, ByteBuffer> upper = metrics.upperBounds();
    if (lower == null || upper == null) {
      return metrics;
    }
    return new Metrics(
        metrics.recordCount(),
        metrics.columnSizes(),
        metrics.valueCounts(),
        metrics.nullValueCounts(),
        metrics.nanValueCounts(),
        adjust(lower, adjustments, false),
        adjust(upper, adjustments, true));
  }

  private static Map<Integer, ByteBuffer> adjust(
      Map<Integer, ByteBuffer> bounds, Map<Integer, BoundAdjustment> adjustments, boolean upper) {
    Map<Integer, ByteBuffer> adjusted = new HashMap<>(bounds);
    for (Map.Entry<Integer, BoundAdjustment> entry : adjustments.entrySet()) {
      ByteBuffer bytes = bounds.get(entry.getKey());
      if (bytes == null) {
        continue;
      }
      try {
        long value = entry.getValue().convert(bytes, upper);
        adjusted.put(entry.getKey(), Conversions.toByteBuffer(Types.LongType.get(), value));
      } catch (ArithmeticException e) {
        // Beyond the table unit's range (e.g. year 9999 in nanos): a missing bound is safe, a
        // wrapped one is not.
        adjusted.remove(entry.getKey());
      }
    }
    return adjusted;
  }

  private long convert(ByteBuffer bytes, boolean upper) {
    ByteBuffer little = bytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    switch (this) {
      case UINT32_TO_LONG:
        return Integer.toUnsignedLong(little.getInt(little.position()));
      case MILLIS_TO_MICROS:
        return Math.multiplyExact(readLong(little), 1000L);
      case MILLIS_TO_NANOS:
        return Math.multiplyExact(readLong(little), 1_000_000L);
      case MICROS_TO_NANOS:
        return Math.multiplyExact(readLong(little), 1000L);
      case NANOS_TO_MICROS:
        long nanos = little.getLong(little.position());
        return upper ? -Math.floorDiv(-nanos, 1000L) : Math.floorDiv(nanos, 1000L);
      default:
        throw new IllegalStateException(name());
    }
  }

  /** A millis TIME bound has 4 bytes: its INT32 column is presented without the annotation. */
  private static long readLong(ByteBuffer little) {
    if (little.remaining() == 4) {
      return little.getInt(little.position());
    }
    return little.getLong(little.position());
  }
}
