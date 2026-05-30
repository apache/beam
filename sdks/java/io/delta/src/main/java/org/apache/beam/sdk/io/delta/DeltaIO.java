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
package org.apache.beam.sdk.io.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import com.google.auto.value.AutoValue;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A connector that reads from <a href="https://delta.io/">Delta Lake</a> tables.
 *
 * <p>This is work in progress. For more details and to track progress, please see <a
 * href="https://github.com/apache/beam/issues/21100">Issue 21100</a>.
 */
@Internal
public class DeltaIO {

  public static ReadRows readRows() {
    return new AutoValue_DeltaIO_ReadRows.Builder().build();
  }

  @AutoValue
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    public abstract @Nullable String getTablePath();

    public abstract @Nullable Long getVersion();

    public abstract @Nullable String getTimestamp();

    public abstract @Nullable Map<String, String> getHadoopConfig();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTablePath(String tablePath);

      abstract Builder setVersion(@Nullable Long version);

      abstract Builder setTimestamp(@Nullable String timestamp);

      abstract Builder setHadoopConfig(@Nullable Map<String, String> hadoopConfig);

      abstract ReadRows build();
    }

    public ReadRows from(String tablePath) {
      return toBuilder().setTablePath(tablePath).build();
    }

    public ReadRows withVersion(@Nullable Long version) {
      return toBuilder().setVersion(version).build();
    }

    public ReadRows withTimestamp(@Nullable String timestamp) {
      return toBuilder().setTimestamp(timestamp).build();
    }

    public ReadRows withConfig(Map<String, String> config) {
      return toBuilder().setHadoopConfig(config).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      if (getTablePath() == null) {
        throw new IllegalArgumentException("Table path must be set.");
      }

      org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(getHadoopConfig());
      Engine engine = DefaultEngine.create(hadoopConfig);
      Table table = Table.forPath(engine, getTablePath());

      StructType tableSchema;
      try {
        Snapshot snapshot = buildSnapshot(engine, table);
        tableSchema = snapshot.getSchema(engine);
      } catch (Exception e) {
        throw new RuntimeException("Failed to load schema for Delta table: " + getTablePath(), e);
      }
      org.apache.beam.sdk.schemas.Schema beamSchema = inferBeamSchema(tableSchema);

      return input
          .apply("CreateTablePath", Create.of(getTablePath()))
          .apply("PlanScan", ParDo.of(new PlanScanFn(getHadoopConfig(), getVersion(), getTimestamp())))
          .apply("ReadFile", ParDo.of(new ReadFileFn(beamSchema)))
          .setRowSchema(beamSchema);
    }

    private Snapshot buildSnapshot(Engine engine, Table table) throws Exception {
      if (getVersion() != null) {
        return table.getSnapshotAsOfVersion(engine, getVersion());
      } else if (getTimestamp() != null) {
        long epochMillis = org.joda.time.Instant.parse(getTimestamp()).getMillis();
        return table.getSnapshotAsOfTimestamp(engine, epochMillis);
      } else {
        return table.getLatestSnapshot(engine);
      }
    }
  }

  public static class PlanScanFn extends DoFn<String, DeltaFileDescriptor> {
    private final @Nullable Map<String, String> hadoopConfig;
    private final @Nullable Long version;
    private final @Nullable String timestamp;

    public PlanScanFn(
        @Nullable Map<String, String> hadoopConfig,
        @Nullable Long version,
        @Nullable String timestamp) {
      this.hadoopConfig = hadoopConfig;
      this.version = version;
      this.timestamp = timestamp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String tablePath = c.element();
      org.apache.hadoop.conf.Configuration config = getHadoopConfiguration(hadoopConfig);
      Engine engine = DefaultEngine.create(config);
      Table table = Table.forPath(engine, tablePath);

      Snapshot snapshot;
      if (version != null) {
        snapshot = table.getSnapshotAsOfVersion(engine, version);
      } else if (timestamp != null) {
        long epochMillis = org.joda.time.Instant.parse(timestamp).getMillis();
        snapshot = table.getSnapshotAsOfTimestamp(engine, epochMillis);
      } else {
        snapshot = table.getLatestSnapshot(engine);
      }

      Scan scan = snapshot.getScanBuilder(engine).build();
      io.delta.kernel.data.Row scanState = scan.getScanState(engine);
      SerializableRow serializableScanState = new SerializableRow(scanState);

      try (CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine)) {
        while (scanFileIter.hasNext()) {
          FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
          try (CloseableIterator<io.delta.kernel.data.Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext()) {
              io.delta.kernel.data.Row scanFileRow = scanFileRows.next();
              FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
              SerializableRow serializableScanFileRow = new SerializableRow(scanFileRow);
              c.output(new DeltaFileDescriptor(
                  tablePath,
                  fileStatus.getPath(),
                  fileStatus.getSize(),
                  fileStatus.getModificationTime(),
                  hadoopConfig,
                  version,
                  timestamp,
                  serializableScanState,
                  serializableScanFileRow
              ));
            }
          }
        }
      }
    }
  }

  @DefaultCoder(SerializableCoder.class)
  public static class DeltaFileDescriptor implements Serializable {
    private final String tablePath;
    private final String filePath;
    private final long fileSize;
    private final long modificationTime;
    private final @Nullable Map<String, String> hadoopConfig;
    private final @Nullable Long version;
    private final @Nullable String timestamp;
    private final SerializableRow scanState;
    private final SerializableRow scanFileRow;

    public DeltaFileDescriptor(
        String tablePath,
        String filePath,
        long fileSize,
        long modificationTime,
        @Nullable Map<String, String> hadoopConfig,
        @Nullable Long version,
        @Nullable String timestamp,
        SerializableRow scanState,
        SerializableRow scanFileRow) {
      this.tablePath = tablePath;
      this.filePath = filePath;
      this.fileSize = fileSize;
      this.modificationTime = modificationTime;
      this.hadoopConfig = hadoopConfig;
      this.version = version;
      this.timestamp = timestamp;
      this.scanState = scanState;
      this.scanFileRow = scanFileRow;
    }

    public String getTablePath() {
      return tablePath;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public @Nullable Map<String, String> getHadoopConfig() {
      return hadoopConfig;
    }

    public @Nullable Long getVersion() {
      return version;
    }

    public @Nullable String getTimestamp() {
      return timestamp;
    }

    public SerializableRow getScanState() {
      return scanState;
    }

    public SerializableRow getScanFileRow() {
      return scanFileRow;
    }
  }

  public static class ReadFileFn extends DoFn<DeltaFileDescriptor, Row> {
    private final org.apache.beam.sdk.schemas.Schema beamSchema;

    public ReadFileFn(org.apache.beam.sdk.schemas.Schema beamSchema) {
      this.beamSchema = beamSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      DeltaFileDescriptor desc = c.element();
      org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(desc.getHadoopConfig());
      Engine engine = DefaultEngine.create(hadoopConfig);

      FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(desc.getScanFileRow());
      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(desc.getScanState());
      CloseableIterator<ColumnarBatch> physicalDataIter =
          engine.getParquetHandler().readParquetFiles(
              singletonCloseableIterator(fileStatus),
              physicalReadSchema,
              Optional.empty()).map(FilteredColumnarBatch::getData);

      try (
          CloseableIterator<FilteredColumnarBatch> transformedData =
              Scan.transformPhysicalData(
                  engine,
                  desc.getScanState(),
                  desc.getScanFileRow(),
                  physicalDataIter)) {
        while (transformedData.hasNext()) {
          FilteredColumnarBatch filteredData = transformedData.next();
          try (CloseableIterator<io.delta.kernel.data.Row> rows = filteredData.getRows()) {
            while (rows.hasNext()) {
              io.delta.kernel.data.Row row = rows.next();
              c.output(convertKernelRowToBeamRow(row, beamSchema));
            }
          }
        }
      }
    }
  }

  private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
      @Nullable Map<String, String> configMap) {
    org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    if (configMap != null) {
      for (Map.Entry<String, String> entry : configMap.entrySet()) {
        config.set(entry.getKey(), entry.getValue());
      }
    }
    return config;
  }

  private static org.apache.beam.sdk.schemas.Schema inferBeamSchema(StructType structType) {
    org.apache.beam.sdk.schemas.Schema.Builder builder = org.apache.beam.sdk.schemas.Schema.builder();
    for (StructField field : structType.fields()) {
      org.apache.beam.sdk.schemas.Schema.Field beamField =
          org.apache.beam.sdk.schemas.Schema.Field.of(field.getName(), toBeamFieldType(field.getDataType()))
              .withNullable(field.isNullable());
      builder.addField(beamField);
    }
    return builder.build();
  }

  private static org.apache.beam.sdk.schemas.Schema.FieldType toBeamFieldType(DataType dataType) {
    if (dataType instanceof IntegerType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
    } else if (dataType instanceof LongType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
    } else if (dataType instanceof StringType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
    } else if (dataType instanceof DoubleType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
    } else if (dataType instanceof FloatType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
    } else if (dataType instanceof BooleanType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
    } else if (dataType instanceof ShortType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
    } else if (dataType instanceof ByteType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
    } else if (dataType instanceof BinaryType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.BYTES;
    } else if (dataType instanceof DecimalType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL;
    } else if (dataType instanceof TimestampType || dataType instanceof DateType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
    } else if (dataType instanceof StructType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.row(inferBeamSchema((StructType) dataType));
    } else if (dataType instanceof ArrayType) {
      return org.apache.beam.sdk.schemas.Schema.FieldType.array(toBeamFieldType(((ArrayType) dataType).getElementType()));
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      return org.apache.beam.sdk.schemas.Schema.FieldType.map(
          toBeamFieldType(mapType.getKeyType()),
          toBeamFieldType(mapType.getValueType()));
    } else {
      throw new IllegalArgumentException("Unsupported Delta type: " + dataType);
    }
  }

  private static Row convertKernelRowToBeamRow(
      io.delta.kernel.data.Row deltaRow, org.apache.beam.sdk.schemas.Schema beamSchema) {
    List<Object> values = new ArrayList<>();
    StructType structType = deltaRow.getSchema();
    for (int i = 0; i < structType.length(); i++) {
      if (deltaRow.isNullAt(i)) {
        values.add(null);
      } else {
        DataType dataType = structType.at(i).getDataType();
        org.apache.beam.sdk.schemas.Schema.FieldType beamFieldType = beamSchema.getField(i).getType();
        values.add(convertValue(deltaRow, i, dataType, beamFieldType));
      }
    }
    return Row.withSchema(beamSchema).addValues(values).build();
  }

  private static Object convertValue(
      io.delta.kernel.data.Row deltaRow,
      int ordinal,
      DataType dataType,
      org.apache.beam.sdk.schemas.Schema.FieldType beamFieldType) {
    if (deltaRow.isNullAt(ordinal)) {
      return null;
    }
    if (dataType instanceof IntegerType) {
      return deltaRow.getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return deltaRow.getLong(ordinal);
    } else if (dataType instanceof StringType) {
      return deltaRow.getString(ordinal);
    } else if (dataType instanceof DoubleType) {
      return deltaRow.getDouble(ordinal);
    } else if (dataType instanceof FloatType) {
      return deltaRow.getFloat(ordinal);
    } else if (dataType instanceof BooleanType) {
      return deltaRow.getBoolean(ordinal);
    } else if (dataType instanceof ShortType) {
      return deltaRow.getShort(ordinal);
    } else if (dataType instanceof ByteType) {
      return deltaRow.getByte(ordinal);
    } else if (dataType instanceof BinaryType) {
      return deltaRow.getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      return deltaRow.getDecimal(ordinal);
    } else if (dataType instanceof TimestampType) {
      long micros = deltaRow.getLong(ordinal);
      return org.joda.time.Instant.ofEpochMilli(micros / 1000);
    } else if (dataType instanceof DateType) {
      int days = deltaRow.getInt(ordinal);
      return org.joda.time.Instant.ofEpochMilli(days * 24L * 60 * 60 * 1000);
    } else if (dataType instanceof StructType) {
      io.delta.kernel.data.Row structRow = deltaRow.getStruct(ordinal);
      return convertKernelRowToBeamRow(structRow, beamFieldType.getRowSchema());
    } else if (dataType instanceof ArrayType) {
      ArrayValue arrayVal = deltaRow.getArray(ordinal);
      int size = arrayVal.getSize();
      List<Object> list = new ArrayList<>(size);
      DataType elemType = ((ArrayType) dataType).getElementType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamCollectionElementType = beamFieldType.getCollectionElementType();
      ColumnVector vec = arrayVal.getElements();
      for (int j = 0; j < size; j++) {
        if (vec.isNullAt(j)) {
          list.add(null);
        } else {
          list.add(convertVectorValue(vec, j, elemType, beamCollectionElementType));
        }
      }
      return list;
    } else if (dataType instanceof MapType) {
      MapValue mapVal = deltaRow.getMap(ordinal);
      int size = mapVal.getSize();
      Map<Object, Object> map = new HashMap<>(size);
      DataType keyType = ((MapType) dataType).getKeyType();
      DataType valueType = ((MapType) dataType).getValueType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamMapKeyType = beamFieldType.getMapKeyType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamMapValueType = beamFieldType.getMapValueType();
      ColumnVector keysVec = mapVal.getKeys();
      ColumnVector valuesVec = mapVal.getValues();
      for (int j = 0; j < size; j++) {
        Object key = convertVectorValue(keysVec, j, keyType, beamMapKeyType);
        Object val = valuesVec.isNullAt(j) ? null : convertVectorValue(valuesVec, j, valueType, beamMapValueType);
        map.put(key, val);
      }
      return map;
    } else {
      return deltaRow.toString();
    }
  }

  private static Object convertVectorValue(
      ColumnVector vec,
      int rowId,
      DataType dataType,
      org.apache.beam.sdk.schemas.Schema.FieldType beamFieldType) {
    if (dataType instanceof IntegerType) {
      return vec.getInt(rowId);
    } else if (dataType instanceof LongType) {
      return vec.getLong(rowId);
    } else if (dataType instanceof StringType) {
      return vec.getString(rowId);
    } else if (dataType instanceof DoubleType) {
      return vec.getDouble(rowId);
    } else if (dataType instanceof FloatType) {
      return vec.getFloat(rowId);
    } else if (dataType instanceof BooleanType) {
      return vec.getBoolean(rowId);
    } else if (dataType instanceof ShortType) {
      return vec.getShort(rowId);
    } else if (dataType instanceof ByteType) {
      return vec.getByte(rowId);
    } else if (dataType instanceof BinaryType) {
      return vec.getBinary(rowId);
    } else if (dataType instanceof DecimalType) {
      return vec.getDecimal(rowId);
    } else if (dataType instanceof TimestampType) {
      long micros = vec.getLong(rowId);
      return org.joda.time.Instant.ofEpochMilli(micros / 1000);
    } else if (dataType instanceof DateType) {
      int days = vec.getInt(rowId);
      return org.joda.time.Instant.ofEpochMilli(days * 24L * 60 * 60 * 1000);
    } else if (dataType instanceof StructType) {
      io.delta.kernel.data.Row structRow = getStructFromVector(vec, rowId, (StructType) dataType);
      return convertKernelRowToBeamRow(structRow, beamFieldType.getRowSchema());
    } else if (dataType instanceof ArrayType) {
      ArrayValue arrayVal = vec.getArray(rowId);
      int size = arrayVal.getSize();
      List<Object> list = new ArrayList<>(size);
      DataType elemType = ((ArrayType) dataType).getElementType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamCollectionElementType = beamFieldType.getCollectionElementType();
      ColumnVector elementsVec = arrayVal.getElements();
      for (int j = 0; j < size; j++) {
        if (elementsVec.isNullAt(j)) {
          list.add(null);
        } else {
          list.add(convertVectorValue(elementsVec, j, elemType, beamCollectionElementType));
        }
      }
      return list;
    } else if (dataType instanceof MapType) {
      MapValue mapVal = vec.getMap(rowId);
      int size = mapVal.getSize();
      Map<Object, Object> map = new HashMap<>(size);
      DataType keyType = ((MapType) dataType).getKeyType();
      DataType valueType = ((MapType) dataType).getValueType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamMapKeyType = beamFieldType.getMapKeyType();
      org.apache.beam.sdk.schemas.Schema.FieldType beamMapValueType = beamFieldType.getMapValueType();
      ColumnVector keysVec = mapVal.getKeys();
      ColumnVector valuesVec = mapVal.getValues();
      for (int j = 0; j < size; j++) {
        Object key = convertVectorValue(keysVec, j, keyType, beamMapKeyType);
        Object val = valuesVec.isNullAt(j) ? null : convertVectorValue(valuesVec, j, valueType, beamMapValueType);
        map.put(key, val);
      }
      return map;
    } else {
      return vec.toString();
    }
  }

  private static io.delta.kernel.data.Row getStructFromVector(ColumnVector vec, int rowId, StructType structType) {
    return new io.delta.kernel.data.Row() {
      @Override
      public StructType getSchema() {
        return structType;
      }

      @Override
      public boolean isNullAt(int ordinal) {
        return vec.getChild(ordinal).isNullAt(rowId);
      }

      @Override
      public boolean getBoolean(int ordinal) {
        return vec.getChild(ordinal).getBoolean(rowId);
      }

      @Override
      public byte getByte(int ordinal) {
        return vec.getChild(ordinal).getByte(rowId);
      }

      @Override
      public short getShort(int ordinal) {
        return vec.getChild(ordinal).getShort(rowId);
      }

      @Override
      public int getInt(int ordinal) {
        return vec.getChild(ordinal).getInt(rowId);
      }

      @Override
      public long getLong(int ordinal) {
        return vec.getChild(ordinal).getLong(rowId);
      }

      @Override
      public float getFloat(int ordinal) {
        return vec.getChild(ordinal).getFloat(rowId);
      }

      @Override
      public double getDouble(int ordinal) {
        return vec.getChild(ordinal).getDouble(rowId);
      }

      @Override
      public String getString(int ordinal) {
        return vec.getChild(ordinal).getString(rowId);
      }

      @Override
      public BigDecimal getDecimal(int ordinal) {
        return vec.getChild(ordinal).getDecimal(rowId);
      }

      @Override
      public byte[] getBinary(int ordinal) {
        return vec.getChild(ordinal).getBinary(rowId);
      }

      @Override
      public io.delta.kernel.data.Row getStruct(int ordinal) {
        return getStructFromVector(vec.getChild(ordinal), rowId, (StructType) vec.getChild(ordinal).getDataType());
      }

      @Override
      public ArrayValue getArray(int ordinal) {
        return vec.getChild(ordinal).getArray(rowId);
      }

      @Override
      public MapValue getMap(int ordinal) {
        return vec.getChild(ordinal).getMap(rowId);
      }
    };
  }

  public abstract static class SerializableDataType implements Serializable {
    public abstract DataType toDataType();

    public static SerializableDataType fromDataType(DataType type) {
      if (type instanceof IntegerType) {
        return new SerializablePrimitive(0);
      } else if (type instanceof LongType) {
        return new SerializablePrimitive(1);
      } else if (type instanceof StringType) {
        return new SerializablePrimitive(2);
      } else if (type instanceof DoubleType) {
        return new SerializablePrimitive(3);
      } else if (type instanceof FloatType) {
        return new SerializablePrimitive(4);
      } else if (type instanceof BooleanType) {
        return new SerializablePrimitive(5);
      } else if (type instanceof ShortType) {
        return new SerializablePrimitive(6);
      } else if (type instanceof ByteType) {
        return new SerializablePrimitive(7);
      } else if (type instanceof BinaryType) {
        return new SerializablePrimitive(8);
      } else if (type instanceof DateType) {
        return new SerializablePrimitive(9);
      } else if (type instanceof TimestampType) {
        return new SerializablePrimitive(10);
      } else if (type instanceof DecimalType) {
        DecimalType dt = (DecimalType) type;
        return new SerializableDecimal(dt.getPrecision(), dt.getScale());
      } else if (type instanceof StructType) {
        return new SerializableStruct((StructType) type);
      } else if (type instanceof ArrayType) {
        ArrayType at = (ArrayType) type;
        return new SerializableArray(at.getElementField());
      } else if (type instanceof MapType) {
        MapType mt = (MapType) type;
        return new SerializableMap(mt.getKeyField(), mt.getValueField());
      } else {
        throw new IllegalArgumentException("Unsupported DataType: " + type);
      }
    }
  }

  public static class SerializablePrimitive extends SerializableDataType {
    private final int typeId;

    public SerializablePrimitive(int typeId) {
      this.typeId = typeId;
    }

    @Override
    public DataType toDataType() {
      switch (typeId) {
        case 0: return IntegerType.INTEGER;
        case 1: return LongType.LONG;
        case 2: return StringType.STRING;
        case 3: return DoubleType.DOUBLE;
        case 4: return FloatType.FLOAT;
        case 5: return BooleanType.BOOLEAN;
        case 6: return ShortType.SHORT;
        case 7: return ByteType.BYTE;
        case 8: return BinaryType.BINARY;
        case 9: return DateType.DATE;
        case 10: return TimestampType.TIMESTAMP;
        default: throw new IllegalStateException();
      }
    }
  }

  public static class SerializableDecimal extends SerializableDataType {
    private final int precision;
    private final int scale;

    public SerializableDecimal(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public DataType toDataType() {
      return new DecimalType(precision, scale);
    }
  }

  public static class SerializableStructField implements Serializable {
    private final String name;
    private final SerializableDataType type;
    private final boolean nullable;

    public SerializableStructField(StructField field) {
      this.name = field.getName();
      this.type = SerializableDataType.fromDataType(field.getDataType());
      this.nullable = field.isNullable();
    }

    public StructField toStructField() {
      return new StructField(name, type.toDataType(), nullable);
    }
  }

  public static class SerializableStruct extends SerializableDataType {
    private final List<SerializableStructField> fields;

    public SerializableStruct(StructType type) {
      this.fields = new ArrayList<>();
      for (StructField f : type.fields()) {
        this.fields.add(new SerializableStructField(f));
      }
    }

    @Override
    public DataType toDataType() {
      List<StructField> list = new ArrayList<>();
      for (SerializableStructField f : fields) {
        list.add(f.toStructField());
      }
      return new StructType(list);
    }
  }

  public static class SerializableArray extends SerializableDataType {
    private final SerializableStructField elementField;

    public SerializableArray(StructField elementField) {
      this.elementField = new SerializableStructField(elementField);
    }

    @Override
    public DataType toDataType() {
      return new ArrayType(elementField.toStructField());
    }
  }

  public static class SerializableMap extends SerializableDataType {
    private final SerializableStructField keyField;
    private final SerializableStructField valueField;

    public SerializableMap(StructField keyField, StructField valueField) {
      this.keyField = new SerializableStructField(keyField);
      this.valueField = new SerializableStructField(valueField);
    }

    @Override
    public DataType toDataType() {
      return new MapType(keyField.toStructField(), valueField.toStructField());
    }
  }

  public static class SerializableRow implements io.delta.kernel.data.Row, Serializable {
    private final SerializableStruct schema;
    private final List<Object> values;

    public SerializableRow(io.delta.kernel.data.Row source) {
      this.schema = new SerializableStruct(source.getSchema());
      this.values = new ArrayList<>();
      StructType structType = source.getSchema();
      for (int i = 0; i < structType.length(); i++) {
        if (source.isNullAt(i)) {
          this.values.add(null);
        } else {
          DataType type = structType.at(i).getDataType();
          this.values.add(deepCopyValue(source, i, type));
        }
      }
    }

    private static Object deepCopyValue(io.delta.kernel.data.Row source, int ordinal, DataType type) {
      if (source.isNullAt(ordinal)) {
        return null;
      }
      if (type instanceof IntegerType) {
        return source.getInt(ordinal);
      } else if (type instanceof LongType) {
        return source.getLong(ordinal);
      } else if (type instanceof StringType) {
        return source.getString(ordinal);
      } else if (type instanceof DoubleType) {
        return source.getDouble(ordinal);
      } else if (type instanceof FloatType) {
        return source.getFloat(ordinal);
      } else if (type instanceof BooleanType) {
        return source.getBoolean(ordinal);
      } else if (type instanceof ShortType) {
        return source.getShort(ordinal);
      } else if (type instanceof ByteType) {
        return source.getByte(ordinal);
      } else if (type instanceof BinaryType) {
        return source.getBinary(ordinal);
      } else if (type instanceof DecimalType) {
        return source.getDecimal(ordinal);
      } else if (type instanceof TimestampType) {
        return source.getLong(ordinal);
      } else if (type instanceof DateType) {
        return source.getInt(ordinal);
      } else if (type instanceof StructType) {
        return new SerializableRow(source.getStruct(ordinal));
      } else if (type instanceof ArrayType) {
        return new SerializableArrayValue(source.getArray(ordinal), ((ArrayType) type).getElementType());
      } else if (type instanceof MapType) {
        return new SerializableMapValue(source.getMap(ordinal), (MapType) type);
      } else {
        throw new IllegalArgumentException("Unsupported type: " + type);
      }
    }

    @Override
    public StructType getSchema() {
      return (StructType) schema.toDataType();
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return values.get(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (Boolean) values.get(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      return (Byte) values.get(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      return (Short) values.get(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      return (Integer) values.get(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return (Long) values.get(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return (Float) values.get(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return (Double) values.get(ordinal);
    }

    @Override
    public String getString(int ordinal) {
      return (String) values.get(ordinal);
    }

    @Override
    public BigDecimal getDecimal(int ordinal) {
      return (BigDecimal) values.get(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) values.get(ordinal);
    }

    @Override
    public io.delta.kernel.data.Row getStruct(int ordinal) {
      return (io.delta.kernel.data.Row) values.get(ordinal);
    }

    @Override
    public ArrayValue getArray(int ordinal) {
      return (ArrayValue) values.get(ordinal);
    }

    @Override
    public MapValue getMap(int ordinal) {
      return (MapValue) values.get(ordinal);
    }
  }

  public static class SerializableArrayValue implements ArrayValue, Serializable {
    private final SerializableDataType elementType;
    private final SerializableColumnVector elements;
    private final int size;

    public SerializableArrayValue(ArrayValue source, DataType elemType) {
      this.elementType = SerializableDataType.fromDataType(elemType);
      this.size = source.getSize();
      this.elements = new SerializableColumnVector(source.getElements(), elemType);
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public ColumnVector getElements() {
      return elements;
    }
  }

  public static class SerializableMapValue implements MapValue, Serializable {
    private final SerializableColumnVector keys;
    private final SerializableColumnVector values;
    private final int size;

    public SerializableMapValue(MapValue source, MapType mapType) {
      this.size = source.getSize();
      this.keys = new SerializableColumnVector(source.getKeys(), mapType.getKeyType());
      this.values = new SerializableColumnVector(source.getValues(), mapType.getValueType());
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public ColumnVector getKeys() {
      return keys;
    }

    @Override
    public ColumnVector getValues() {
      return values;
    }
  }

  public static class SerializableColumnVector implements ColumnVector, Serializable {
    private final SerializableDataType dataType;
    private final int size;
    private final List<Object> values;
    private final List<SerializableColumnVector> children;

    public SerializableColumnVector(ColumnVector source, DataType type) {
      this.dataType = SerializableDataType.fromDataType(type);
      this.size = source.getSize();
      this.values = new ArrayList<>();
      this.children = new ArrayList<>();

      if (type instanceof StructType) {
        StructType structType = (StructType) type;
        for (int i = 0; i < structType.length(); i++) {
          children.add(new SerializableColumnVector(source.getChild(i), structType.at(i).getDataType()));
        }
      }

      for (int i = 0; i < size; i++) {
        if (source.isNullAt(i)) {
          this.values.add(null);
        } else {
          this.values.add(deepCopyVectorValue(source, i, type));
        }
      }
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      if (children.isEmpty()) {
        throw new UnsupportedOperationException(
            "Child vectors are not available for vector of type " + getDataType());
      }
      return children.get(ordinal);
    }

    private static Object deepCopyVectorValue(ColumnVector source, int rowId, DataType type) {
      if (type instanceof IntegerType) {
        return source.getInt(rowId);
      } else if (type instanceof LongType) {
        return source.getLong(rowId);
      } else if (type instanceof StringType) {
        return source.getString(rowId);
      } else if (type instanceof DoubleType) {
        return source.getDouble(rowId);
      } else if (type instanceof FloatType) {
        return source.getFloat(rowId);
      } else if (type instanceof BooleanType) {
        return source.getBoolean(rowId);
      } else if (type instanceof ShortType) {
        return source.getShort(rowId);
      } else if (type instanceof ByteType) {
        return source.getByte(rowId);
      } else if (type instanceof BinaryType) {
        return source.getBinary(rowId);
      } else if (type instanceof DecimalType) {
        return source.getDecimal(rowId);
      } else if (type instanceof TimestampType) {
        return source.getLong(rowId);
      } else if (type instanceof DateType) {
        return source.getInt(rowId);
      } else if (type instanceof StructType) {
        return new SerializableRow(getStructFromVector(source, rowId, (StructType) type));
      } else if (type instanceof ArrayType) {
        return new SerializableArrayValue(source.getArray(rowId), ((ArrayType) type).getElementType());
      } else if (type instanceof MapType) {
        return new SerializableMapValue(source.getMap(rowId), (MapType) type);
      } else {
        throw new IllegalArgumentException("Unsupported vector type: " + type);
      }
    }

    @Override
    public DataType getDataType() {
      return dataType.toDataType();
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return values.get(rowId) == null;
    }

    @Override
    public boolean getBoolean(int rowId) {
      return (Boolean) values.get(rowId);
    }

    @Override
    public byte getByte(int rowId) {
      return (Byte) values.get(rowId);
    }

    @Override
    public short getShort(int rowId) {
      return (Short) values.get(rowId);
    }

    @Override
    public int getInt(int rowId) {
      return (Integer) values.get(rowId);
    }

    @Override
    public long getLong(int rowId) {
      return (Long) values.get(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return (Float) values.get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return (Double) values.get(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
      return (byte[]) values.get(rowId);
    }

    @Override
    public String getString(int rowId) {
      return (String) values.get(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      return (BigDecimal) values.get(rowId);
    }

    @Override
    public ArrayValue getArray(int rowId) {
      return (ArrayValue) values.get(rowId);
    }

    @Override
    public MapValue getMap(int rowId) {
      return (MapValue) values.get(rowId);
    }

    @Override
    public void close() {}
  }
}
