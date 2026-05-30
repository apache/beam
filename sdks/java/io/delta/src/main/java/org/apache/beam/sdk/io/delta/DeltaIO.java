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

      try {
        Snapshot snapshot = buildSnapshot(engine, table);
        Scan scan = snapshot.getScanBuilder(engine).build();
        StructType readSchema = scan.getSchema(engine);
        org.apache.beam.sdk.schemas.Schema beamSchema = inferBeamSchema(readSchema);

        List<DeltaFileDescriptor> fileDescriptors = buildFileDescriptors(engine, scan);

        return input
            .apply("CreateFileDescriptors", Create.of(fileDescriptors)
                .withCoder(SerializableCoder.of(DeltaFileDescriptor.class)))
            .apply("ReadFile", ParDo.of(new ReadFileFn(beamSchema)))
            .setRowSchema(beamSchema);

      } catch (Exception e) {
        throw new RuntimeException("Failed to read Delta table: " + getTablePath(), e);
      }
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

    private List<DeltaFileDescriptor> buildFileDescriptors(Engine engine, Scan scan) throws Exception {
      List<DeltaFileDescriptor> descriptors = new ArrayList<>();
      try (CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine)) {
        while (scanFileIter.hasNext()) {
          FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
          try (CloseableIterator<io.delta.kernel.data.Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext()) {
              io.delta.kernel.data.Row scanFileRow = scanFileRows.next();
              FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
              descriptors.add(new DeltaFileDescriptor(
                  getTablePath(),
                  fileStatus.getPath(),
                  fileStatus.getSize(),
                  fileStatus.getModificationTime(),
                  getHadoopConfig(),
                  getVersion(),
                  getTimestamp()
              ));
            }
          }
        }
      }
      return descriptors;
    }
  }

  public static class DeltaFileDescriptor implements Serializable {
    private final String tablePath;
    private final String filePath;
    private final long fileSize;
    private final long modificationTime;
    private final @Nullable Map<String, String> hadoopConfig;
    private final @Nullable Long version;
    private final @Nullable String timestamp;

    public DeltaFileDescriptor(
        String tablePath,
        String filePath,
        long fileSize,
        long modificationTime,
        @Nullable Map<String, String> hadoopConfig,
        @Nullable Long version,
        @Nullable String timestamp) {
      this.tablePath = tablePath;
      this.filePath = filePath;
      this.fileSize = fileSize;
      this.modificationTime = modificationTime;
      this.hadoopConfig = hadoopConfig;
      this.version = version;
      this.timestamp = timestamp;
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
      Table table = Table.forPath(engine, desc.getTablePath());

      Snapshot snapshot;
      if (desc.getVersion() != null) {
        snapshot = table.getSnapshotAsOfVersion(engine, desc.getVersion());
      } else if (desc.getTimestamp() != null) {
        long epochMillis = org.joda.time.Instant.parse(desc.getTimestamp()).getMillis();
        snapshot = table.getSnapshotAsOfTimestamp(engine, epochMillis);
      } else {
        snapshot = table.getLatestSnapshot(engine);
      }

      Scan scan = snapshot.getScanBuilder(engine).build();
      io.delta.kernel.data.Row scanState = scan.getScanState(engine);

      try (CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine)) {
        while (scanFileIter.hasNext()) {
          FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
          try (CloseableIterator<io.delta.kernel.data.Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext()) {
              io.delta.kernel.data.Row scanFileRow = scanFileRows.next();
              FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
              if (fileStatus.getPath().equals(desc.getFilePath())) {
                StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);
                CloseableIterator<ColumnarBatch> physicalDataIter =
                    engine.getParquetHandler().readParquetFiles(
                        singletonCloseableIterator(fileStatus),
                        physicalReadSchema,
                        Optional.empty()).map(FilteredColumnarBatch::getData);
                try (
                    CloseableIterator<FilteredColumnarBatch> transformedData =
                        Scan.transformPhysicalData(
                            engine,
                            scanState,
                            scanFileRow,
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
      builder.addField(field.getName(), toBeamFieldType(field.getDataType()));
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
        values.add(convertValue(deltaRow, i, dataType));
      }
    }
    return Row.withSchema(beamSchema).addValues(values).build();
  }

  private static Object convertValue(io.delta.kernel.data.Row deltaRow, int ordinal, DataType dataType) {
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
      return convertKernelRowToBeamRow(structRow, inferBeamSchema((StructType) dataType));
    } else if (dataType instanceof ArrayType) {
      ArrayValue arrayVal = deltaRow.getArray(ordinal);
      int size = arrayVal.getSize();
      List<Object> list = new ArrayList<>(size);
      DataType elemType = ((ArrayType) dataType).getElementType();
      ColumnVector vec = arrayVal.getElements();
      for (int j = 0; j < size; j++) {
        if (vec.isNullAt(j)) {
          list.add(null);
        } else {
          list.add(convertVectorValue(vec, j, elemType));
        }
      }
      return list;
    } else if (dataType instanceof MapType) {
      MapValue mapVal = deltaRow.getMap(ordinal);
      int size = mapVal.getSize();
      Map<Object, Object> map = new HashMap<>(size);
      DataType keyType = ((MapType) dataType).getKeyType();
      DataType valueType = ((MapType) dataType).getValueType();
      ColumnVector keysVec = mapVal.getKeys();
      ColumnVector valuesVec = mapVal.getValues();
      for (int j = 0; j < size; j++) {
        Object key = convertVectorValue(keysVec, j, keyType);
        Object val = valuesVec.isNullAt(j) ? null : convertVectorValue(valuesVec, j, valueType);
        map.put(key, val);
      }
      return map;
    } else {
      return deltaRow.toString();
    }
  }

  private static Object convertVectorValue(ColumnVector vec, int rowId, DataType dataType) {
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
    } else {
      return vec.toString();
    }
  }
}
