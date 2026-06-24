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

import io.delta.kernel.Scan;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Splittable DoFn that processes {@link DeltaReadTask} elements, performs logical reads, and
 * supports dynamic work rebalancing.
 */
@DoFn.BoundedPerElement
class DeltaSourceDoFn extends DoFn<DeltaReadTask, Row> {
  @Nullable Map<String, String> hadoopConfig;
  private transient @Nullable Engine engine;
  private transient @Nullable Configuration conf;

  public DeltaSourceDoFn(@Nullable Map<String, String> hadoopConfig) {
    this.hadoopConfig = hadoopConfig;
  }

  private synchronized Configuration getConfiguration() {
    Configuration localConf = conf;
    if (localConf == null) {
      localConf = new Configuration();
      if (hadoopConfig != null) {
        for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
          localConf.set(entry.getKey(), entry.getValue());
        }
      }
      conf = localConf;
    }
    return localConf;
  }

  // Returns the sizes of the row groups for a given DeltaReadTask.
  private List<Long> getRowGroupSizes(DeltaReadTask task) {
    List<Long> sizes = new ArrayList<>();
    for (List<Long> fileSizes : task.getRowGroupSizesPerFile()) {
      sizes.addAll(fileSizes);
    }
    return sizes;
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element DeltaReadTask task) {
    List<Long> rowGroupSizes = getRowGroupSizes(task);
    // Note that we use the number of row groups, `rowGroupSizes.size()`, here
    // as the upper bound not the byte size of row groups.
    return new OffsetRange(0L, rowGroupSizes.size());
  }

  @NewTracker
  public DeltaReadTaskTracker newTracker(
      @Restriction OffsetRange restriction, @Element DeltaReadTask task) {
    return new DeltaReadTaskTracker(restriction, getRowGroupSizes(task));
  }

  @Setup
  public void setUp() {
    engine = DefaultEngine.create(getConfiguration());
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element DeltaReadTask task,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws Exception {

    SerializableRow scanStateRow = task.getScanStateRow();
    StructType physicalSchema = ScanStateRow.getPhysicalDataReadSchema(scanStateRow);
    StructType logicalSchema = ScanStateRow.getLogicalSchema(scanStateRow);
    Schema beamSchema = DeltaIO.ReadRows.convertToBeamSchema(logicalSchema);

    Engine currentEngine = engine;
    if (currentEngine == null) {
      throw new IllegalArgumentException("Expected the engine to not be null");
    }

    // `BeamParquetHandler` takes a reference to the `RestrictionTracker` so that it
    // can perform `getFrom`, `getTo`, `tryClaim` requests to return the correct set
    // of row groups that map to the current restriction.
    BeamParquetHandler parquetHandler =
        new BeamParquetHandler(getConfiguration(), currentEngine.getParquetHandler(), tracker);
    BeamEngine beamEngine = new BeamEngine(currentEngine, parquetHandler);

    long currentStartRgIndex = 0L;

    // We have to go through files in the `DeltaReadTask` in order so that the
    // `RestrictionTracker`
    // can correctly handle the range of the current split.
    List<SerializableRow> scanFileRows = task.getScanFileRows();
    List<List<Long>> rowGroupSizesPerFile = task.getRowGroupSizesPerFile();
    for (int i = 0; i < scanFileRows.size(); i++) {
      if (currentStartRgIndex >= tracker.currentRestriction().getTo()) {
        // Breaking early to prevent metadata reads.
        break;
      }
      SerializableRow scanFileRow = scanFileRows.get(i);
      FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

      long fileBlocks = rowGroupSizesPerFile.get(i).size();

      try (CloseableIterator<FileReadResult> fileReadResults =
          parquetHandler.readParquetFiles(
              Utils.singletonCloseableIterator(fileStatus),
              physicalSchema,
              Optional.empty(),
              currentStartRgIndex)) {

        // Get the correct set of physical data for the current file that are within the
        // range for the current `RestrictionTracker`.
        CloseableIterator<ColumnarBatch> physicalData =
            new CloseableIterator<ColumnarBatch>() {
              @Override
              public void close() throws java.io.IOException {}

              @Override
              public boolean hasNext() {
                return fileReadResults.hasNext();
              }

              @Override
              public ColumnarBatch next() {
                return fileReadResults.next().getData();
              }
            };

        // Convert physical data to logical data.
        try (CloseableIterator<FilteredColumnarBatch> logicalBatches =
            Scan.transformPhysicalData(beamEngine, scanStateRow, scanFileRow, physicalData)) {

          while (logicalBatches.hasNext()) {
            FilteredColumnarBatch batch = logicalBatches.next();
            try (CloseableIterator<io.delta.kernel.data.Row> logicalRows = batch.getRows()) {
              while (logicalRows.hasNext()) {
                io.delta.kernel.data.Row deltaRow = logicalRows.next();
                Row beamRow = toBeamRow(deltaRow, beamSchema);
                out.output(beamRow);
              }
            }
          }
        }
      }

      // Advance the total number of blocked handled so far.
      currentStartRgIndex += fileBlocks;

      // If the tryClaim failed during processing of the current file, there's no need
      // to look at the rest of the files within the task.
      if (parquetHandler.hasClaimFailed()) {
        break;
      }
    }
    return ProcessContinuation.stop();
  }

  // Convert Delta `Row` to Beam `Row`.
  private static Row toBeamRow(io.delta.kernel.data.Row deltaRow, Schema beamSchema) {
    Row.Builder builder = Row.withSchema(beamSchema);
    StructType deltaSchema = deltaRow.getSchema();
    List<StructField> fields = deltaSchema.fields();
    for (int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      builder.addValue(getFieldValue(deltaRow, i, field.getDataType()));
    }
    return builder.build();
  }

  // Returns the value at a specific index in a given row.
  private static @Nullable Object getFieldValue(
      io.delta.kernel.data.Row row, int index, DataType type) {
    if (row.isNullAt(index)) {
      return null;
    }
    if (type instanceof BooleanType) {
      return row.getBoolean(index);
    } else if (type instanceof ByteType) {
      return (int) row.getByte(index);
    } else if (type instanceof ShortType) {
      return (int) row.getShort(index);
    } else if (type instanceof IntegerType) {
      return row.getInt(index);
    } else if (type instanceof LongType) {
      return row.getLong(index);
    } else if (type instanceof FloatType) {
      return row.getFloat(index);
    } else if (type instanceof DoubleType) {
      return row.getDouble(index);
    } else if (type instanceof StringType) {
      return row.getString(index);
    } else if (type instanceof BinaryType) {
      return row.getBinary(index);
    } else if (type instanceof TimestampType) {
      long microSeconds = row.getLong(index);
      return new org.joda.time.Instant(microSeconds / 1000L);
    } else if (type instanceof DateType) {
      int daysSinceEpoch = row.getInt(index);
      return new org.joda.time.Instant(daysSinceEpoch * 86400000L);
    } else if (type instanceof ArrayType) {
      ArrayValue arrayValue = row.getArray(index);
      int size = arrayValue.getSize();
      ColumnVector elements = arrayValue.getElements();
      DataType elementType = ((ArrayType) type).getElementType();
      List<@Nullable Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(getVectorValue(elements, i, elementType));
      }
      return list;
    } else if (type instanceof MapType) {
      MapValue mapValue = row.getMap(index);
      int size = mapValue.getSize();
      ColumnVector keys = mapValue.getKeys();
      ColumnVector values = mapValue.getValues();
      DataType keyType = ((MapType) type).getKeyType();
      DataType valueType = ((MapType) type).getValueType();
      Map<Object, @Nullable Object> map = new LinkedHashMap<>(size);
      for (int i = 0; i < size; i++) {
        Object key = getVectorValue(keys, i, keyType);
        if (key != null) {
          map.put(key, getVectorValue(values, i, valueType));
        }
      }
      return map;
    } else if (type instanceof StructType) {
      io.delta.kernel.data.Row nestedRow = row.getStruct(index);
      Schema nestedBeamSchema = DeltaIO.ReadRows.convertToBeamSchema((StructType) type);
      return toBeamRow(nestedRow, nestedBeamSchema);
    }
    throw new UnsupportedOperationException("Unsupported type: " + type.getClass());
  }

  // Returns the value at a specific index in a given column vector.
  private static @Nullable Object getVectorValue(ColumnVector vector, int index, DataType type) {
    if (vector.isNullAt(index)) {
      return null;
    }
    if (type instanceof BooleanType) {
      return vector.getBoolean(index);
    } else if (type instanceof ByteType) {
      return (int) vector.getByte(index);
    } else if (type instanceof ShortType) {
      return (int) vector.getShort(index);
    } else if (type instanceof IntegerType) {
      return vector.getInt(index);
    } else if (type instanceof LongType) {
      return vector.getLong(index);
    } else if (type instanceof FloatType) {
      return vector.getFloat(index);
    } else if (type instanceof DoubleType) {
      return vector.getDouble(index);
    } else if (type instanceof StringType) {
      return vector.getString(index);
    } else if (type instanceof BinaryType) {
      return vector.getBinary(index);
    } else if (type instanceof TimestampType) {
      long microSeconds = vector.getLong(index);
      return new org.joda.time.Instant(microSeconds / 1000L);
    } else if (type instanceof DateType) {
      // Convert days since epoch to milliseconds since epoch.
      int daysSinceEpoch = vector.getInt(index);
      return new org.joda.time.Instant(daysSinceEpoch * 24L * 60L * 60L * 1000L);
    } else if (type instanceof ArrayType) {
      ArrayValue arrayValue = vector.getArray(index);
      int size = arrayValue.getSize();
      ColumnVector elements = arrayValue.getElements();
      DataType elementType = ((ArrayType) type).getElementType();
      List<@Nullable Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(getVectorValue(elements, i, elementType));
      }
      return list;
    } else if (type instanceof MapType) {
      MapValue mapValue = vector.getMap(index);
      int size = mapValue.getSize();
      ColumnVector keys = mapValue.getKeys();
      ColumnVector values = mapValue.getValues();
      DataType keyType = ((MapType) type).getKeyType();
      DataType valueType = ((MapType) type).getValueType();
      Map<Object, @Nullable Object> map = new LinkedHashMap<>(size);
      for (int i = 0; i < size; i++) {
        Object key = getVectorValue(keys, i, keyType);
        if (key != null) {
          map.put(key, getVectorValue(values, i, valueType));
        }
      }
      return map;
    } else if (type instanceof StructType) {
      StructType structType = (StructType) type;
      int numFields = structType.fields().size();
      ColumnVector[] childVectors = new ColumnVector[numFields];
      for (int i = 0; i < numFields; i++) {
        childVectors[i] = vector.getChild(i);
      }
      io.delta.kernel.data.Row nestedRow = new VectorRow(structType, childVectors, index);
      Schema nestedBeamSchema = DeltaIO.ReadRows.convertToBeamSchema(structType);
      return toBeamRow(nestedRow, nestedBeamSchema);
    }
    throw new UnsupportedOperationException("Unsupported vector type: " + type.getClass());
  }

  // A new new Delta Row type to efficiently convert columnar data from Delta Lake
  // to Beam Rows. We use this to store columnar vectors without creating
  // additinal memory copies for all fields but return values of the specific
  // row index.
  private static class VectorRow implements io.delta.kernel.data.Row {
    private final StructType schema;
    private final ColumnVector[] fields;
    private final int rowIndex;

    VectorRow(StructType schema, ColumnVector[] fields, int rowIndex) {
      this.schema = schema;
      this.fields = fields;
      this.rowIndex = rowIndex;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public boolean isNullAt(int ord) {
      return fields[ord].isNullAt(rowIndex);
    }

    @Override
    public boolean getBoolean(int ord) {
      return fields[ord].getBoolean(rowIndex);
    }

    @Override
    public byte getByte(int ord) {
      return fields[ord].getByte(rowIndex);
    }

    @Override
    public short getShort(int ord) {
      return fields[ord].getShort(rowIndex);
    }

    @Override
    public int getInt(int ord) {
      return fields[ord].getInt(rowIndex);
    }

    @Override
    public long getLong(int ord) {
      return fields[ord].getLong(rowIndex);
    }

    @Override
    public float getFloat(int ord) {
      return fields[ord].getFloat(rowIndex);
    }

    @Override
    public double getDouble(int ord) {
      return fields[ord].getDouble(rowIndex);
    }

    @Override
    public String getString(int ord) {
      return fields[ord].getString(rowIndex);
    }

    @Override
    public byte[] getBinary(int ord) {
      return fields[ord].getBinary(rowIndex);
    }

    @Override
    public BigDecimal getDecimal(int ord) {
      return fields[ord].getDecimal(rowIndex);
    }

    @Override
    public io.delta.kernel.data.Row getStruct(int ord) {
      StructType childSchema = (StructType) schema.fields().get(ord).getDataType();
      int numFields = childSchema.fields().size();
      ColumnVector[] childFields = new ColumnVector[numFields];
      for (int j = 0; j < numFields; j++) {
        childFields[j] = fields[ord].getChild(j);
      }
      return new VectorRow(childSchema, childFields, rowIndex);
    }

    @Override
    public ArrayValue getArray(int ord) {
      return fields[ord].getArray(rowIndex);
    }

    @Override
    public MapValue getMap(int ord) {
      return fields[ord].getMap(rowIndex);
    }
  }
}
