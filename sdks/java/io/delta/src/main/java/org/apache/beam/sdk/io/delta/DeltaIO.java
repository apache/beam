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

import com.google.auto.value.AutoValue;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
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
      String path = getTablePath();
      if (path == null) {
        throw new IllegalArgumentException("Table path must be set.");
      }

      Configuration conf = new Configuration();
      Map<String, String> hadoopConfig = getHadoopConfig();
      if (hadoopConfig != null) {
        for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }
      Engine engine = DefaultEngine.create(conf);
      Table table = Table.forPath(engine, path);
      io.delta.kernel.Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType deltaSchema = snapshot.getSchema();
      if (deltaSchema == null) {
        throw new IllegalStateException("Table schema is null.");
      }
      Schema beamSchema = convertToBeamSchema(deltaSchema);

      return input
          .apply("Create Path", Create.of(path))
          .apply("Plan Files", ParDo.of(new CreateReadTasksDoFn(hadoopConfig)))
          .apply("Read Logical Data", ParDo.of(new DeltaSourceDoFn(hadoopConfig)))
          .setRowSchema(beamSchema);
    }

    static Schema convertToBeamSchema(StructType deltaSchema) {
      Schema.Builder builder = Schema.builder();
      for (StructField field : deltaSchema.fields()) {
        builder.addField(field.getName(), convertToBeamFieldType(field.getDataType()));
      }
      return builder.build();
    }

    static Schema.FieldType convertToBeamFieldType(DataType deltaType) {
      if (deltaType instanceof StringType) {
        return Schema.FieldType.STRING;
      } else if (deltaType instanceof IntegerType) {
        return Schema.FieldType.INT32;
      } else if (deltaType instanceof LongType) {
        return Schema.FieldType.INT64;
      } else if (deltaType instanceof FloatType) {
        return Schema.FieldType.FLOAT;
      } else if (deltaType instanceof DoubleType) {
        return Schema.FieldType.DOUBLE;
      } else if (deltaType instanceof BooleanType) {
        return Schema.FieldType.BOOLEAN;
      } else if (deltaType instanceof BinaryType) {
        return Schema.FieldType.BYTES;
      } else if (deltaType instanceof TimestampType) {
        return Schema.FieldType.DATETIME;
      } else if (deltaType instanceof DateType) {
        return Schema.FieldType.DATETIME;
      } else if (deltaType instanceof ArrayType) {
        DataType elementType = ((ArrayType) deltaType).getElementType();
        return Schema.FieldType.iterable(convertToBeamFieldType(elementType));
      } else if (deltaType instanceof MapType) {
        DataType keyType = ((MapType) deltaType).getKeyType();
        DataType valueType = ((MapType) deltaType).getValueType();
        return Schema.FieldType.map(
            convertToBeamFieldType(keyType), convertToBeamFieldType(valueType));
      } else if (deltaType instanceof StructType) {
        return Schema.FieldType.row(convertToBeamSchema((StructType) deltaType));
      } else {
        throw new UnsupportedOperationException("Unsupported Delta type: " + deltaType.getClass());
      }
    }
  }
}
