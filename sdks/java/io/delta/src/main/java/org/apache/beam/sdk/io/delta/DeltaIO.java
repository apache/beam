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
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
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
      // TODO(https://github.com/apache/beam/issues/38551): Implement expansion for
      // Delta Lake ReadRows
      throw new UnsupportedOperationException("Not implemented yet.");
    }
  }
}
