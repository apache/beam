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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FileReadSchemaTransformConfiguration {
  public void validate() {
    Set<String> validProviders = Providers.loadProviders(FileReadSchemaTransformFormatProvider.class).keySet();
    checkArgument(
        !Strings.isNullOrEmpty(this.getFormat()) && validProviders.contains(this.getFormat()),
        "A valid file format must be specified. Please specify one of: "
            + validProviders);

    validProviders.remove("line");
    if (!this.getFormat().equals("line")) {
      checkArgument(
          !Strings.isNullOrEmpty(this.getSchema()),
          String.format(
              "A schema must be specified when reading files with %s formats. You may provide a schema string or a path to a file containing the schema.",
              validProviders));
    }

    Long terminateAfterSecondsSinceNewOutput = this.getTerminateAfterSecondsSinceNewOutput();
    Long pollIntervalMillis = this.getPollIntervalMillis();
    if (terminateAfterSecondsSinceNewOutput != null && terminateAfterSecondsSinceNewOutput > 0L) {
      checkArgument(
          pollIntervalMillis != null && pollIntervalMillis > 0L,
          "Found positive value for terminateAfterSecondsSinceNewOutput but non-positive"
              + "value for pollIntervalMillis. Please set pollIntervalMillis as well to enable"
              + "streaming.");
    }
  }

  public static Builder builder() {
    return new AutoValue_FileReadSchemaTransformConfiguration.Builder();
  }

  /**
   * The format of the file(s) to read.
   *
   * <p>Possible values are: `"lines"`, `"avro"`, `"parquet"`, `"json"`
   */
  public abstract String getFormat();

  /**
   * The filepattern used to match and read files.
   *
   * <p>May instead use an input PCollection<Row> of filepatterns.
   */
  @Nullable
  public abstract String getFilepattern();

  /**
   * The schema used by sources to deserialize data and create Beam Rows.
   *
   * <p>May be provided as a schema String or as a String path to a file that contains the schema.
   */
  @Nullable
  public abstract String getSchema();

  /**
   * The time, in milliseconds, to wait before polling for new files.
   *
   * <p>This will set the pipeline to be a streaming pipeline and will continuously watch for new
   * files.
   *
   * <p>Note: This only polls for new files. New updates to an existing file will not be watched
   * for.
   */
  @Nullable
  public abstract Long getPollIntervalMillis();

  /**
   * If no new files are found after this many seconds, this transform will cease to watch for new
   * files.
   *
   * <p>The default is to never terminate. To set this parameter, a poll interval must also be
   * provided.
   */
  @Nullable
  public abstract Long getTerminateAfterSecondsSinceNewOutput();

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFormat(String format);

    public abstract Builder setFilepattern(String filepattern);

    public abstract Builder setSchema(String schema);

    public abstract Builder setPollIntervalMillis(Long millis);

    public abstract Builder setTerminateAfterSecondsSinceNewOutput(Long seconds);

    public abstract FileReadSchemaTransformConfiguration build();
  }
}
