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
package org.apache.beam.sdk.schemas.transforms.providers;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Logging.
 *
 * <p>Specifically, this is used by YAML's LogForTesting.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class LoggingTransformProvider
    extends TypedSchemaTransformProvider<LoggingTransformProvider.Configuration> {

  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new LoggingTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:log_for_testing:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    private static final Map<String, Level> SUPPORTED_LOG_LEVELS =
        ImmutableMap.of("ERROR", Level.ERROR, "INFO", Level.INFO, "DEBUG", Level.DEBUG);

    @Nullable
    public abstract String getLevel();

    public Level getLogLevel() {
      if (getLevel() == null) {
        return Level.INFO;
      } else if (SUPPORTED_LOG_LEVELS.containsKey(getLevel())) {
        return SUPPORTED_LOG_LEVELS.get(getLevel());
      } else {
        throw new IllegalArgumentException(
            "Unknown log level "
                + getLevel()
                + ". Valid log levels are "
                + ImmutableList.copyOf(SUPPORTED_LOG_LEVELS.keySet()));
      }
    }

    @Nullable
    public abstract String getPrefix();

    public String getNonNullPrefix() {
      String prefix = getPrefix();
      return prefix == null ? "" : prefix;
    }

    public static Builder builder() {
      return new AutoValue_LoggingTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setLevel(@Nullable String level);

      public abstract Builder setPrefix(@Nullable String prefix);

      public abstract Configuration build();
    }
  }

  /** A {@link SchemaTransform} for logging. */
  protected static class LoggingTransform extends SchemaTransform {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingTransform.class);

    private final Configuration configuration;

    LoggingTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      PCollection<Row> result =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "LogAsJson",
                  ParDo.of(
                      createDoFn(
                          configuration.getLogLevel(),
                          configuration.getNonNullPrefix(),
                          inputSchema)))
              .setRowSchema(inputSchema);
      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, result);
    }

    private static DoFn<Row, Row> createDoFn(Level logLevel, String prefix, Schema rowSchema) {
      SerializableFunction<Row, byte[]> fn = JsonUtils.getRowToJsonBytesFunction(rowSchema);
      return new DoFn<Row, Row>() {
        @ProcessElement
        public void processElement(@Element Row row, OutputReceiver<Row> out) {
          String msg = prefix + new String(fn.apply(row), StandardCharsets.UTF_8);
          // Looks like this is the best we can do.
          // https://stackoverflow.com/questions/2621701/setting-log-level-of-message-at-runtime-in-slf4j
          switch (logLevel) {
            case DEBUG:
              LOG.debug(msg);
              break;
            case INFO:
              LOG.info(msg);
              break;
            case ERROR:
            default:
              LOG.error(msg);
          }
          out.output(row);
        }
      };
    }
  }
}
