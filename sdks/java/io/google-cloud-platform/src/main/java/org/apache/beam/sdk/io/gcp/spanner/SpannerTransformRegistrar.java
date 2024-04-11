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
package org.apache.beam.sdk.io.gcp.spanner;

import static com.google.cloud.spanner.TimestampBound.Mode.MAX_STALENESS;
import static com.google.cloud.spanner.TimestampBound.Mode.READ_TIMESTAMP;

import com.google.auto.service.AutoService;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TimestampBound;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Exposes {@link SpannerIO.WriteRows} and {@link SpannerIO.ReadRows} as an external transform for
 * cross-language usage.
 */
@AutoService(ExternalTransformRegistrar.class)
public class SpannerTransformRegistrar implements ExternalTransformRegistrar {
  public static final String INSERT_URN = "beam:transform:org.apache.beam:spanner_insert:v1";
  public static final String UPDATE_URN = "beam:transform:org.apache.beam:spanner_update:v1";
  public static final String REPLACE_URN = "beam:transform:org.apache.beam:spanner_replace:v1";
  public static final String INSERT_OR_UPDATE_URN =
      "beam:transform:org.apache.beam:spanner_insert_or_update:v1";
  public static final String DELETE_URN = "beam:transform:org.apache.beam:spanner_delete:v1";
  public static final String READ_URN = "beam:transform:org.apache.beam:spanner_read:v1";

  @Override
  @NonNull
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.<String, ExternalTransformBuilder<?, ?, ?>>builder()
        .put(INSERT_URN, new InsertBuilder())
        .put(UPDATE_URN, new UpdateBuilder())
        .put(REPLACE_URN, new ReplaceBuilder())
        .put(INSERT_OR_UPDATE_URN, new InsertOrUpdateBuilder())
        .put(DELETE_URN, new DeleteBuilder())
        .put(READ_URN, new ReadBuilder())
        .build();
  }

  public abstract static class CrossLanguageConfiguration {
    String instanceId = "";
    String databaseId = "";
    String projectId = "";
    @Nullable String host;
    @Nullable String emulatorHost;

    public void setInstanceId(String instanceId) {
      this.instanceId = instanceId;
    }

    public void setDatabaseId(String databaseId) {
      this.databaseId = databaseId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public void setHost(@Nullable String host) {
      this.host = host;
    }

    public void setEmulatorHost(@Nullable String emulatorHost) {
      this.emulatorHost = emulatorHost;
    }

    void checkMandatoryFields() {
      if (projectId.isEmpty()) {
        throw new IllegalArgumentException("projectId can't be empty");
      }
      if (databaseId.isEmpty()) {
        throw new IllegalArgumentException("databaseId can't be empty");
      }
      if (instanceId.isEmpty()) {
        throw new IllegalArgumentException("instanceId can't be empty");
      }
    }
  }

  public static class ReadBuilder
      implements ExternalTransformBuilder<ReadBuilder.Configuration, PBegin, PCollection<Row>> {

    public static class Configuration extends CrossLanguageConfiguration {
      // TODO: https://github.com/apache/beam/issues/20415 Come up with something to determine
      // schema without this explicit parameter
      private Schema schema = Schema.builder().build();
      private @Nullable String sql;
      private @Nullable String table;
      private @Nullable Boolean batching;
      private @Nullable String timestampBoundMode;
      private @Nullable String readTimestamp;
      private @Nullable String timeUnit;
      private @Nullable Long staleness;

      public void setSql(@Nullable String sql) {
        this.sql = sql;
      }

      public void setTable(@Nullable String table) {
        this.table = table;
      }

      public void setBatching(@Nullable Boolean batching) {
        this.batching = batching;
      }

      public void setTimestampBoundMode(@Nullable String timestampBoundMode) {
        this.timestampBoundMode = timestampBoundMode;
      }

      public void setSchema(byte[] schema) throws InvalidProtocolBufferException {
        this.schema = SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(schema));
      }

      public void setReadTimestamp(@Nullable String readTimestamp) {
        this.readTimestamp = readTimestamp;
      }

      public void setTimeUnit(@Nullable String timeUnit) {
        this.timeUnit = timeUnit;
      }

      public void setStaleness(@Nullable Long staleness) {
        this.staleness = staleness;
      }

      private @Nullable TimestampBound getTimestampBound() {
        if (timestampBoundMode == null) {
          return null;
        }
        TimestampBound.Mode mode = TimestampBound.Mode.valueOf(timestampBoundMode);
        switch (mode) {
          case STRONG:
            return TimestampBound.strong();
          case MAX_STALENESS:
          case EXACT_STALENESS:
            if (staleness == null) {
              throw new NullPointerException(
                  "Staleness value cannot be empty when MAX_STALENESS or EXACT_STALENESS mode is selected");
            }
            if (timeUnit == null) {
              throw new NullPointerException(
                  "Time unit cannot be null when MAX_STALENESS or EXACT_STALENESS mode is selected");
            }
            return mode == MAX_STALENESS
                ? TimestampBound.ofMaxStaleness(staleness, TimeUnit.valueOf(timeUnit))
                : TimestampBound.ofExactStaleness(staleness, TimeUnit.valueOf(timeUnit));
          case READ_TIMESTAMP:
          case MIN_READ_TIMESTAMP:
            if (readTimestamp == null) {
              throw new NullPointerException(
                  "Timestamp cannot be null when READ_TIMESTAMP or MIN_READ_TIMESTAMP mode is selected");
            }
            return mode == READ_TIMESTAMP
                ? TimestampBound.ofReadTimestamp(Timestamp.parseTimestamp(readTimestamp))
                : TimestampBound.ofMinReadTimestamp(Timestamp.parseTimestamp(readTimestamp));
          default:
            throw new IllegalArgumentException("Unknown timestamp bound mode: " + mode);
        }
      }

      public ReadOperation getReadOperation() {
        if (sql != null && table != null) {
          throw new IllegalStateException(
              "Query and table params are mutually exclusive. Set just one of them.");
        }
        ReadOperation readOperation = ReadOperation.create();
        if (sql != null) {
          return readOperation.withQuery(sql);
        }
        if (Schema.builder().build().equals(schema)) {
          throw new IllegalArgumentException("Schema can't be empty");
        }
        if (table != null) {
          return readOperation.withTable(table).withColumns(schema.getFieldNames());
        }
        throw new IllegalStateException("Can't happen");
      }
    }

    @Override
    @NonNull
    public PTransform<PBegin, PCollection<Row>> buildExternal(
        ReadBuilder.Configuration configuration) {
      configuration.checkMandatoryFields();

      SpannerIO.Read readTransform =
          SpannerIO.read()
              .withProjectId(configuration.projectId)
              .withDatabaseId(configuration.databaseId)
              .withInstanceId(configuration.instanceId)
              .withReadOperation(configuration.getReadOperation());

      if (configuration.host != null) {
        readTransform = readTransform.withHost(configuration.host);
      }
      if (configuration.emulatorHost != null) {
        readTransform = readTransform.withEmulatorHost(configuration.emulatorHost);
      }
      @Nullable TimestampBound timestampBound = configuration.getTimestampBound();
      if (timestampBound != null) {
        readTransform = readTransform.withTimestampBound(timestampBound);
      }
      if (configuration.batching != null) {
        readTransform = readTransform.withBatching(configuration.batching);
      }

      return new SpannerIO.ReadRows(readTransform, configuration.schema);
    }
  }

  public static class InsertBuilder extends WriteBuilder {
    public InsertBuilder() {
      super(Mutation.Op.INSERT);
    }
  }

  public static class UpdateBuilder extends WriteBuilder {
    public UpdateBuilder() {
      super(Mutation.Op.UPDATE);
    }
  }

  public static class InsertOrUpdateBuilder extends WriteBuilder {
    public InsertOrUpdateBuilder() {
      super(Mutation.Op.INSERT_OR_UPDATE);
    }
  }

  public static class ReplaceBuilder extends WriteBuilder {
    public ReplaceBuilder() {
      super(Mutation.Op.REPLACE);
    }
  }

  public static class DeleteBuilder extends WriteBuilder {
    public DeleteBuilder() {
      super(Mutation.Op.DELETE);
    }
  }

  private abstract static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<Row>, PDone> {

    private final Mutation.Op operation;

    WriteBuilder(Mutation.Op operation) {
      this.operation = operation;
    }

    public static class Configuration extends CrossLanguageConfiguration {
      private String table = "";
      private @Nullable Long maxBatchSizeBytes;
      private @Nullable Long maxNumberMutations;
      private @Nullable Long maxNumberRows;
      private @Nullable Integer groupingFactor;
      private @Nullable Duration commitDeadline;
      private @Nullable Duration maxCumulativeBackoff;
      private @Nullable String failureMode;
      private Boolean highPriority = false;

      public void setTable(String table) {
        this.table = table;
      }

      public void setMaxBatchSizeBytes(@Nullable Long maxBatchSizeBytes) {
        this.maxBatchSizeBytes = maxBatchSizeBytes;
      }

      public void setMaxNumberMutations(@Nullable Long maxNumberMutations) {
        this.maxNumberMutations = maxNumberMutations;
      }

      public void setMaxNumberRows(@Nullable Long maxNumberRows) {
        this.maxNumberRows = maxNumberRows;
      }

      public void setGroupingFactor(@Nullable Long groupingFactor) {
        if (groupingFactor != null) {
          this.groupingFactor = groupingFactor.intValue();
        }
      }

      public void setCommitDeadline(@Nullable Long commitDeadline) {
        if (commitDeadline != null) {
          this.commitDeadline = Duration.standardSeconds(commitDeadline);
        }
      }

      public void setMaxCumulativeBackoff(@Nullable Long maxCumulativeBackoff) {
        if (maxCumulativeBackoff != null) {
          this.maxCumulativeBackoff = Duration.standardSeconds(maxCumulativeBackoff);
        }
      }

      public void setFailureMode(@Nullable String failureMode) {
        this.failureMode = failureMode;
      }

      public void setHighPriority(Boolean highPriority) {
        this.highPriority = highPriority;
      }
    }

    @Override
    @NonNull
    public PTransform<PCollection<Row>, PDone> buildExternal(
        WriteBuilder.Configuration configuration) {
      configuration.checkMandatoryFields();

      SpannerIO.Write writeTransform =
          SpannerIO.write()
              .withProjectId(configuration.projectId)
              .withDatabaseId(configuration.databaseId)
              .withInstanceId(configuration.instanceId);

      if (configuration.highPriority) {
        writeTransform = writeTransform.withHighPriority();
      }
      if (configuration.maxBatchSizeBytes != null) {
        writeTransform = writeTransform.withBatchSizeBytes(configuration.maxBatchSizeBytes);
      }
      if (configuration.maxNumberMutations != null) {
        writeTransform = writeTransform.withMaxNumMutations(configuration.maxNumberMutations);
      }
      if (configuration.maxNumberRows != null) {
        writeTransform = writeTransform.withMaxNumRows(configuration.maxNumberRows);
      }
      if (configuration.groupingFactor != null) {
        writeTransform = writeTransform.withGroupingFactor(configuration.groupingFactor);
      }
      if (configuration.host != null) {
        writeTransform = writeTransform.withHost(configuration.host);
      }
      if (configuration.emulatorHost != null) {
        writeTransform = writeTransform.withEmulatorHost(configuration.emulatorHost);
      }
      if (configuration.commitDeadline != null) {
        writeTransform = writeTransform.withCommitDeadline(configuration.commitDeadline);
      }
      if (configuration.maxCumulativeBackoff != null) {
        writeTransform =
            writeTransform.withMaxCumulativeBackoff(configuration.maxCumulativeBackoff);
      }
      if (configuration.failureMode != null) {
        writeTransform =
            writeTransform.withFailureMode(
                SpannerIO.FailureMode.valueOf(configuration.failureMode));
      }
      return SpannerIO.WriteRows.of(writeTransform, operation, configuration.table);
    }
  }
}
