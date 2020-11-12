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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TimestampBound;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Exposes {@link SpannerIO.WriteRows} and {@link SpannerIO.ReadRows} as an external transform for
 * cross-language usage.
 */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SpannerTransformRegistrar implements ExternalTransformRegistrar {
  public static final String INSERT_URN = "beam:external:java:spanner:insert:v1";
  public static final String UPDATE_URN = "beam:external:java:spanner:update:v1";
  public static final String REPLACE_URN = "beam:external:java:spanner:replace:v1";
  public static final String INSERT_OR_UPDATE_URN =
      "beam:external:java:spanner:insert_or_update:v1";
  public static final String DELETE_URN = "beam:external:java:spanner:delete:v1";
  public static final String READ_URN = "beam:external:java:spanner:read:v1";

  @Override
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
    String instanceId;
    String databaseId;
    String projectId;
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
  }

  @Experimental(Kind.PORTABILITY)
  public static class ReadBuilder
      implements ExternalTransformBuilder<ReadBuilder.Configuration, PBegin, PCollection<Row>> {

    public static class Configuration extends CrossLanguageConfiguration {
      // TODO: BEAM-10851 Come up with something to determine schema without this explicit parameter
      private Schema schema;
      private @Nullable String sql;
      private @Nullable String table;
      private @Nullable Boolean batching;
      private @Nullable String timestampBoundMode;
      private @Nullable String readTimestamp;
      private @Nullable String timeUnit;
      private @Nullable Long exactStaleness;

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

      public void setExactStaleness(@Nullable Long exactStaleness) {
        this.exactStaleness = exactStaleness;
      }

      private @Nullable TimestampBound getTimestampBound() {
        if (timestampBoundMode == null) {
          return null;
        }

        TimestampBound.Mode mode = TimestampBound.Mode.valueOf(timestampBoundMode);
        if (mode == TimestampBound.Mode.MAX_STALENESS
            || mode == TimestampBound.Mode.EXACT_STALENESS) {
          checkArgument(
              exactStaleness != null,
              "Staleness value cannot be null when MAX_STALENESS or EXACT_STALENESS mode is selected");
          checkArgument(
              timeUnit != null,
              "Time unit cannot be null when MAX_STALENESS or EXACT_STALENESS mode is selected");
        }
        if (mode == TimestampBound.Mode.READ_TIMESTAMP
            || mode == TimestampBound.Mode.MIN_READ_TIMESTAMP) {
          checkArgument(
              readTimestamp != null,
              "Timestamp cannot be null when READ_TIMESTAMP or MIN_READ_TIMESTAMP mode is selected");
        }
        switch (mode) {
          case STRONG:
            return TimestampBound.strong();
          case MAX_STALENESS:
            return TimestampBound.ofMaxStaleness(exactStaleness, TimeUnit.valueOf(timeUnit));
          case EXACT_STALENESS:
            return TimestampBound.ofExactStaleness(exactStaleness, TimeUnit.valueOf(timeUnit));
          case READ_TIMESTAMP:
            return TimestampBound.ofReadTimestamp(Timestamp.parseTimestamp(readTimestamp));
          case MIN_READ_TIMESTAMP:
            return TimestampBound.ofMinReadTimestamp(Timestamp.parseTimestamp(readTimestamp));
          default:
            throw new RuntimeException("Unknown timestamp bound mode: " + mode);
        }
      }

      public ReadOperation getReadOperation() {
        checkArgument(
            sql == null || table == null,
            "Query and table params are mutually exclusive. Set just one of them.");
        if (sql != null) {
          return ReadOperation.create().withQuery(sql);
        }
        return ReadOperation.create().withTable(table).withColumns(schema.getFieldNames());
      }
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildExternal(Configuration configuration) {
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
      if (configuration.getTimestampBound() != null) {
        readTransform = readTransform.withTimestampBound(configuration.getTimestampBound());
      }
      if (configuration.batching != null) {
        readTransform = readTransform.withBatching(configuration.batching);
      }

      return new SpannerIO.ReadRows(readTransform, configuration.schema);
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class InsertBuilder extends WriteBuilder {
    public InsertBuilder() {
      super(Mutation.Op.INSERT);
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class UpdateBuilder extends WriteBuilder {
    public UpdateBuilder() {
      super(Mutation.Op.UPDATE);
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class InsertOrUpdateBuilder extends WriteBuilder {
    public InsertOrUpdateBuilder() {
      super(Mutation.Op.INSERT_OR_UPDATE);
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class ReplaceBuilder extends WriteBuilder {
    public ReplaceBuilder() {
      super(Mutation.Op.REPLACE);
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class DeleteBuilder extends WriteBuilder {
    public DeleteBuilder() {
      super(Mutation.Op.DELETE);
    }
  }

  @Experimental(Kind.PORTABILITY)
  private abstract static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<Row>, PDone> {

    private final Mutation.Op operation;

    WriteBuilder(Mutation.Op operation) {
      this.operation = operation;
    }

    public static class Configuration extends CrossLanguageConfiguration {
      private String table;
      private @Nullable Long maxBatchSizeBytes;
      private @Nullable Long maxNumberMutations;
      private @Nullable Long maxNumberRows;
      private @Nullable Integer groupingFactor;
      private @Nullable Duration commitDeadline;
      private @Nullable Duration maxCumulativeBackoff;

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
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
      SpannerIO.Write writeTransform =
          SpannerIO.write()
              .withProjectId(configuration.projectId)
              .withDatabaseId(configuration.databaseId)
              .withInstanceId(configuration.instanceId);

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
      return SpannerIO.WriteRows.of(writeTransform, operation, configuration.table);
    }
  }
}
