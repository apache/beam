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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery Storage Write API jobs
 * configured via {@link BigQueryStorageWriteApiSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Experimental(Kind.SCHEMAS)
@AutoService(SchemaTransformProvider.class)
public class BigQueryStorageWriteApiSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryStorageWriteApiSchemaTransformConfiguration> {
  private static final String INPUT_TAG = "INPUT_ROWS";
  private static final String OUTPUT_FAILED_ROWS_TAG = "FAILED_ROWS";

  @Override
  protected Class<BigQueryStorageWriteApiSchemaTransformConfiguration> configurationClass() {
    return BigQueryStorageWriteApiSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(
      BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
    return new BigQueryStorageWriteApiSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("beam:transform:org.apache.beam:bigquery_storage_write:v1");
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_FAILED_ROWS_TAG);
  }

  /** Configuration for writing to BigQuery with Storage Write API. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigQueryStorageWriteApiSchemaTransformConfiguration {
    static final Map<String, CreateDisposition> CREATE_DISPOSITIONS =
        ImmutableMap.<String, CreateDisposition>builder()
            .put(CreateDisposition.CREATE_IF_NEEDED.name(), CreateDisposition.CREATE_IF_NEEDED)
            .put(CreateDisposition.CREATE_NEVER.name(), CreateDisposition.CREATE_NEVER)
            .build();

    static final Map<String, WriteDisposition> WRITE_DISPOSITIONS =
        ImmutableMap.<String, WriteDisposition>builder()
            .put(WriteDisposition.WRITE_TRUNCATE.name(), WriteDisposition.WRITE_TRUNCATE)
            .put(WriteDisposition.WRITE_EMPTY.name(), WriteDisposition.WRITE_EMPTY)
            .put(WriteDisposition.WRITE_APPEND.name(), WriteDisposition.WRITE_APPEND)
            .build();

    public void validate() {
      String invalidConfigMessage = "Invalid BigQuery Storage Write configuration: ";

      // validate output table spec
      checkArgument(
          !Strings.isNullOrEmpty(this.getOuputTable()),
          invalidConfigMessage + "Table spec for a BigQuery Write must be specified.");
      checkNotNull(BigQueryHelpers.parseTableSpec(this.getOuputTable()));

      // validate create and write dispositions
      CreateDisposition createDisposition = null;
      WriteDisposition writeDisposition = null;
      if (!Strings.isNullOrEmpty(this.getCreateDisposition())) {
        checkNotNull(
            CREATE_DISPOSITIONS.get(this.getCreateDisposition().toUpperCase()),
            invalidConfigMessage
                + "Invalid create disposition was specified. Available dispositions are: ",
            CREATE_DISPOSITIONS.keySet());
        createDisposition = CREATE_DISPOSITIONS.get(this.getCreateDisposition().toUpperCase());
      }
      if (!Strings.isNullOrEmpty(this.getWriteDisposition())) {
        checkNotNull(
            WRITE_DISPOSITIONS.get(this.getWriteDisposition().toUpperCase()),
            invalidConfigMessage
                + "Invalid write disposition was specified. Available dispositions are: ",
            WRITE_DISPOSITIONS.keySet());
        writeDisposition = WRITE_DISPOSITIONS.get(this.getWriteDisposition().toUpperCase());
      }

      // validate schema
      if (!Strings.isNullOrEmpty(this.getJsonSchema())) {
        // check if a TableSchema can be deserialized from the input schema string
        checkNotNull(BigQueryHelpers.fromJsonString(this.getJsonSchema(), TableSchema.class));
      } else if (!this.getUseBeamSchema()) {
        checkArgument(
            createDisposition == CreateDisposition.CREATE_NEVER,
            invalidConfigMessage
                + "Create disposition is CREATE_IF_NEEDED, but no schema was provided.");
      }
    }

    /**
     * Instantiates a {@link BigQueryStorageWriteApiSchemaTransformConfiguration.Builder} instance.
     */
    public static Builder builder() {
      return new AutoValue_BigQueryStorageWriteApiSchemaTransformProvider_BigQueryStorageWriteApiSchemaTransformConfiguration
          .Builder();
    }

    @Nullable
    public abstract String getOuputTable();

    @Nullable
    public abstract String getJsonSchema();

    @Nullable
    public abstract boolean getUseBeamSchema();

    @Nullable
    public abstract String getCreateDisposition();

    @Nullable
    public abstract String getWriteDisposition();

    @Nullable
    public abstract int getNumStorageWriteApiStreams();

    @Nullable
    public abstract int getNumFileShards();

    @Nullable
    public abstract int getTriggeringFrequencySeconds();

    @Nullable
    public abstract BigQueryServices getBigQueryServices();

    /** Builder for {@link BigQueryStorageWriteApiSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOuputTable(String tableSpec);

      public abstract Builder setJsonSchema(String jsonSchema);

      public abstract Builder setUseBeamSchema(boolean useBeamSchema);

      public abstract Builder setCreateDisposition(String createDisposition);

      public abstract Builder setWriteDisposition(String writeDisposition);

      public abstract Builder setNumStorageWriteApiStreams(int numStorageWriteApiStreams);

      public abstract Builder setNumFileShards(int numFileShards);

      public abstract Builder setTriggeringFrequencySeconds(int seconds);

      public abstract Builder setBigQueryServices(BigQueryServices bigQueryServices);

      /** Builds a {@link BigQueryStorageWriteApiSchemaTransformConfiguration} instance. */
      public abstract BigQueryStorageWriteApiSchemaTransformProvider
              .BigQueryStorageWriteApiSchemaTransformConfiguration
          build();
    }
  }

  private static class BigQueryStorageWriteApiSchemaTransform implements SchemaTransform {
    private final BigQueryStorageWriteApiSchemaTransformConfiguration configuration;

    BigQueryStorageWriteApiSchemaTransform(
        BigQueryStorageWriteApiSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          return null;
        }
      };
    }
  }
}
