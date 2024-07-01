package main.java.org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.StructUtils;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Read;
import org.apache.beam.sdk.io.gcp.spanner.SpannerReadResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteSchemaTransformProvider;

@AutoService(SchemaTransformProvider.class)
public class SpannerReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerReadSchemaTransformProvider.SpannerReadSchemaTransformConfiguration> {

  static class SpannerSchemaTransformRead extends SchemaTransform implements Serializable {
    private final SpannerReadSchemaTransformConfiguration configuration;

    SpannerSchemaTransformRead(SpannerReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkNotNull(input, "Input to SpannerReadSchemaTransform cannot be null.");
      PCollection<Struct> spannerRows = null; 

      if (!Strings.isNullOrEmpty(configuration.getSqlQuery())) {
          spannerRows = input.getPipeline().apply(
            SpannerIO.read()
            .withInstanceId(configuration.getInstanceId())
            .withDatabaseId(configuration.getDatabaseId())
            .withQuery(configuration.getQuery())
            );
      } 
      else {
        spannerRows = input.getPipeline().apply(
          SpannerIO.read()
          .withInstanceId(configuration.getInstanceId())
          .withDatabaseId(configuration.getDatabaseId())
          .withTable(configuration.getTable())
          );
      }

      PCollection<Row> rows = spannerRows.apply(MapElements.into(TypeDescriptor.of(Row.class))
          .via((Struct struct) -> structToBeamRow(struct, inputSchema)));

      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, rows);
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_read:v1";
  }

  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SpannerReadSchemaTransformConfiguration implements Serializable {
    
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setDatabaseId(String databaseId);

      public abstract Builder setTableId(String tableId);

      public abstract Builder setQuery(String query);

      public abstract SpannerReadSchemaTransformConfiguration build();
    }

    public void validate() {
      String invalidConfigMessage = "Invalid Cloud Spanner Read configuration: ";
      if (!Strings.isNullOrEmpty(this.getQuery())) {
        checkNotNull(this.getInstanceId(), invalidConfigMessage + "Instance ID must be specified for SQL query.");
        checkNotNull(this.getDatabaseId(), invalidConfigMessage + "Database ID must be specified for SQL query.");
      } 
      else {
        checkNotNull(this.getTableId(), invalidConfigMessage + "Table name must be specified.");
        checkNotNull(this.getInstanceId(), invalidConfigMessage + "Instance ID must be specified for table read.");
        checkNotNull(this.getDatabaseId(), invalidConfigMessage + "Database ID must be specified for table read.");
      }
    }

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    public abstract String getDatabaseId();

    @Nullable
    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    public abstract String getTableId();

    @Nullable
    @SchemaFieldDescription("Specifies the SQL query to execute.")
    public abstract String getQuery();

    public static Builder builder() {
      return new AutoValue_SpannerReadSchemaTransformProvider_SpannerReadSchemaTransformConfiguration
          .Builder();
  }
}

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<SpannerReadSchemaTransformConfiguration>
      configurationClass() {
    return SpannerReadSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      SpannerReadSchemaTransformConfiguration configuration) {
    return new SpannerSchemaTransformRead(configuration);
  }
}