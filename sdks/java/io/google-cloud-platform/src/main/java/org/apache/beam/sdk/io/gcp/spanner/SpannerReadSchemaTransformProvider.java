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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/**
 * A provider for reading from Cloud Spanner using a Schema Transform Provider.
 *
 * <p>This provider enables reading from Cloud Spanner using a specified SQL query or by directly
 * accessing a table and its columns. It supports configuration through the {@link
 * SpannerReadSchemaTransformConfiguration} class, allowing users to specify project, instance,
 * database, table, query, and columns.
 *
 * <p>The transformation leverages the {@link SpannerIO} to perform the read operation and maps the
 * results to Beam rows, preserving the schema.
 *
 * <p>Example usage in a YAML pipeline using query:
 *
 * <pre>{@code
 * pipeline:
 *   transforms:
 *     - type: ReadFromSpanner
 *       name: ReadShipments
 *       # Columns: shipment_id, customer_id, shipment_date, shipment_cost, customer_name, customer_email
 *       config:
 *         project_id: 'apache-beam-testing'
 *         instance_id: 'shipment-test'
 *         database_id: 'shipment'
 *         query: 'SELECT * FROM shipments'
 * }</pre>
 *
 * <p>Example usage in a YAML pipeline using a table and columns:
 *
 * <pre>{@code
 * pipeline:
 *   transforms:
 *     - type: ReadFromSpanner
 *       name: ReadShipments
 *       # Columns: shipment_id, customer_id, shipment_date, shipment_cost, customer_name, customer_email
 *       config:
 *         project_id: 'apache-beam-testing'
 *         instance_id: 'shipment-test'
 *         database_id: 'shipment'
 *         table: 'shipments'
 *         columns: ['customer_id', 'customer_name']
 * }</pre>
 */
@AutoService(SchemaTransformProvider.class)
public class SpannerReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerReadSchemaTransformProvider.SpannerReadSchemaTransformConfiguration> {

  static class SpannerSchemaTransformRead extends SchemaTransform implements Serializable {
    private final SpannerReadSchemaTransformConfiguration configuration;

    SpannerSchemaTransformRead(SpannerReadSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkNotNull(input, "Input to SpannerReadSchemaTransform cannot be null.");
      SpannerIO.Read read =
          SpannerIO.readWithSchema()
              .withProjectId(configuration.getProjectId())
              .withInstanceId(configuration.getInstanceId())
              .withDatabaseId(configuration.getDatabaseId());

      if (!Strings.isNullOrEmpty(configuration.getQuery())) {
        read = read.withQuery(configuration.getQuery());
      } else {
        read = read.withTable(configuration.getTableId()).withColumns(configuration.getColumns());
      }
      PCollection<Struct> spannerRows = input.getPipeline().apply(read);
      Schema schema = spannerRows.getSchema();
      PCollection<Row> rows =
          spannerRows.apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via((Struct struct) -> StructUtils.structToBeamRow(struct, schema)));

      return PCollectionRowTuple.of("output", rows.setRowSchema(schema));
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SpannerReadSchemaTransformConfiguration implements Serializable {
    @AutoValue.Builder
    @Nullable
    public abstract static class Builder {
      public abstract Builder setProjectId(String projectId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setDatabaseId(String databaseId);

      public abstract Builder setTableId(String tableId);

      public abstract Builder setQuery(String query);

      public abstract Builder setColumns(List<String> columns);

      public abstract SpannerReadSchemaTransformConfiguration build();
    }

    public void validate() {
      String invalidConfigMessage = "Invalid Cloud Spanner Read configuration: ";
      checkArgument(
          !Strings.isNullOrEmpty(this.getInstanceId()),
          invalidConfigMessage + "Instance ID must be specified.");
      checkArgument(
          !Strings.isNullOrEmpty(this.getDatabaseId()),
          invalidConfigMessage + "Database ID must be specified.");
      if (Strings.isNullOrEmpty(this.getQuery())) {
        checkArgument(
            !Strings.isNullOrEmpty(this.getTableId()),
            invalidConfigMessage + "Table name must be specified for table read.");
        checkArgument(
            this.getColumns() != null && !this.getColumns().isEmpty(),
            invalidConfigMessage + "Columns must be specified for table read.");
      } else {
        checkArgument(
            !Strings.isNullOrEmpty(this.getQuery()),
            invalidConfigMessage + "Query must be specified for query read.");
        checkArgument(
            Strings.isNullOrEmpty(this.getTableId()),
            invalidConfigMessage + "Table name should not be specified when using a query.");
        checkArgument(
            this.getColumns() == null || this.getColumns().isEmpty(),
            invalidConfigMessage + "Columns should not be specified when using a query.");
      }
    }

    public static Builder builder() {
      return new AutoValue_SpannerReadSchemaTransformProvider_SpannerReadSchemaTransformConfiguration
          .Builder();
    }

    @SchemaFieldDescription("Specifies the GCP project ID.")
    @Nullable
    public abstract String getProjectId();

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    public abstract String getDatabaseId();

    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    @Nullable
    public abstract String getTableId();

    @SchemaFieldDescription("Specifies the SQL query to execute.")
    @Nullable
    public abstract String getQuery();

    @SchemaFieldDescription("Specifies the columns to read from the table.")
    @Nullable
    public abstract List<String> getColumns();
  }

  @Override
  protected Class<SpannerReadSchemaTransformConfiguration> configurationClass() {
    return SpannerReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(SpannerReadSchemaTransformConfiguration configuration) {
    return new SpannerSchemaTransformRead(configuration);
  }
}
