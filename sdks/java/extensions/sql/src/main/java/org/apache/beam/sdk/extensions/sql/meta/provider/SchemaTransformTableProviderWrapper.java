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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

import avro.shaded.com.google.common.collect.Iterables;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.NullBehavior;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/** A general {@link TableProvider} for transforms for consumption by Beam SQL. */
@Internal
@Experimental
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class SchemaTransformTableProviderWrapper extends InMemoryMetaTableProvider
    implements Serializable {

  // Subclasses should provide the SchemaTransformProvider that is specific to its transforms.
  // Requirements:
  //    Providers have matching configuration schemas.
  //    Readers go from empty PCollectionTuple to PCollectionTuple with 1 PCollection<Row>.
  //    Writers go from PCollectionTuple with 1 PCollection<Row> to empty PCollectionTuple.
  public abstract SchemaTransformProvider getReadTransformProvider();

  public abstract SchemaTransformProvider getWriteTransformProvider();

  // The configuration field name that has the location.
  public abstract String getLocationConfigName();

  // The configuration field name that has the schema.
  public abstract @Nullable String getSchemaConfigName();

  // If the write configuration takes the schema. If false, it may take any schema or lookup it's
  // own schema.
  public abstract boolean schemaConfigOnlyOnRead();

  // If the transform is bounded.
  public abstract PCollection.IsBounded isBounded();

  @Override
  public String getTableType() {
    // For now just parse from getReadTransform, but users may also override.
    // If there's enforcement of URNs then we could do some parsing long term and fail if doesn't
    // match.
    return Iterables.get(Splitter.on(':').split(getReadTransformProvider().identifier()), 0);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table tableDefinition) {
    JSONObject tableProperties = tableDefinition.getProperties();

    try {
      // Schema and location fields should not be in the configuration, so drop them and parse.
      RowJson.RowJsonDeserializer deserializer =
          RowJson.RowJsonDeserializer.forSchema(
                  removeSchemaFields(getReadTransformProvider().configurationSchema()))
              .withNullBehavior(NullBehavior.ACCEPT_MISSING_OR_NULL);

      Row configurationRow =
          newObjectMapperWith(deserializer).readValue(tableProperties.toString(), Row.class);
      // Add the schema and location fields to the schema for the transform. Read config and write
      // config may differ if they have default values or if schema isn't needed on write.
      Row readConfigs =
          addSchemaFields(
              getReadTransformProvider().configurationSchema(),
              configurationRow,
              tableDefinition.getSchema(),
              tableDefinition.getLocation());
      Row writeConfigs =
          addSchemaFields(
              getWriteTransformProvider().configurationSchema(),
              configurationRow,
              schemaConfigOnlyOnRead() ? null : tableDefinition.getSchema(),
              tableDefinition.getLocation());

      SchemaTransform read = getReadTransformProvider().from(readConfigs);
      SchemaTransform write = getWriteTransformProvider().from(writeConfigs);
      String readCollectionName = getReadTransformProvider().getOutputCollectionNames().get(0);
      String writeCollectionName = getWriteTransformProvider().getInputCollectionNames().get(0);

      return new SchemaTransformTableWrapper(
          read, write, readCollectionName, writeCollectionName, tableDefinition.getSchema());
    } catch (InvalidConfigurationException | InvalidSchemaException e) {
      throw new InvalidTableException(e.getMessage());
    } catch (JsonProcessingException e) {
      throw new AssertionError(
          "Failed to re-parse TBLPROPERTIES JSON " + tableProperties.toString());
    }
  }

  protected BeamTableStatistics getTableStatistics(
      PipelineOptions options, SchemaTransform readTransform, SchemaTransform writeTransform) {
    if (isBounded().equals(PCollection.IsBounded.BOUNDED)) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }

  // Returns a schema that matches configuration but without location or schema fields.
  private Schema removeSchemaFields(Schema configuration) {
    Schema.Builder schema = Schema.builder();
    for (Field field : configuration.getFields()) {
      if (!field.getName().equals(getLocationConfigName())
          && (getSchemaConfigName() == null || !field.getName().equals(getSchemaConfigName()))) {
        schema.addField(field);
      }
    }
    return schema.build();
  }

  // Takes the output schema (with location/schema) and the configuration (without location/schema)
  // and outputs a configuration matching the final output schema using schema and location.
  private Row addSchemaFields(
      Schema outputRowSchema, Row inputRow, @Nullable Schema schema, @Nullable String location) {
    Row.Builder builder = Row.withSchema(outputRowSchema);
    for (Field field : outputRowSchema.getFields()) {
      if (field.getName().equals(getLocationConfigName())) {
        builder.addValue(location);
      } else if (getSchemaConfigName() != null && field.getName().equals(getSchemaConfigName())) {
        builder.addValue(schema);
      } else {
        builder.addValue(inputRow.getValue(field.getName()));
      }
    }
    return builder.build();
  }

  /** A generalized {@link BeamSqlTable} for IOs to create IO readers and writers. */
  private class SchemaTransformTableWrapper extends BaseBeamTable {
    protected final SchemaTransform schemaReadTransform;
    protected final SchemaTransform schemaWriteTransform;
    protected final String readCollectionName;
    protected final String writeCollectionName;
    protected final Schema schema;

    private SchemaTransformTableWrapper(
        SchemaTransform schemaReadTransform,
        SchemaTransform schemaWriteTransform,
        String readCollectionName,
        String writeCollectionName,
        Schema schema) {
      this.schemaReadTransform = schemaReadTransform;
      this.schemaWriteTransform = schemaWriteTransform;
      this.readCollectionName = readCollectionName;
      this.writeCollectionName = writeCollectionName;
      this.schema = schema;
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return SchemaTransformTableProviderWrapper.this.isBounded();
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      PCollectionRowTuple result =
          PCollectionRowTuple.empty(begin.getPipeline())
              .apply(schemaReadTransform.buildTransform());
      return result.get(readCollectionName);
    }

    @Override
    public PCollection<Row> buildIOReader(
        PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {

      // TODO: Update the pushdown API to PCollectionTuple? No needed but maybe more consisten.
      PTransform<PCollectionRowTuple, PCollectionRowTuple> readerTransform =
          schemaReadTransform.buildTransform();
      if (!(filters instanceof DefaultTableFilter)) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter pushdown is not yet supported in %s. BEAM-12663",
                SchemaTransformTableWrapper.class));
      }
      if (!fieldNames.isEmpty()) {
        if (readerTransform instanceof ProjectionProducer) {
          ProjectionProducer<PTransform<PBegin, PCollection<Row>>> projectionProducer =
              (ProjectionProducer<PTransform<PBegin, PCollection<Row>>>) readerTransform;
          FieldAccessDescriptor fieldAccessDescriptor =
              FieldAccessDescriptor.withFieldNames(fieldNames);
          // The pushdown must return a PTransform that can be applied to a PBegin, or this cast
          // will fail.
          begin.apply(
              (PTransform<PBegin, PCollection<Row>>)
                  projectionProducer.actuateProjectionPushdown(
                      ImmutableMap.of(new TupleTag<PCollection<Row>>("output"), fieldAccessDescriptor)));
        } else {
          throw new UnsupportedOperationException(
              String.format("%s does not support projection pushdown.", this.getClass()));
        }
      }
      return buildIOReader(begin); // Result to default.
    }

    @Override
    public ProjectSupport supportsProjects() {
      PTransform<PCollectionRowTuple, PCollectionRowTuple> readerTransform =
          schemaReadTransform.buildTransform();
      if (readerTransform instanceof ProjectionProducer) {
        if (((ProjectionProducer<?>) readerTransform).supportsProjectionPushdown()) {
          // For ProjectionProducer, supportsProjectionPushdown implies field reordering support.
          return ProjectSupport.WITH_FIELD_REORDERING;
        }
      }
      return ProjectSupport.NONE;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      PCollectionRowTuple result =
          PCollectionRowTuple.of(writeCollectionName, input)
              .apply(schemaWriteTransform.buildTransform());
      if (!result.getAll().isEmpty()) {
        throw new IllegalArgumentException("Unexpected write transform not empty.");
      }
      return PDone.in(input.getPipeline());
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
      return SchemaTransformTableProviderWrapper.this.getTableStatistics(
          options, schemaReadTransform, schemaWriteTransform);
    }
  }
}
