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

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Serializable;
import java.util.List;
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
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** A general {@link TableProvider} for IOs for consumption by Beam SQL. */
@Internal
@Experimental
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class SchemaIOTableProviderWrapper extends InMemoryMetaTableProvider
    implements Serializable {

  // Subclasses should provide the schemaIOProvider that is specific to its IO.
  public abstract SchemaIOProvider getSchemaIOProvider();

  @Override
  public String getTableType() {
    return getSchemaIOProvider().identifier();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table tableDefinition) {
    JSONObject tableProperties = tableDefinition.getProperties();

    try {
      RowJson.RowJsonDeserializer deserializer =
          RowJson.RowJsonDeserializer.forSchema(getSchemaIOProvider().configurationSchema())
              .withNullBehavior(RowJson.RowJsonDeserializer.NullBehavior.ACCEPT_MISSING_OR_NULL);

      Row configurationRow =
          newObjectMapperWith(deserializer).readValue(tableProperties.toString(), Row.class);

      SchemaIO schemaIO =
          getSchemaIOProvider()
              .from(tableDefinition.getLocation(), configurationRow, tableDefinition.getSchema());

      return new SchemaIOTableWrapper(schemaIO);
    } catch (InvalidConfigurationException | InvalidSchemaException e) {
      throw new InvalidTableException(e.getMessage());
    } catch (JsonProcessingException e) {
      throw new AssertionError(
          "Failed to re-parse TBLPROPERTIES JSON " + tableProperties.toString());
    }
  }

  protected BeamTableStatistics getTableStatistics(PipelineOptions options, SchemaIO schemaIO) {
    if (isBounded().equals(PCollection.IsBounded.BOUNDED)) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }

  private PCollection.IsBounded isBounded() {
    return getSchemaIOProvider().isBounded();
  }

  /** A generalized {@link BeamSqlTable} for IOs to create IO readers and writers. */
  private class SchemaIOTableWrapper extends BaseBeamTable {
    protected final SchemaIO schemaIO;

    private SchemaIOTableWrapper(SchemaIO schemaIO) {
      this.schemaIO = schemaIO;
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return SchemaIOTableProviderWrapper.this.isBounded();
    }

    @Override
    public Schema getSchema() {
      return schemaIO.schema();
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      PTransform<PBegin, PCollection<Row>> readerTransform = schemaIO.buildReader();
      return begin.apply(readerTransform);
    }

    @Override
    public PCollection<Row> buildIOReader(
        PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
      PTransform<PBegin, PCollection<Row>> readerTransform = schemaIO.buildReader();
      if (!(filters instanceof DefaultTableFilter)) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter pushdown is not yet supported in %s. BEAM-12663",
                SchemaIOTableWrapper.class));
      }
      if (!fieldNames.isEmpty()) {
        if (readerTransform instanceof ProjectionProducer) {
          // The pushdown must return a PTransform that can be applied to a PBegin, or this cast
          // will fail.
          ProjectionProducer<PTransform<PBegin, PCollection<Row>>> projectionProducer =
              (ProjectionProducer<PTransform<PBegin, PCollection<Row>>>) readerTransform;
          FieldAccessDescriptor fieldAccessDescriptor =
              FieldAccessDescriptor.withFieldNames(fieldNames);
          readerTransform =
              projectionProducer.actuateProjectionPushdown(
                  ImmutableMap.of(new TupleTag<PCollection<Row>>("output"), fieldAccessDescriptor));
        } else {
          throw new UnsupportedOperationException(
              String.format("%s does not support projection pushdown.", this.getClass()));
        }
      }
      return begin.apply(readerTransform);
    }

    @Override
    public ProjectSupport supportsProjects() {
      PTransform<PBegin, PCollection<Row>> readerTransform = schemaIO.buildReader();
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
      PTransform<PCollection<Row>, ? extends POutput> writerTransform = schemaIO.buildWriter();
      return input.apply(writerTransform);
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
      return SchemaIOTableProviderWrapper.this.getTableStatistics(options, schemaIO);
    }
  }
}
