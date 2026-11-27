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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.parquet.ParquetIO.Read;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
class ParquetTable extends SchemaBaseBeamTable implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetTable.class);

  private final Table table;

  ParquetTable(Table table) {
    super(table.getSchema());
    this.table = table;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    final Schema schema = AvroUtils.toAvroSchema(table.getSchema());
    String filePattern = resolveFilePattern(checkArgumentNotNull(table.getLocation()));
    Read read = ParquetIO.read(schema).withBeamSchemas(true).from(filePattern);
    return toRows(begin.apply("ParquetIORead", read), schema);
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    final Schema schema = AvroUtils.toAvroSchema(table.getSchema());
    Schema readSchema = schema;
    String filePattern = resolveFilePattern(checkArgumentNotNull(table.getLocation()));
    Read read = ParquetIO.read(schema).withBeamSchemas(true).from(filePattern);
    if (!fieldNames.isEmpty()) {
      readSchema = projectSchema(schema, fieldNames);
      LOG.info("Projecting fields schema: {}", readSchema);
      read = read.withProjection(readSchema, readSchema);
    }
    return toRows(begin.apply("ParquetIORead", read), readSchema);
  }

  private PCollection<Row> toRows(PCollection<GenericRecord> records, Schema avroSchema) {
    org.apache.beam.sdk.schemas.Schema inferredSchema = AvroUtils.toBeamSchema(avroSchema);
    org.apache.beam.sdk.schemas.Schema.Builder builder =
        org.apache.beam.sdk.schemas.Schema.builder();
    for (org.apache.beam.sdk.schemas.Schema.Field field : inferredSchema.getFields()) {
      org.apache.beam.sdk.schemas.Schema.FieldType tableFieldType =
          table.getSchema().getField(field.getName()).getType();
      builder.addField(
          tableFieldType.isLogicalType(SqlTypes.TIME.getIdentifier())
              ? field.toBuilder().setType(tableFieldType).build()
              : field);
    }
    org.apache.beam.sdk.schemas.Schema readSchema = builder.build();
    return records
        .apply(
            "ToRows",
            MapElements.into(TypeDescriptors.rows())
                .via(AvroUtils.getGenericRecordToRowFunction(readSchema)))
        .setRowSchema(readSchema);
  }

  /** Returns a copy of the {@link Schema} with only the fieldNames fields. */
  private static Schema projectSchema(Schema schema, List<String> fieldNames) {
    List<Field> selectedFields = new ArrayList<>();
    for (String fieldName : fieldNames) {
      selectedFields.add(deepCopyField(schema.getField(fieldName)));
    }
    return Schema.createRecord(
        schema.getName() + "_projected",
        schema.getDoc(),
        schema.getNamespace(),
        schema.isError(),
        selectedFields);
  }

  private static Field deepCopyField(Field field) {
    Schema.Field newField =
        new Schema.Field(
            field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
    for (Map.Entry<String, Object> kv : field.getObjectProps().entrySet()) {
      newField.addProp(kv.getKey(), kv.getValue());
    }
    if (field.aliases() != null) {
      for (String alias : field.aliases()) {
        newField.addAlias(alias);
      }
    }
    return newField;
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    final org.apache.avro.Schema schema = AvroUtils.toAvroSchema(input.getSchema());
    return input
        .apply("ToGenericRecords", Convert.to(GenericRecord.class))
        .apply(
            "ParquetIOWrite",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(schema))
                .to(checkArgumentNotNull(table.getLocation())));
  }

  @Override
  public IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public ProjectSupport supportsProjects() {
    return ProjectSupport.WITH_FIELD_REORDERING;
  }

  private String resolveFilePattern(String location) {
    if (location.endsWith("/")) {
      return location + "*";
    }
    return location;
  }
}
