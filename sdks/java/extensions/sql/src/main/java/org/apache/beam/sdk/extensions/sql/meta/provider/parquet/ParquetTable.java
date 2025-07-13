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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.beam.sdk.io.parquet.ParquetFilterFactory;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.parquet.ParquetIO.Read;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
@SuppressWarnings({"nullness"})
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
    String filePattern = resolveFilePattern(table.getLocation());
    Read read = ParquetIO.read(schema).withBeamSchemas(true).from(filePattern);
    return begin.apply("ParquetIORead", read).apply("ToRows", Convert.toRows());
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filter, List<String> projectedFieldNames) {

    // Determine ALL fields required for the read (projection + filter fields).
    Set<String> requiredFieldsForRead = new HashSet<>(projectedFieldNames);
    if (filter instanceof ParquetFilter) {
      ParquetFilter parquetFilter = (ParquetFilter) filter;
      requiredFieldsForRead.addAll(parquetFilter.getReferencedFields(getSchema()));
    }

    // If no fields are projected or filtered, read the full schema.
    final Schema schema = AvroUtils.toAvroSchema(table.getSchema());
    String filePattern = resolveFilePattern(table.getLocation());
    Read read = ParquetIO.read(schema).withBeamSchemas(true).from(filePattern);
    if (!fieldNames.isEmpty()) {
      Schema projectionSchema = projectSchema(schema, fieldNames);
      LOG.info("Projecting fields schema: {}", projectionSchema);
      read = read.withProjection(projectionSchema, projectionSchema);
    Schema readSchema =
        requiredFieldsForRead.isEmpty()
            ? schema
            : projectSchema(schema, new ArrayList<>(requiredFieldsForRead));

    LOG.info("Projecting fields schema: {}", readSchema);

    Read read = ParquetIO.read(readSchema).withBeamSchemas(true).from(table.getLocation() + "/*");
    read = read.withProjection(readSchema, readSchema);

    if (filter instanceof ParquetFilter) {
      ParquetFilter parquetFilter = (ParquetFilter) filter;
      List<RexNode> supported = parquetFilter.getSupported();
      if (!supported.isEmpty()) {
        org.apache.beam.sdk.io.parquet.ParquetFilter predicate =
            ParquetFilterFactory.create(supported, getSchema());
        read = read.withFilter(predicate);
      }
    }

    PCollection<Row> rowsWithRequiredFields =
        begin.apply("ParquetIORead With Filtering", read).apply("ToRows", Convert.toRows());

    // If we read extra fields for filtering, project them away to match the final SELECT list.
    if (!projectedFieldNames.isEmpty()
        && !new HashSet<>(projectedFieldNames).equals(requiredFieldsForRead)) {
      return rowsWithRequiredFields.apply(
          Select.fieldNames(projectedFieldNames.toArray(new String[0])));
    } else {
      return rowsWithRequiredFields;
    }
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return new ParquetFilter(filter);
  }

  /** Returns a copy of the {@link Schema} with only the fieldNames fields. */
  private static Schema projectSchema(Schema schema, List<String> fieldNames) {
    if (fieldNames.isEmpty()) {
      return schema;
    }

    Set<String> fieldNameSet = new HashSet<>(fieldNames);
    List<Field> selectedFields = new ArrayList<>();

    // Iterate through the original schema's fields to maintain their order.
    for (Schema.Field field : schema.getFields()) {
      if (fieldNameSet.contains(field.name())) {
        selectedFields.add(deepCopyField(field));
      }
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
            FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to(table.getLocation()));
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
