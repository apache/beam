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
package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import java.io.Serializable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

public class DataStoreV1Table extends SchemaBaseBeamTable implements Serializable {
  // Should match: `projectId/kind`.
  private static final Pattern locationPattern = Pattern.compile("(?<projectId>.+)/(?<kind>.+)");
  @VisibleForTesting final String projectId;
  @VisibleForTesting final String kind;

  public DataStoreV1Table(Table table) {
    super(table.getSchema());

    String location = table.getLocation();
    Matcher matcher = locationPattern.matcher(location);
    checkArgument(
        matcher.matches(),
        "DataStoreV1 location must be in the following format: 'projectId/kind'");

    this.projectId = matcher.group("projectId");
    this.kind = matcher.group("kind");
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(kind);
    Query query = q.build();

    DatastoreV1.Read readInstance =
        DatastoreIO.v1().read().withProjectId(projectId).withQuery(query);

    PCollection<Entity> readEntities = readInstance.expand(begin);

    return readEntities
        .apply(ParDo.of(EntityToRowConverter.create(getSchema())))
        .setRowSchema(schema);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new RuntimeException("Writing to DataStoreV1 via SQL is unimplemented at the moment.");
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    long count =
        DatastoreIO.v1().read().withProjectId(projectId).getNumEntities(options, kind, null);

    if (count < 0) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }

    return BeamTableStatistics.createBoundedTableStatistics((double) count);
  }

  @VisibleForTesting
  static class EntityToRowConverter extends DoFn<Entity, Row> {
    private final Schema schema;

    private EntityToRowConverter(Schema schema) {
      this.schema = schema;
    }

    public static EntityToRowConverter create(Schema schema) {
      return new EntityToRowConverter(schema);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext context) {
      Entity entity = context.element();
      Map<String, Value> values = entity.getPropertiesMap();

      Row.Builder builder = Row.withSchema(schema);
      // It is not a guarantee that the values will be in the same order as the schema.
      // TODO: figure out in what order the elements are in (without relying on Beam schema).
      for (String fieldName : schema.getFieldNames()) {
        Value val = values.get(fieldName);
        switch (val.getValueTypeCase()) {
          case NULL_VALUE:
            builder.addValue(val.getNullValue());
            break;
          case BOOLEAN_VALUE:
            builder.addValue(val.getBooleanValue());
            break;
          case INTEGER_VALUE:
            builder.addValue(val.getIntegerValue());
            break;
          case DOUBLE_VALUE:
            builder.addValue(val.getDoubleValue());
            break;
          case TIMESTAMP_VALUE:
            builder.addValue(
                Instant.ofEpochSecond(val.getTimestampValue().getSeconds()).toDateTime());
            break;
          case KEY_VALUE:
            // Key is not a Beam type.
            builder.addValue(val.getKeyValue());
            break;
          case STRING_VALUE:
            builder.addValue(val.getStringValue());
            break;
          case BLOB_VALUE:
            // ByteString is not a Beam type. Can be converted to byte array.
            builder.addValue(val.getBlobValue());
            break;
          case GEO_POINT_VALUE:
            // LatLng is not a Beam type.
            builder.addValue(val.getGeoPointValue());
            break;
          case ENTITY_VALUE:
            // Entity is not a Beam type. Used for nested fields.
            builder.addValue(val.getEntityValue());
            break;
          case ARRAY_VALUE:
            // ArrayValue is not a Beam type.
            builder.addValue(val.getArrayValue());
            break;
          case VALUETYPE_NOT_SET:
            builder.addValue(null);
            break;
        }
      }
      context.output(builder.build());
    }
  }
}
