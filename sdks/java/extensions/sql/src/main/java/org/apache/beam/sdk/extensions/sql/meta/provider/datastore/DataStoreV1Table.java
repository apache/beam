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

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
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
    return input
        .apply(ParDo.of(RowToEntityConverter.create(getSchema(), kind)))
        .apply(DatastoreIO.v1().write().withProjectId(projectId));
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

  @VisibleForTesting
  static class RowToEntityConverter extends DoFn<Row, Entity> {
    private final boolean randomName;
    private final Schema schema;
    private final String kind;

    private RowToEntityConverter(boolean randomName, Schema schema, String kind) {
      this.randomName = randomName;
      this.schema = schema;
      this.kind = kind;
    }

    public static RowToEntityConverter create(Schema schema, String kind) {
      return new RowToEntityConverter(true, schema, kind);
    }

    static RowToEntityConverter createTest(Schema schema, String kind) {
      return new RowToEntityConverter(false, schema, kind);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();

      Entity.Builder entityBuilder = Entity.newBuilder();
      // TODO: some entities might want a non random, meaningful key.
      Key key;
      if (randomName) {
        key = makeKey(kind, UUID.randomUUID().toString()).build();
      } else {
        key = makeKey(kind).build();
      }
      entityBuilder.setKey(key);

      for (Schema.Field field : schema.getFields()) {
        Value.Builder valBuilder;
        switch (field.getType().getTypeName()) {
          case BOOLEAN:
            valBuilder = makeValue((boolean) row.getValue(field.getName()));
            break;
          case BYTE:
            valBuilder = makeValue((byte) row.getValue(field.getName()));
            break;
          case BYTES:
            valBuilder = makeValue((ByteString) row.getValue(field.getName()));
            break;
          case DECIMAL:
            valBuilder = makeValue(((BigDecimal) row.getValue(field.getName())).longValue());
            break;
          case FLOAT:
          case DOUBLE:
            valBuilder = makeValue(((double) row.getValue(field.getName())));
            break;
          case INT16:
            valBuilder = makeValue((short) row.getValue(field.getName()));
            break;
          case INT32:
            valBuilder = makeValue((int) row.getValue(field.getName()));
            break;
          case INT64:
            valBuilder = makeValue((long) row.getValue(field.getName()));
            break;
            /*case STRING:
            valBuilder = makeValue((String) row.getValue(field.getName()));
            break;*/
          case LOGICAL_TYPE:
            Schema.FieldType logicalBaseType = field.getType().getLogicalType().getBaseType();
            // TODO: make this recursive.
            if (logicalBaseType.getTypeName().equals(TypeName.STRING)) {
              valBuilder = makeValue((String) row.getValue(field.getName()));
            } else {
              throw new IllegalStateException(
                  "Unexpected logical FieldType: " + logicalBaseType.getTypeName());
            }
            break;
          default:
            throw new IllegalStateException(
                "Unexpected FieldType: " + field.getType().getTypeName());
        }
        entityBuilder.putProperties(field.getName(), valBuilder.build());
      }

      context.output(entityBuilder.build());
    }
  }
}
