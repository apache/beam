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
import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ITERABLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.LOGICAL_TYPE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.MAP;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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

    // TODO: allow users to specify a namespace in a location string.
    String location = table.getLocation();
    assert location != null;
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

      context.output(extractRowFromProperties(schema, values));
    }

    private Row extractRowFromProperties(Schema schema, Map<String, Value> values) {
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
            // TODO: DataStore may not support milliseconds.
            com.google.protobuf.Timestamp time = val.getTimestampValue();
            long millis = time.getSeconds() * 1000 + time.getNanos() / 1000;
            builder.addValue(Instant.ofEpochMilli(millis).toDateTime());
            break;
          case KEY_VALUE:
            // Key is not a Beam type.
            // TODO: Convert to some Beam type (ex: String or Row)
            //  More information about keys can be found here:
            // https://cloud.google.com/datastore/docs/concepts/entities.
            Key key = val.getKeyValue();
            // builder.addValue(null);
            throw new IllegalStateException("Unsupported ValueType: " + val.getValueTypeCase());
          case STRING_VALUE:
            builder.addValue(val.getStringValue());
            break;
          case BLOB_VALUE:
            // ByteString is not a Beam type. Can be converted to byte array.
            builder.addValue(val.getBlobValue().toByteArray());
            break;
          case ENTITY_VALUE:
            // Entity is not a Beam type. Used for nested fields.
            Schema entitySchema = schema.getField(fieldName).getType().getRowSchema();
            assert entitySchema != null;
            Entity childEntity = val.getEntityValue();
            builder.addValue(
                extractRowFromProperties(entitySchema, childEntity.getPropertiesMap()));
            break;
          case ARRAY_VALUE:
            // TODO: Support non-primitive types. Make this recursive.
            FieldType elementType = schema.getField(fieldName).getType().getCollectionElementType();
            assert elementType != null;
            if (elementType.equals(FieldType.of(ARRAY))
                || elementType.equals(FieldType.of(ITERABLE))
                || elementType.equals(FieldType.of(MAP))
                || elementType.equals(FieldType.of(ROW))
                || elementType.equals(FieldType.of(LOGICAL_TYPE))) {
              throw new IllegalStateException(
                  "Arrays support only primitive types, but received: " + elementType.toString());
            }
            List<Value> valueList = val.getArrayValue().getValuesList();
            List<Object> array =
                valueList.stream()
                    .flatMap(v -> v.getAllFields().values().stream())
                    .collect(Collectors.toList());
            builder.addValue(array);
            break;
          case VALUETYPE_NOT_SET:
            builder.addValue(null);
            break;
          default:
            throw new IllegalStateException("Unexpected ValueType: " + val.getValueTypeCase());
        }
      }
      return builder.build();
    }
  }

  @VisibleForTesting
  static class RowToEntityConverter extends DoFn<Row, Entity> {
    private static final Function<Long, Void> MAPPING_NOT_FOUND = v -> { throw new IllegalStateException("No conversion exists for primitive type: " + v.getClass()); };
    private static final ImmutableMap<Object, Object> MAPPING_FUNCTIONS = ImmutableMap.builder()
        .put(Boolean.class, (Function<Boolean, Value>) v -> makeValue(v).build())
        .put(Byte.class, (Function<Byte, Value>) v -> makeValue(v).build())
        .put(Long.class, (Function<Long, Value>) v -> makeValue(v).build())
        .put(Short.class, (Function<Short, Value>) v -> makeValue(v).build())
        .put(Integer.class, (Function<Integer, Value>) v -> makeValue(v).build())
        .put(Double.class, (Function<Double, Value>) v -> makeValue(v).build())
        .put(Float.class, (Function<Float, Value>) v -> makeValue(v).build())
        .put(String.class, (Function<String, Value>) v -> makeValue(v).build())
        .put(Instant.class, (Function<Instant, Value>) v -> makeValue(v.toDate()).build())
        .put(byte[].class, (Function<byte[], Value>) v -> makeValue(ByteString.copyFrom(v)).build())
        .build();
    private final Supplier<String> randomKeySupplier;
    private final Schema schema;
    private final String kind;

    private RowToEntityConverter(Supplier<String> randomKeySupplier, Schema schema, String kind) {
      this.randomKeySupplier = randomKeySupplier;
      this.schema = schema;
      this.kind = kind;
    }

    public static RowToEntityConverter create(Schema schema, String kind) {
      return new RowToEntityConverter(
          (Supplier<String> & Serializable) () -> UUID.randomUUID().toString(), schema, kind);
    }

    @VisibleForTesting
    static RowToEntityConverter createTest(String randomKey, Schema schema, String kind) {
      return new RowToEntityConverter(
          (Supplier<String> & Serializable) () -> randomKey, schema, kind);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();

      Entity.Builder entityBuilder = constructEntityFromRow(schema, row);
      // TODO: some entities might want a non random, meaningful key.
      Key key = makeKey(kind, randomKeySupplier.get()).build();
      entityBuilder.setKey(key);

      context.output(entityBuilder.build());
    }

    private static Value mapObjectToValue(Object value) {
      return ((Function<Object, Value>) MAPPING_FUNCTIONS.getOrDefault(value.getClass(), MAPPING_NOT_FOUND)).apply(value);
    }

    private Entity.Builder constructEntityFromRow(Schema schema, Row row) {
      Entity.Builder entityBuilder = Entity.newBuilder();
      for (Schema.Field field : schema.getFields()) {
        Value valBuilder;

        Object value = row.getValue(field.getName());
        Schema.TypeName typeName = field.getType().getTypeName();

        // TODO: add support for nested rows `typeName.isCompositeType()`.
        if (value == null) {
          valBuilder = Value.newBuilder().build();
        } else if (typeName.isPrimitiveType() || value instanceof String) {
          valBuilder = mapObjectToValue(value);
        } else if (typeName.isCollectionType()) {
          // TODO: support collections of complex types (ex: Rows, Arrays).
          Collection<Object> collection = (Collection<Object>) value;
          List<Value> arrayValues =
              collection.stream().map(RowToEntityConverter::mapObjectToValue).collect(Collectors.toList());
          valBuilder = makeValue(arrayValues).build();
        } else {
          throw new IllegalStateException("Unsupported row type: " + typeName.toString());
        }

        entityBuilder.putProperties(field.getName(), valBuilder);
      }
      return entityBuilder;
    }
  }
}
