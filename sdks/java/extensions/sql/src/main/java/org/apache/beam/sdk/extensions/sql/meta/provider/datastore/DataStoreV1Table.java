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

import com.alibaba.fastjson.JSONObject;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.Value.ValueTypeCase;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
@Experimental
class DataStoreV1Table extends SchemaBaseBeamTable implements Serializable {
  public static final String KEY_FIELD_PROPERTY = "keyField";
  @VisibleForTesting static final String DEFAULT_KEY_FIELD = "__key__";
  private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreV1Table.class);
  // Should match: `projectId/kind`.
  private static final Pattern locationPattern = Pattern.compile("(?<projectId>.+)/(?<kind>.+)");
  @VisibleForTesting final String keyField;
  @VisibleForTesting final String projectId;
  @VisibleForTesting final String kind;

  DataStoreV1Table(Table table) {
    super(table.getSchema());

    // TODO: allow users to specify a name of the field to store a key value via TableProperties.
    JSONObject properties = table.getProperties();
    if (properties.containsKey(KEY_FIELD_PROPERTY)) {
      String field = properties.getString(KEY_FIELD_PROPERTY);
      if (!(field != null && !field.isEmpty())) {
        throw new InvalidTableException(
            String.format("'%s' property cannot be null.", KEY_FIELD_PROPERTY));
      }
      keyField = field;
    } else {
      keyField = DEFAULT_KEY_FIELD;
    }
    // TODO: allow users to specify a namespace in a location string.
    String location = table.getLocation();
    if (location == null) {
      throw new InvalidTableException("DataStoreV1 location must be set: " + table);
    }
    Matcher matcher = locationPattern.matcher(location);

    if (!matcher.matches()) {
      throw new InvalidTableException(
          "DataStoreV1 location must be in the following format: 'projectId/kind'"
              + " but was:"
              + location);
    }

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

    return begin
        .apply("Read Datastore Entities", readInstance)
        .apply("Convert Datastore Entities to Rows", EntityToRow.create(getSchema(), keyField));
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input
        .apply("Convert Rows to Datastore Entities", RowToEntity.create(keyField, kind))
        .apply("Write Datastore Entities", DatastoreIO.v1().write().withProjectId(projectId));
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

  /**
   * A {@code PTransform} to perform a conversion of {@code PCollection<Entity>} to {@code
   * PCollection<Row>}.
   */
  public static class EntityToRow extends PTransform<PCollection<Entity>, PCollection<Row>> {
    private final Schema schema;
    private final String keyField;

    private EntityToRow(Schema schema, String keyField) {
      this.schema = schema;
      this.keyField = keyField;

      if (schema.getFieldNames().contains(keyField)) {
        if (!schema.getField(keyField).getType().getTypeName().equals(TypeName.BYTES)) {
          throw new IllegalStateException(
              "Field `"
                  + keyField
                  + "` should of type `VARBINARY`. Please change the type or specify a field to"
                  + " store the KEY value.");
        }
        LOGGER.info("Entity KEY will be stored under `" + keyField + "` field.");
      }
    }

    /**
     * Create a PTransform instance.
     *
     * @param schema {@code Schema} of the target row.
     * @param keyField A name of the row field to store the {@code Key} in.
     * @return {@code PTransform} instance for Entity to Row conversion.
     */
    public static EntityToRow create(Schema schema, String keyField) {
      return new EntityToRow(schema, keyField);
    }

    @Override
    public PCollection<Row> expand(PCollection<Entity> input) {
      return input.apply(ParDo.of(new EntityToRowConverter())).setRowSchema(schema);
    }

    @VisibleForTesting
    class EntityToRowConverter extends DoFn<Entity, Row> {

      @DoFn.ProcessElement
      public void processElement(ProcessContext context) {
        Entity entity = context.element();
        ImmutableMap.Builder<String, Value> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(keyField, makeValue(entity.getKey()).build());
        mapBuilder.putAll(entity.getPropertiesMap());

        context.output(extractRowFromProperties(schema, mapBuilder.build()));
      }

      /**
       * Convert DataStore {@code Value} to Beam type.
       *
       * @param currentFieldType Beam {@code Schema.FieldType} to convert to (used for {@code Row}
       *     and {@code Array}).
       * @param val DataStore {@code Value}.
       * @return resulting Beam type.
       */
      private Object convertValueToObject(FieldType currentFieldType, Value val) {
        ValueTypeCase typeCase = val.getValueTypeCase();

        switch (typeCase) {
          case NULL_VALUE:
          case VALUETYPE_NOT_SET:
            return null;
          case BOOLEAN_VALUE:
            return val.getBooleanValue();
          case INTEGER_VALUE:
            return val.getIntegerValue();
          case DOUBLE_VALUE:
            return val.getDoubleValue();
          case TIMESTAMP_VALUE:
            com.google.protobuf.Timestamp time = val.getTimestampValue();
            long millis = time.getSeconds() * 1000 + time.getNanos() / 1000;
            return Instant.ofEpochMilli(millis).toDateTime();
          case STRING_VALUE:
            return val.getStringValue();
          case KEY_VALUE:
            return val.getKeyValue().toByteArray();
          case BLOB_VALUE:
            return val.getBlobValue().toByteArray();
          case ENTITY_VALUE:
            // Recursive mapping for row type.
            Schema rowSchema = currentFieldType.getRowSchema();
            assert rowSchema != null;
            Entity entity = val.getEntityValue();
            return extractRowFromProperties(rowSchema, entity.getPropertiesMap());
          case ARRAY_VALUE:
            // Recursive mapping for collection type.
            FieldType elementType = currentFieldType.getCollectionElementType();
            List<Value> valueList = val.getArrayValue().getValuesList();
            return valueList.stream()
                .map(v -> convertValueToObject(elementType, v))
                .collect(Collectors.toList());
          case GEO_POINT_VALUE:
          default:
            throw new IllegalStateException(
                "No conversion exists from type: "
                    + val.getValueTypeCase().name()
                    + " to Beam type.");
        }
      }

      /**
       * Converts all properties of an {@code Entity} to Beam {@code Row}.
       *
       * @param schema Target row {@code Schema}.
       * @param values A map of property names and values.
       * @return resulting Beam {@code Row}.
       */
      private Row extractRowFromProperties(Schema schema, Map<String, Value> values) {
        Row.Builder builder = Row.withSchema(schema);
        // It is not a guarantee that the values will be in the same order as the schema.
        // Maybe metadata:
        // https://cloud.google.com/appengine/docs/standard/python/datastore/metadataqueries
        // TODO: figure out in what order the elements are in (without relying on Beam schema).
        for (Schema.Field field : schema.getFields()) {
          Value val = values.get(field.getName());
          builder.addValue(convertValueToObject(field.getType(), val));
        }
        return builder.build();
      }
    }
  }

  /**
   * A {@code PTransform} to perform a conversion of {@code PCollection<Row>} to {@code
   * PCollection<Entity>}.
   */
  public static class RowToEntity extends PTransform<PCollection<Row>, PCollection<Entity>> {
    private final Supplier<String> keySupplier;
    private final String kind;
    private final String keyField;

    private RowToEntity(Supplier<String> keySupplier, String kind, String keyField) {
      this.keySupplier = keySupplier;
      this.kind = kind;
      this.keyField = keyField;
    }

    @Override
    public PCollection<Entity> expand(PCollection<Row> input) {
      boolean isFieldPresent = input.getSchema().getFieldNames().contains(keyField);
      if (isFieldPresent) {
        if (!input.getSchema().getField(keyField).getType().getTypeName().equals(TypeName.BYTES)) {
          throw new IllegalStateException(
              "Field `"
                  + keyField
                  + "` should of type `VARBINARY`. Please change the type or specify a field to"
                  + " write the KEY value from via TableProperties.");
        }
        LOGGER.info("Field to use as Entity KEY is set to: `" + keyField + "`.");
      }
      return input.apply(ParDo.of(new RowToEntityConverter(isFieldPresent)));
    }

    /**
     * Create a PTransform instance.
     *
     * @param keyField Row field containing a serialized {@code Key}, must be set when using user
     *     specified keys.
     * @param kind DataStore `Kind` data will be written to (required when generating random {@code
     *     Key}s).
     * @return {@code PTransform} instance for Row to Entity conversion.
     */
    public static RowToEntity create(String keyField, String kind) {
      return new RowToEntity(
          (Supplier<String> & Serializable) () -> UUID.randomUUID().toString(), kind, keyField);
    }

    @VisibleForTesting
    static RowToEntity createTest(String keyString, String keyField, String kind) {
      return new RowToEntity((Supplier<String> & Serializable) () -> keyString, kind, keyField);
    }

    @VisibleForTesting
    class RowToEntityConverter extends DoFn<Row, Entity> {
      private final boolean useNonRandomKey;

      RowToEntityConverter(boolean useNonRandomKey) {
        super();
        this.useNonRandomKey = useNonRandomKey;
      }

      @DoFn.ProcessElement
      public void processElement(ProcessContext context) {
        Row row = context.element();

        Schema schemaWithoutKeyField =
            Schema.builder()
                .addFields(
                    row.getSchema().getFields().stream()
                        .filter(field -> !field.getName().equals(keyField))
                        .collect(Collectors.toList()))
                .build();
        Entity.Builder entityBuilder = constructEntityFromRow(schemaWithoutKeyField, row);
        entityBuilder.setKey(constructKeyFromRow(row));

        context.output(entityBuilder.build());
      }

      /**
       * Converts an entire {@code Row} to an appropriate DataStore {@code Entity.Builder}.
       *
       * @param row {@code Row} to convert.
       * @return resulting {@code Entity.Builder}.
       */
      private Entity.Builder constructEntityFromRow(Schema schema, Row row) {
        Entity.Builder entityBuilder = Entity.newBuilder();
        for (Schema.Field field : schema.getFields()) {
          Value val = mapObjectToValue(row.getValue(field.getName()));
          entityBuilder.putProperties(field.getName(), val);
        }
        return entityBuilder;
      }

      /**
       * Create a random key for a {@code Row} without a keyField or use a user-specified key by
       * parsing it from byte array when keyField is set.
       *
       * @param row {@code Row} to construct a key for.
       * @return resulting {@code Key}.
       */
      private Key constructKeyFromRow(Row row) {
        if (!useNonRandomKey) {
          // When key field is not present - use key supplier to generate a random one.
          return makeKey(kind, keySupplier.get()).build();
        }
        byte[] keyBytes = row.getBytes(keyField);
        try {
          return Key.parseFrom(keyBytes);
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalStateException("Failed to parse DataStore key from bytes.");
        }
      }

      /**
       * Converts a {@code Row} value to an appropriate DataStore {@code Value} object.
       *
       * @param value {@code Row} value to convert.
       * @throws IllegalStateException when no mapping function for object of given type exists.
       * @return resulting {@code Value}.
       */
      private Value mapObjectToValue(Object value) {
        if (value == null) {
          return Value.newBuilder().build();
        }

        if (Boolean.class.equals(value.getClass())) {
          return makeValue((Boolean) value).build();
        } else if (Byte.class.equals(value.getClass())) {
          return makeValue((Byte) value).build();
        } else if (Long.class.equals(value.getClass())) {
          return makeValue((Long) value).build();
        } else if (Short.class.equals(value.getClass())) {
          return makeValue((Short) value).build();
        } else if (Integer.class.equals(value.getClass())) {
          return makeValue((Integer) value).build();
        } else if (Double.class.equals(value.getClass())) {
          return makeValue((Double) value).build();
        } else if (Float.class.equals(value.getClass())) {
          return makeValue((Float) value).build();
        } else if (String.class.equals(value.getClass())) {
          return makeValue((String) value).build();
        } else if (Instant.class.equals(value.getClass())) {
          return makeValue(((Instant) value).toDate()).build();
        } else if (byte[].class.equals(value.getClass())) {
          return makeValue(ByteString.copyFrom((byte[]) value)).build();
        } else if (value instanceof Row) {
          // Recursive conversion to handle nested rows.
          Row row = (Row) value;
          return makeValue(constructEntityFromRow(row.getSchema(), row)).build();
        } else if (value instanceof Collection) {
          // Recursive to handle nested collections.
          Collection<Object> collection = (Collection<Object>) value;
          List<Value> arrayValues =
              collection.stream().map(this::mapObjectToValue).collect(Collectors.toList());
          return makeValue(arrayValues).build();
        }
        throw new IllegalStateException(
            "No conversion exists from type: " + value.getClass() + " to DataStove Value.");
      }
    }
  }
}
