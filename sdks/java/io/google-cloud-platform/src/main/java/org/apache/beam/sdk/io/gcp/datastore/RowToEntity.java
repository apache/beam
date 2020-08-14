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
package org.apache.beam.sdk.io.gcp.datastore;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@code PTransform} to perform a conversion of {@link Row} to {@link Entity}. */
public class RowToEntity extends PTransform<PCollection<Row>, PCollection<Entity>> {
  private final Supplier<String> keySupplier;
  private final String kind;
  private final String keyField;
  private static final Logger LOG = LoggerFactory.getLogger(DataStoreV1SchemaIOProvider.class);

  private RowToEntity(Supplier<String> keySupplier, String kind, String keyField) {
    this.keySupplier = keySupplier;
    this.kind = kind;
    this.keyField = keyField;
  }

  @Override
  public PCollection<Entity> expand(PCollection<Row> input) {
    boolean isFieldPresent = input.getSchema().getFieldNames().contains(keyField);
    if (isFieldPresent) {
      if (!input
          .getSchema()
          .getField(keyField)
          .getType()
          .getTypeName()
          .equals(Schema.TypeName.BYTES)) {
        throw new IllegalStateException(
            "Field `"
                + keyField
                + "` should of type `VARBINARY`. Please change the type or specify a field to"
                + " write the KEY value from via TableProperties.");
      }
      LOG.info("Field to use as Entity KEY is set to: `" + keyField + "`.");
    }
    return input.apply(ParDo.of(new RowToEntity.RowToEntityConverter(isFieldPresent)));
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

  public static RowToEntity createTest(String keyString, String keyField, String kind) {
    return new RowToEntity((Supplier<String> & Serializable) () -> keyString, kind, keyField);
  }

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
    private com.google.datastore.v1.Key constructKeyFromRow(Row row) {
      if (!useNonRandomKey) {
        // When key field is not present - use key supplier to generate a random one.
        return makeKey(kind, keySupplier.get()).build();
      }
      byte[] keyBytes = row.getBytes(keyField);
      try {
        return com.google.datastore.v1.Key.parseFrom(keyBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException("Failed to parse DataStore key from bytes.");
      }
    }

    /**
     * Converts a {@code Row} value to an appropriate DataStore {@code Value} object.
     *
     * @param value {@code Row} value to convert.
     * @return resulting {@code Value}.
     * @throws IllegalStateException when no mapping function for object of given type exists.
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
