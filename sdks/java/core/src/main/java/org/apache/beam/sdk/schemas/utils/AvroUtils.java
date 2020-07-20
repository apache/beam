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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.AvroCoder.JodaTimestampConversion;
import org.apache.beam.sdk.schemas.AvroRecordSchema;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForSetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Duplication;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.TypeCreation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** Utils to convert AVRO records to Beam rows. */
@Experimental(Kind.SCHEMAS)
public class AvroUtils {
  static {
    // This works around a bug in the Avro library (AVRO-1891) around SpecificRecord's handling
    // of DateTime types.
    SpecificData.get().addLogicalTypeConversion(new JodaTimestampConversion());
    GenericData.get().addLogicalTypeConversion(new JodaTimestampConversion());
  }

  // Unwrap an AVRO schema into the base type an whether it is nullable.
  static class TypeWithNullability {
    public final org.apache.avro.Schema type;
    public final boolean nullable;

    TypeWithNullability(org.apache.avro.Schema avroSchema) {
      if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        List<org.apache.avro.Schema> types = avroSchema.getTypes();

        // optional fields in AVRO have form of:
        // {"name": "foo", "type": ["null", "something"]}

        // don't need recursion because nested unions aren't supported in AVRO
        List<org.apache.avro.Schema> nonNullTypes =
            types.stream()
                .filter(x -> x.getType() != org.apache.avro.Schema.Type.NULL)
                .collect(Collectors.toList());

        if (nonNullTypes.size() == types.size() || nonNullTypes.isEmpty()) {
          // union without `null` or all 'null' union, keep as is.
          type = avroSchema;
          nullable = false;
        } else if (nonNullTypes.size() > 1) {
          type = org.apache.avro.Schema.createUnion(nonNullTypes);
          nullable = true;
        } else {
          // One non-null type.
          type = nonNullTypes.get(0);
          nullable = true;
        }
      } else {
        type = avroSchema;
        nullable = false;
      }
    }
  }

  /** Wrapper for fixed byte fields. */
  public static class FixedBytesField {
    private final int size;

    private FixedBytesField(int size) {
      this.size = size;
    }

    /** Create a {@link FixedBytesField} with the specified size. */
    public static FixedBytesField withSize(int size) {
      return new FixedBytesField(size);
    }

    /** Create a {@link FixedBytesField} from a Beam {@link FieldType}. */
    public static @Nullable FixedBytesField fromBeamFieldType(FieldType fieldType) {
      if (fieldType.getTypeName().isLogicalType()
          && fieldType.getLogicalType().getIdentifier().equals(FixedBytes.IDENTIFIER)) {
        int length = fieldType.getLogicalType(FixedBytes.class).getLength();
        return new FixedBytesField(length);
      } else {
        return null;
      }
    }

    /** Create a {@link FixedBytesField} from an AVRO type. */
    public static @Nullable FixedBytesField fromAvroType(org.apache.avro.Schema type) {
      if (type.getType().equals(Type.FIXED)) {
        return new FixedBytesField(type.getFixedSize());
      } else {
        return null;
      }
    }

    /** Get the size. */
    public int getSize() {
      return size;
    }

    /** Convert to a Beam type. */
    public FieldType toBeamType() {
      return Schema.FieldType.logicalType(FixedBytes.of(size));
    }

    /** Convert to an AVRO type. */
    public org.apache.avro.Schema toAvroType(String name, String namespace) {
      return org.apache.avro.Schema.createFixed(name, null, namespace, size);
    }
  }

  public static class AvroConvertType extends ConvertType {
    public AvroConvertType(boolean returnRawType) {
      super(returnRawType);
    }

    @Override
    protected java.lang.reflect.Type convertDefault(TypeDescriptor<?> type) {
      if (type.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
        return byte[].class;
      } else {
        return super.convertDefault(type);
      }
    }
  }

  public static class AvroConvertValueForGetter extends ConvertValueForGetter {
    AvroConvertValueForGetter(StackManipulation readValue) {
      super(readValue);
    }

    @Override
    protected TypeConversionsFactory getFactory() {
      return new AvroTypeConversionFactory();
    }

    @Override
    protected StackManipulation convertDefault(TypeDescriptor<?> type) {
      if (type.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
        // Generate the following code:
        // return value.bytes();
        return new Compound(
            readValue,
            MethodInvocation.invoke(
                new ForLoadedType(GenericFixed.class)
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.named("bytes")
                            .and(ElementMatchers.returns(new ForLoadedType(byte[].class))))
                    .getOnly()));
      }
      return super.convertDefault(type);
    }
  }

  public static class AvroConvertValueForSetter extends ConvertValueForSetter {
    AvroConvertValueForSetter(StackManipulation readValue) {
      super(readValue);
    }

    @Override
    protected TypeConversionsFactory getFactory() {
      return new AvroTypeConversionFactory();
    }

    @Override
    protected StackManipulation convertDefault(TypeDescriptor<?> type) {
      final ForLoadedType byteArrayType = new ForLoadedType(byte[].class);
      if (type.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
        // Generate the following code:
        // return new T((byte[]) value);
        ForLoadedType loadedType = new ForLoadedType(type.getRawType());
        return new Compound(
            TypeCreation.of(loadedType),
            Duplication.SINGLE,
            // Load the parameter and cast it to a byte[].
            readValue,
            TypeCasting.to(byteArrayType),
            // Create a new instance that wraps this byte[].
            MethodInvocation.invoke(
                loadedType
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.isConstructor()
                            .and(ElementMatchers.takesArguments(byteArrayType)))
                    .getOnly()));
      }
      return super.convertDefault(type);
    }
  }

  static class AvroTypeConversionFactory implements TypeConversionsFactory {

    @Override
    public TypeConversion<java.lang.reflect.Type> createTypeConversion(boolean returnRawTypes) {
      return new AvroConvertType(returnRawTypes);
    }

    @Override
    public TypeConversion<StackManipulation> createGetterConversions(StackManipulation readValue) {
      return new AvroConvertValueForGetter(readValue);
    }

    @Override
    public TypeConversion<StackManipulation> createSetterConversions(StackManipulation readValue) {
      return new AvroConvertValueForSetter(readValue);
    }
  }

  /** Get Beam Field from avro Field. */
  public static Schema.Field toBeamField(org.apache.avro.Schema.Field field) {
    TypeWithNullability nullableType = new TypeWithNullability(field.schema());
    FieldType beamFieldType = toFieldType(nullableType);
    return Field.of(field.name(), beamFieldType);
  }

  /** Get Avro Field from Beam Field. */
  public static org.apache.avro.Schema.Field toAvroField(Schema.Field field, String namespace) {
    org.apache.avro.Schema fieldSchema =
        getFieldSchema(field.getType(), field.getName(), namespace);
    return new org.apache.avro.Schema.Field(
        field.getName(), fieldSchema, field.getDescription(), (Object) null);
  }

  private AvroUtils() {}

  /**
   * Converts AVRO schema to Beam row schema.
   *
   * @param schema schema of type RECORD
   */
  public static Schema toBeamSchema(org.apache.avro.Schema schema) {
    Schema.Builder builder = Schema.builder();

    for (org.apache.avro.Schema.Field field : schema.getFields()) {
      Field beamField = toBeamField(field);
      if (field.doc() != null) {
        beamField = beamField.withDescription(field.doc());
      }
      builder.addField(beamField);
    }

    return builder.build();
  }

  /** Converts a Beam Schema into an AVRO schema. */
  public static org.apache.avro.Schema toAvroSchema(
      Schema beamSchema, @Nullable String name, @Nullable String namespace) {
    final String schemaName = Strings.isNullOrEmpty(name) ? "topLevelRecord" : name;
    final String schemaNamespace = namespace == null ? "" : namespace;
    String childNamespace =
        !"".equals(schemaNamespace) ? schemaNamespace + "." + schemaName : schemaName;
    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : beamSchema.getFields()) {
      org.apache.avro.Schema.Field recordField = toAvroField(field, childNamespace);
      fields.add(recordField);
    }
    return org.apache.avro.Schema.createRecord(schemaName, null, schemaNamespace, false, fields);
  }

  public static org.apache.avro.Schema toAvroSchema(Schema beamSchema) {
    return toAvroSchema(beamSchema, null, null);
  }

  /**
   * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
   * conversion. If Schema is not provided, one is inferred from the AVRO schema.
   */
  public static Row toBeamRowStrict(GenericRecord record, @Nullable Schema schema) {
    if (schema == null) {
      schema = toBeamSchema(record.getSchema());
    }

    Row.Builder builder = Row.withSchema(schema);
    org.apache.avro.Schema avroSchema = record.getSchema();

    for (Schema.Field field : schema.getFields()) {
      Object value = record.get(field.getName());
      org.apache.avro.Schema fieldAvroSchema = avroSchema.getField(field.getName()).schema();
      builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType()));
    }

    return builder.build();
  }

  /**
   * Convert from a Beam Row to an AVRO GenericRecord. If a Schema is not provided, one is inferred
   * from the Beam schema on the row.
   */
  public static GenericRecord toGenericRecord(
      Row row, org.apache.avro.@Nullable Schema avroSchema) {
    Schema beamSchema = row.getSchema();
    // Use the provided AVRO schema if present, otherwise infer an AVRO schema from the row
    // schema.
    if (avroSchema != null && avroSchema.getFields().size() != beamSchema.getFieldCount()) {
      throw new IllegalArgumentException(
          "AVRO schema doesn't match row schema. Row schema "
              + beamSchema
              + ". AVRO schema + "
              + avroSchema);
    }
    if (avroSchema == null) {
      avroSchema = toAvroSchema(beamSchema);
    }

    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    for (int i = 0; i < beamSchema.getFieldCount(); ++i) {
      Schema.Field field = beamSchema.getField(i);
      builder.set(
          field.getName(),
          genericFromBeamField(
              field.getType(), avroSchema.getField(field.getName()).schema(), row.getValue(i)));
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  public static <T> SerializableFunction<T, Row> getToRowFunction(
      Class<T> clazz, org.apache.avro.@Nullable Schema schema) {
    if (GenericRecord.class.equals(clazz)) {
      Schema beamSchema = toBeamSchema(schema);
      return (SerializableFunction<T, Row>) getGenericRecordToRowFunction(beamSchema);
    } else {
      return new AvroRecordSchema().toRowFunction(TypeDescriptor.of(clazz));
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> SerializableFunction<Row, T> getFromRowFunction(Class<T> clazz) {
    return (GenericRecord.class.equals(clazz))
        ? (SerializableFunction<Row, T>) getRowToGenericRecordFunction(null)
        : new AvroRecordSchema().fromRowFunction(TypeDescriptor.of(clazz));
  }

  public static @Nullable <T> Schema getSchema(
      Class<T> clazz, org.apache.avro.@Nullable Schema schema) {
    if (schema != null) {
      return schema.getType().equals(Type.RECORD) ? toBeamSchema(schema) : null;
    }
    if (GenericRecord.class.equals(clazz)) {
      throw new IllegalArgumentException("No schema provided for getSchema(GenericRecord)");
    }
    return new AvroRecordSchema().schemaFor(TypeDescriptor.of(clazz));
  }

  /**
   * Returns a function mapping AVRO {@link GenericRecord}s to Beam {@link Row}s for use in {@link
   * org.apache.beam.sdk.values.PCollection#setSchema}.
   */
  public static SerializableFunction<GenericRecord, Row> getGenericRecordToRowFunction(
      @Nullable Schema schema) {
    return new GenericRecordToRowFn(schema);
  }

  private static class GenericRecordToRowFn implements SerializableFunction<GenericRecord, Row> {
    private final Schema schema;

    GenericRecordToRowFn(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Row apply(GenericRecord input) {
      return toBeamRowStrict(input, schema);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      GenericRecordToRowFn that = (GenericRecordToRowFn) other;
      return schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema);
    }
  }

  /**
   * Returns a function mapping Beam {@link Row}s to AVRO {@link GenericRecord}s for use in {@link
   * org.apache.beam.sdk.values.PCollection#setSchema}.
   */
  public static SerializableFunction<Row, GenericRecord> getRowToGenericRecordFunction(
      org.apache.avro.@Nullable Schema avroSchema) {
    return new RowToGenericRecordFn(avroSchema);
  }

  private static class RowToGenericRecordFn implements SerializableFunction<Row, GenericRecord> {
    private transient org.apache.avro.Schema avroSchema;

    RowToGenericRecordFn(org.apache.avro.@Nullable Schema avroSchema) {
      this.avroSchema = avroSchema;
    }

    @Override
    public GenericRecord apply(Row input) {
      return toGenericRecord(input, avroSchema);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      RowToGenericRecordFn that = (RowToGenericRecordFn) other;
      return avroSchema.equals(that.avroSchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(avroSchema);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      final String avroSchemaAsString = (avroSchema == null) ? null : avroSchema.toString();
      out.writeObject(avroSchemaAsString);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      final String avroSchemaAsString = (String) in.readObject();
      avroSchema =
          (avroSchemaAsString == null)
              ? null
              : new org.apache.avro.Schema.Parser().parse(avroSchemaAsString);
    }
  }

  /**
   * Returns an {@code SchemaCoder} instance for the provided element type.
   *
   * @param <T> the element type
   */
  public static <T> SchemaCoder<T> schemaCoder(TypeDescriptor<T> type) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) type.getRawType();
    org.apache.avro.Schema avroSchema = new ReflectData(clazz.getClassLoader()).getSchema(clazz);
    Schema beamSchema = toBeamSchema(avroSchema);
    return SchemaCoder.of(
        beamSchema, type, getToRowFunction(clazz, avroSchema), getFromRowFunction(clazz));
  }

  /**
   * Returns an {@code SchemaCoder} instance for the provided element class.
   *
   * @param <T> the element type
   */
  public static <T> SchemaCoder<T> schemaCoder(Class<T> clazz) {
    return schemaCoder(TypeDescriptor.of(clazz));
  }

  /**
   * Returns an {@code SchemaCoder} instance for the Avro schema. The implicit type is
   * GenericRecord.
   */
  public static SchemaCoder<GenericRecord> schemaCoder(org.apache.avro.Schema schema) {
    Schema beamSchema = toBeamSchema(schema);
    return SchemaCoder.of(
        beamSchema,
        TypeDescriptor.of(GenericRecord.class),
        getGenericRecordToRowFunction(beamSchema),
        getRowToGenericRecordFunction(schema));
  }

  /**
   * Returns an {@code SchemaCoder} instance for the provided element type using the provided Avro
   * schema.
   *
   * <p>If the type argument is GenericRecord, the schema may be arbitrary. Otherwise, the schema
   * must correspond to the type provided.
   *
   * @param <T> the element type
   */
  public static <T> SchemaCoder<T> schemaCoder(Class<T> clazz, org.apache.avro.Schema schema) {
    return SchemaCoder.of(
        getSchema(clazz, schema),
        TypeDescriptor.of(clazz),
        getToRowFunction(clazz, schema),
        getFromRowFunction(clazz));
  }

  /**
   * Returns an {@code SchemaCoder} instance based on the provided AvroCoder for the element type.
   *
   * @param <T> the element type
   */
  public static <T> SchemaCoder<T> schemaCoder(AvroCoder<T> avroCoder) {
    return schemaCoder(avroCoder.getType(), avroCoder.getSchema());
  }

  private static final class AvroSpecificRecordFieldValueTypeSupplier
      implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      throw new RuntimeException("Unexpected call.");
    }

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz, Schema schema) {
      Map<String, String> mapping = getMapping(schema);
      List<FieldValueTypeInformation> types = Lists.newArrayList();
      for (Method method : ReflectUtils.getMethods(clazz)) {
        if (ReflectUtils.isGetter(method)) {
          FieldValueTypeInformation fieldValueTypeInformation =
              FieldValueTypeInformation.forGetter(method);
          String name = mapping.get(fieldValueTypeInformation.getName());
          if (name != null) {
            types.add(fieldValueTypeInformation.withName(name));
          }
        }
      }

      // Return the list ordered by the schema fields.
      return StaticSchemaInference.sortBySchema(types, schema);
    }

    private Map<String, String> getMapping(Schema schema) {
      Map<String, String> mapping = Maps.newHashMap();
      for (Field field : schema.getFields()) {
        String underscore = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field.getName());
        mapping.put(underscore, field.getName());
        // The Avro compiler might add a $ at the end of a getter to disambiguate.
        mapping.put(underscore + "$", field.getName());
        // If the field is in camel case already, then it's the identity mapping.
        mapping.put(field.getName(), field.getName());
      }
      return mapping;
    }
  }

  private static final class AvroPojoFieldValueTypeSupplier implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      Map<String, FieldValueTypeInformation> types = Maps.newHashMap();
      for (java.lang.reflect.Field f : ReflectUtils.getFields(clazz)) {
        if (!f.isAnnotationPresent(AvroIgnore.class)) {
          FieldValueTypeInformation typeInformation = FieldValueTypeInformation.forField(f);
          AvroName avroname = f.getAnnotation(AvroName.class);
          if (avroname != null) {
            typeInformation = typeInformation.withName(avroname.value());
          }
          types.put(typeInformation.getName(), typeInformation);
        }
      }
      return Lists.newArrayList(types.values());
    }
  }

  /** Get field types for an AVRO-generated SpecificRecord or a POJO. */
  public static <T> List<FieldValueTypeInformation> getFieldTypes(Class<T> clazz, Schema schema) {
    if (TypeDescriptor.of(clazz).isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return JavaBeanUtils.getFieldTypes(
          clazz, schema, new AvroSpecificRecordFieldValueTypeSupplier());
    } else {
      return POJOUtils.getFieldTypes(clazz, schema, new AvroPojoFieldValueTypeSupplier());
    }
  }

  /** Get generated getters for an AVRO-generated SpecificRecord or a POJO. */
  public static <T> List<FieldValueGetter> getGetters(Class<T> clazz, Schema schema) {
    if (TypeDescriptor.of(clazz).isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return JavaBeanUtils.getGetters(
          clazz,
          schema,
          new AvroSpecificRecordFieldValueTypeSupplier(),
          new AvroTypeConversionFactory());
    } else {
      return POJOUtils.getGetters(
          clazz, schema, new AvroPojoFieldValueTypeSupplier(), new AvroTypeConversionFactory());
    }
  }

  /** Get an object creator for an AVRO-generated SpecificRecord. */
  public static <T> SchemaUserTypeCreator getCreator(Class<T> clazz, Schema schema) {
    if (TypeDescriptor.of(clazz).isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return AvroByteBuddyUtils.getCreator((Class<? extends SpecificRecord>) clazz, schema);
    } else {
      return POJOUtils.getSetFieldCreator(
          clazz, schema, new AvroPojoFieldValueTypeSupplier(), new AvroTypeConversionFactory());
    }
  }

  /** Converts AVRO schema to Beam field. */
  private static Schema.FieldType toFieldType(TypeWithNullability type) {
    Schema.FieldType fieldType = null;
    org.apache.avro.Schema avroSchema = type.type;

    LogicalType logicalType = LogicalTypes.fromSchema(avroSchema);
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        fieldType = FieldType.DECIMAL;
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        // TODO: There is a desire to move Beam schema DATETIME to a micros representation. When
        // this is done, this logical type needs to be changed.
        fieldType = FieldType.DATETIME;
      } else if (logicalType instanceof LogicalTypes.Date) {
        fieldType = FieldType.DATETIME;
      }
    }

    if (fieldType == null) {
      switch (type.type.getType()) {
        case RECORD:
          fieldType = Schema.FieldType.row(toBeamSchema(avroSchema));
          break;

        case ENUM:
          fieldType = FieldType.logicalType(EnumerationType.create(type.type.getEnumSymbols()));
          break;

        case ARRAY:
          Schema.FieldType elementType =
              toFieldType(new TypeWithNullability(avroSchema.getElementType()));
          fieldType = Schema.FieldType.array(elementType);
          break;

        case MAP:
          fieldType =
              Schema.FieldType.map(
                  Schema.FieldType.STRING,
                  toFieldType(new TypeWithNullability(avroSchema.getValueType())));
          break;

        case FIXED:
          fieldType = FixedBytesField.fromAvroType(type.type).toBeamType();
          break;

        case STRING:
          fieldType = Schema.FieldType.STRING;
          break;

        case BYTES:
          fieldType = Schema.FieldType.BYTES;
          break;

        case INT:
          fieldType = Schema.FieldType.INT32;
          break;

        case LONG:
          fieldType = Schema.FieldType.INT64;
          break;

        case FLOAT:
          fieldType = Schema.FieldType.FLOAT;
          break;

        case DOUBLE:
          fieldType = Schema.FieldType.DOUBLE;
          break;

        case BOOLEAN:
          fieldType = Schema.FieldType.BOOLEAN;
          break;

        case UNION:
          throw new IllegalArgumentException("Union types not yet supported");

        case NULL:
          throw new IllegalArgumentException("Can't convert 'null' to FieldType");

        default:
          throw new AssertionError("Unexpected AVRO Schema.Type: " + avroSchema.getType());
      }
    }
    fieldType = fieldType.withNullable(type.nullable);
    return fieldType;
  }

  private static org.apache.avro.Schema getFieldSchema(
      Schema.FieldType fieldType, String fieldName, String namespace) {
    org.apache.avro.Schema baseType;
    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
        baseType = org.apache.avro.Schema.create(Type.INT);
        break;

      case INT64:
        baseType = org.apache.avro.Schema.create(Type.LONG);
        break;

      case DECIMAL:
        baseType =
            LogicalTypes.decimal(Integer.MAX_VALUE)
                .addToSchema(org.apache.avro.Schema.create(Type.BYTES));
        break;

      case FLOAT:
        baseType = org.apache.avro.Schema.create(Type.FLOAT);
        break;

      case DOUBLE:
        baseType = org.apache.avro.Schema.create(Type.DOUBLE);
        break;

      case STRING:
        baseType = org.apache.avro.Schema.create(Type.STRING);
        break;

      case DATETIME:
        // TODO: There is a desire to move Beam schema DATETIME to a micros representation. When
        // this is done, this logical type needs to be changed.
        baseType =
            LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(Type.LONG));
        break;

      case BOOLEAN:
        baseType = org.apache.avro.Schema.create(Type.BOOLEAN);
        break;

      case BYTES:
        baseType = org.apache.avro.Schema.create(Type.BYTES);
        break;

      case LOGICAL_TYPE:
        switch (fieldType.getLogicalType().getIdentifier()) {
          case FixedBytes.IDENTIFIER:
            FixedBytesField fixedBytesField =
                checkNotNull(FixedBytesField.fromBeamFieldType(fieldType));
            baseType = fixedBytesField.toAvroType("fixed", namespace + "." + fieldName);
            break;
          case EnumerationType.IDENTIFIER:
            EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
            baseType =
                org.apache.avro.Schema.createEnum(fieldName, "", "", enumerationType.getValues());
            break;
          default:
            throw new RuntimeException(
                "Unhandled logical type " + fieldType.getLogicalType().getIdentifier());
        }
        break;

      case ARRAY:
      case ITERABLE:
        baseType =
            org.apache.avro.Schema.createArray(
                getFieldSchema(fieldType.getCollectionElementType(), fieldName, namespace));
        break;

      case MAP:
        if (fieldType.getMapKeyType().getTypeName().isStringType()) {
          // Avro only supports string keys in maps.
          baseType =
              org.apache.avro.Schema.createMap(
                  getFieldSchema(fieldType.getMapValueType(), fieldName, namespace));
        } else {
          throw new IllegalArgumentException("Avro only supports maps with string keys");
        }
        break;

      case ROW:
        baseType = toAvroSchema(fieldType.getRowSchema(), fieldName, namespace);
        break;

      default:
        throw new IllegalArgumentException("Unexpected type " + fieldType);
    }
    return fieldType.getNullable() ? ReflectData.makeNullable(baseType) : baseType;
  }

  private static @Nullable Object genericFromBeamField(
      Schema.FieldType fieldType, org.apache.avro.Schema avroSchema, @Nullable Object value) {
    TypeWithNullability typeWithNullability = new TypeWithNullability(avroSchema);
    if (!fieldType.getNullable().equals(typeWithNullability.nullable)) {
      throw new IllegalArgumentException(
          "FieldType "
              + fieldType
              + " and AVRO schema "
              + avroSchema
              + " don't have matching nullability");
    }

    if (value == null) {
      return value;
    }

    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return value;

      case STRING:
        return new Utf8((String) value);

      case DECIMAL:
        BigDecimal decimal = (BigDecimal) value;
        LogicalType logicalType = typeWithNullability.type.getLogicalType();
        return new Conversions.DecimalConversion().toBytes(decimal, null, logicalType);

      case DATETIME:
        if (typeWithNullability.type.getType() == Type.INT) {
          ReadableInstant instant = (ReadableInstant) value;
          return (int) Days.daysBetween(Instant.EPOCH, instant).getDays();
        } else if (typeWithNullability.type.getType() == Type.LONG) {
          ReadableInstant instant = (ReadableInstant) value;
          return (long) instant.getMillis();
        } else {
          throw new IllegalArgumentException(
              "Can't represent " + fieldType + " as " + typeWithNullability.type.getType());
        }

      case BYTES:
        return ByteBuffer.wrap((byte[]) value);

      case LOGICAL_TYPE:
        switch (fieldType.getLogicalType().getIdentifier()) {
          case FixedBytes.IDENTIFIER:
            FixedBytesField fixedBytesField =
                checkNotNull(FixedBytesField.fromBeamFieldType(fieldType));
            byte[] byteArray = (byte[]) value;
            if (byteArray.length != fixedBytesField.getSize()) {
              throw new IllegalArgumentException("Incorrectly sized byte array.");
            }
            return GenericData.get().createFixed(null, (byte[]) value, typeWithNullability.type);
          case EnumerationType.IDENTIFIER:
            EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
            return GenericData.get()
                .createEnum(
                    enumerationType.toString((EnumerationType.Value) value),
                    typeWithNullability.type);
          default:
            throw new RuntimeException(
                "Unhandled logical type " + fieldType.getLogicalType().getIdentifier());
        }

      case ARRAY:
      case ITERABLE:
        Iterable iterable = (Iterable) value;
        List<Object> translatedArray = Lists.newArrayListWithExpectedSize(Iterables.size(iterable));

        for (Object arrayElement : iterable) {
          translatedArray.add(
              genericFromBeamField(
                  fieldType.getCollectionElementType(),
                  typeWithNullability.type.getElementType(),
                  arrayElement));
        }
        return translatedArray;

      case MAP:
        Map map = Maps.newHashMap();
        Map<Object, Object> valueMap = (Map<Object, Object>) value;
        for (Map.Entry entry : valueMap.entrySet()) {
          Utf8 key = new Utf8((String) entry.getKey());
          map.put(
              key,
              genericFromBeamField(
                  fieldType.getMapValueType(),
                  typeWithNullability.type.getValueType(),
                  entry.getValue()));
        }
        return map;

      case ROW:
        return toGenericRecord((Row) value, typeWithNullability.type);

      default:
        throw new IllegalArgumentException("Unsupported type " + fieldType);
    }
  }

  /**
   * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
   * conversion.
   *
   * @param value {@link GenericRecord} or any nested value
   * @param avroSchema schema for value
   * @param fieldType target beam field type
   * @return value converted for {@link Row}
   */
  @SuppressWarnings("unchecked")
  public static @Nullable Object convertAvroFieldStrict(
      @Nullable Object value,
      @Nonnull org.apache.avro.Schema avroSchema,
      @Nonnull Schema.FieldType fieldType) {
    if (value == null) {
      return null;
    }

    TypeWithNullability type = new TypeWithNullability(avroSchema);
    LogicalType logicalType = LogicalTypes.fromSchema(type.type);
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        BigDecimal bigDecimal =
            new Conversions.DecimalConversion()
                .fromBytes(byteBuffer.duplicate(), type.type, logicalType);
        return convertDecimal(bigDecimal, fieldType);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        if (value instanceof ReadableInstant) {
          return convertDateTimeStrict(((ReadableInstant) value).getMillis(), fieldType);
        } else {
          return convertDateTimeStrict((Long) value, fieldType);
        }
      } else if (logicalType instanceof LogicalTypes.Date) {
        if (value instanceof ReadableInstant) {
          int epochDays = Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
          return convertDateStrict(epochDays, fieldType);
        } else {
          return convertDateStrict((Integer) value, fieldType);
        }
      }
    }

    switch (type.type.getType()) {
      case FIXED:
        return convertFixedStrict((GenericFixed) value, fieldType);

      case BYTES:
        return convertBytesStrict((ByteBuffer) value, fieldType);

      case STRING:
        return convertStringStrict((CharSequence) value, fieldType);

      case INT:
        return convertIntStrict((Integer) value, fieldType);

      case LONG:
        return convertLongStrict((Long) value, fieldType);

      case FLOAT:
        return convertFloatStrict((Float) value, fieldType);

      case DOUBLE:
        return convertDoubleStrict((Double) value, fieldType);

      case BOOLEAN:
        return convertBooleanStrict((Boolean) value, fieldType);

      case RECORD:
        return convertRecordStrict((GenericRecord) value, fieldType);

      case ENUM:
        // enums are either Java enums, or GenericEnumSymbol,
        // they don't share common interface, but override toString()
        return convertEnumStrict(value, fieldType);

      case ARRAY:
        return convertArrayStrict((List<Object>) value, type.type.getElementType(), fieldType);

      case MAP:
        return convertMapStrict(
            (Map<CharSequence, Object>) value, type.type.getValueType(), fieldType);

      case UNION:
        throw new IllegalArgumentException("Union types not yet supported");

      case NULL:
        throw new IllegalArgumentException("Can't convert 'null' to non-nullable field");

      default:
        throw new AssertionError("Unexpected AVRO Schema.Type: " + type.type.getType());
    }
  }

  private static Object convertRecordStrict(GenericRecord record, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.ROW, "record");
    return toBeamRowStrict(record, fieldType.getRowSchema());
  }

  private static Object convertBytesStrict(ByteBuffer bb, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.BYTES, "bytes");

    byte[] bytes = new byte[bb.remaining()];
    bb.duplicate().get(bytes);
    return bytes;
  }

  private static Object convertFixedStrict(GenericFixed fixed, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.LOGICAL_TYPE, "fixed");
    checkArgument(FixedBytes.IDENTIFIER.equals(fieldType.getLogicalType().getIdentifier()));
    return fixed.bytes().clone(); // clone because GenericFixed is mutable
  }

  private static Object convertStringStrict(CharSequence value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.STRING, "string");
    return value.toString();
  }

  private static Object convertIntStrict(Integer value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.INT32, "int");
    return value;
  }

  private static Object convertLongStrict(Long value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.INT64, "long");
    return value;
  }

  private static Object convertDecimal(BigDecimal value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DECIMAL, "decimal");
    return value;
  }

  private static Object convertDateStrict(Integer epochDays, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DATETIME, "date");
    return Instant.EPOCH.plus(Duration.standardDays(epochDays));
  }

  private static Object convertDateTimeStrict(Long value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DATETIME, "dateTime");
    return new Instant(value);
  }

  private static Object convertFloatStrict(Float value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.FLOAT, "float");
    return value;
  }

  private static Object convertDoubleStrict(Double value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.DOUBLE, "double");
    return value;
  }

  private static Object convertBooleanStrict(Boolean value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.BOOLEAN, "boolean");
    return value;
  }

  private static Object convertEnumStrict(Object value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.LOGICAL_TYPE, "enum");
    checkArgument(fieldType.getLogicalType().getIdentifier().equals(EnumerationType.IDENTIFIER));
    EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
    return enumerationType.valueOf(value.toString());
  }

  private static Object convertArrayStrict(
      List<Object> values, org.apache.avro.Schema elemAvroSchema, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.ARRAY, "array");

    List<Object> ret = new ArrayList<>(values.size());
    Schema.FieldType elemFieldType = fieldType.getCollectionElementType();

    for (Object value : values) {
      ret.add(convertAvroFieldStrict(value, elemAvroSchema, elemFieldType));
    }

    return ret;
  }

  private static Object convertMapStrict(
      Map<CharSequence, Object> values,
      org.apache.avro.Schema valueAvroSchema,
      Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.MAP, "map");
    checkNotNull(fieldType.getMapKeyType());
    checkNotNull(fieldType.getMapValueType());

    if (!fieldType.getMapKeyType().equals(Schema.FieldType.STRING)) {
      throw new IllegalArgumentException(
          "Can't convert 'string' map keys to " + fieldType.getMapKeyType());
    }

    Map<Object, Object> ret = new HashMap<>();

    for (Map.Entry<CharSequence, Object> value : values.entrySet()) {
      ret.put(
          convertStringStrict(value.getKey(), fieldType.getMapKeyType()),
          convertAvroFieldStrict(value.getValue(), valueAvroSchema, fieldType.getMapValueType()));
    }

    return ret;
  }

  private static void checkTypeName(Schema.TypeName got, Schema.TypeName expected, String label) {
    checkArgument(
        got.equals(expected),
        "Can't convert '" + label + "' to " + got + ", expected: " + expected);
  }
}
