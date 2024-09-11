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
package org.apache.beam.sdk.extensions.avro.schemas.utils;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.implementation.bytecode.Division;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.Multiplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.constant.LongConstant;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.avro.AvroRuntimeException;
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
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.AvroRecordSchema;
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
import org.apache.beam.sdk.schemas.logicaltypes.FixedString;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForGetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForSetter;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversion;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.TypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.POJOUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/**
 * Utils to convert AVRO records to Beam rows. Imposes a mapping between common avro types and Beam
 * portable schemas (https://s.apache.org/beam-schemas):
 *
 * <pre>
 *   Avro                Beam Field Type
 *   INT         <-----> INT32
 *   LONG        <-----> INT64
 *   FLOAT       <-----> FLOAT
 *   DOUBLE      <-----> DOUBLE
 *   BOOLEAN     <-----> BOOLEAN
 *   STRING      <-----> STRING
 *   BYTES       <-----> BYTES
 *               <------ LogicalType(urn="beam:logical_type:var_bytes:v1")
 *   FIXED       <-----> LogicalType(urn="beam:logical_type:fixed_bytes:v1")
 *   ARRAY       <-----> ARRAY
 *   ENUM        <-----> LogicalType(EnumerationType)
 *   MAP         <-----> MAP
 *   RECORD      <-----> ROW
 *   UNION       <-----> LogicalType(OneOfType)
 *   LogicalTypes.Date              <-----> LogicalType(DATE)
 *                                  <------ LogicalType(urn="beam:logical_type:date:v1")
 *   LogicalTypes.TimestampMillis   <-----> DATETIME
 *   LogicalTypes.Decimal           <-----> DECIMAL
 * </pre>
 *
 * For SQL CHAR/VARCHAR types, an Avro schema
 *
 * <pre>
 *   LogicalType({"type":"string","logicalType":"char","maxLength":MAX_LENGTH}) or
 *   LogicalType({"type":"string","logicalType":"varchar","maxLength":MAX_LENGTH})
 * </pre>
 *
 * is used.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class AvroUtils {
  private static final ForLoadedType BYTES = new ForLoadedType(byte[].class);
  private static final ForLoadedType JAVA_INSTANT = new ForLoadedType(java.time.Instant.class);
  private static final ForLoadedType JAVA_LOCALE_DATE =
      new ForLoadedType(java.time.LocalDate.class);
  private static final ForLoadedType JODA_READABLE_INSTANT =
      new ForLoadedType(ReadableInstant.class);
  private static final ForLoadedType JODA_INSTANT = new ForLoadedType(Instant.class);

  public static void addLogicalTypeConversions(final GenericData data) {
    // do not add DecimalConversion by default as schema must have extra 'scale' and 'precision'
    // properties. avro reflect already handles BigDecimal as string with the 'java-class' property
    data.addLogicalTypeConversion(new Conversions.UUIDConversion());
    // joda-time
    data.addLogicalTypeConversion(new AvroJodaTimeConversions.DateConversion());
    data.addLogicalTypeConversion(new AvroJodaTimeConversions.TimeConversion());
    data.addLogicalTypeConversion(new AvroJodaTimeConversions.TimeMicrosConversion());
    data.addLogicalTypeConversion(new AvroJodaTimeConversions.TimestampConversion());
    data.addLogicalTypeConversion(new AvroJodaTimeConversions.TimestampMicrosConversion());
    // java-time
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.DateConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.TimeMillisConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.TimeMicrosConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.TimestampMillisConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.TimestampMicrosConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.LocalTimestampMillisConversion());
    data.addLogicalTypeConversion(new AvroJavaTimeConversions.LocalTimestampMicrosConversion());
  }

  // Unwrap an AVRO schema into the base type an whether it is nullable.
  public static class TypeWithNullability {
    public final org.apache.avro.Schema type;
    public final boolean nullable;

    public static TypeWithNullability create(org.apache.avro.Schema avroSchema) {
      return new TypeWithNullability(avroSchema);
    }

    TypeWithNullability(org.apache.avro.Schema avroSchema) {
      if (avroSchema.getType() == Type.UNION) {
        List<org.apache.avro.Schema> types = avroSchema.getTypes();

        // optional fields in AVRO have form of:
        // {"name": "foo", "type": ["null", "something"]}

        // don't need recursion because nested unions aren't supported in AVRO
        List<org.apache.avro.Schema> nonNullTypes =
            types.stream().filter(x -> x.getType() != Type.NULL).collect(Collectors.toList());

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

    public Boolean isNullable() {
      return nullable;
    }

    public org.apache.avro.Schema getType() {
      return type;
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
      return FieldType.logicalType(FixedBytes.of(size));
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
      if (type.isSubtypeOf(TypeDescriptor.of(java.time.Instant.class))
          || type.isSubtypeOf(TypeDescriptor.of(java.time.LocalDate.class))) {
        return convertDateTime(type);
      } else if (type.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
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
                    .filter(ElementMatchers.named("bytes").and(ElementMatchers.returns(BYTES)))
                    .getOnly()));
      } else if (java.time.Instant.class.isAssignableFrom(type.getRawType())) {
        // Generates the following code:
        //   return Instant.ofEpochMilli(value.toEpochMilli())
        StackManipulation onNotNull =
            new Compound(
                readValue,
                MethodInvocation.invoke(
                    JAVA_INSTANT
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("toEpochMilli"))
                        .getOnly()),
                MethodInvocation.invoke(
                    JODA_INSTANT
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic().and(ElementMatchers.named("ofEpochMilli")))
                        .getOnly()));
        return shortCircuitReturnNull(readValue, onNotNull);
      } else if (java.time.LocalDate.class.isAssignableFrom(type.getRawType())) {
        // Generates the following code:
        //   return Instant.ofEpochMilli(value.toEpochDay() * TimeUnit.DAYS.toMillis(1))
        StackManipulation onNotNull =
            new Compound(
                readValue,
                MethodInvocation.invoke(
                    JAVA_LOCALE_DATE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("toEpochDay"))
                        .getOnly()),
                LongConstant.forValue(TimeUnit.DAYS.toMillis(1)),
                Multiplication.LONG,
                MethodInvocation.invoke(
                    JODA_INSTANT
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic().and(ElementMatchers.named("ofEpochMilli")))
                        .getOnly()));
        return shortCircuitReturnNull(readValue, onNotNull);
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
      if (type.isSubtypeOf(TypeDescriptor.of(GenericFixed.class))) {
        // Generate the following code:
        //   return new T((byte[]) value);
        ForLoadedType loadedType = new ForLoadedType(type.getRawType());
        return new Compound(
            TypeCreation.of(loadedType),
            Duplication.SINGLE,
            // Load the parameter and cast it to a byte[].
            readValue,
            TypeCasting.to(BYTES),
            // Create a new instance that wraps this byte[].
            MethodInvocation.invoke(
                loadedType
                    .getDeclaredMethods()
                    .filter(
                        ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(BYTES)))
                    .getOnly()));
      } else if (java.time.Instant.class.isAssignableFrom(type.getRawType())) {
        // Generates the following code:
        //   return java.time.Instant.ofEpochMilli(value.getMillis())
        StackManipulation onNotNull =
            new Compound(
                readValue,
                MethodInvocation.invoke(
                    JODA_READABLE_INSTANT
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getMillis"))
                        .getOnly()),
                MethodInvocation.invoke(
                    JAVA_INSTANT
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic().and(ElementMatchers.named("ofEpochMilli")))
                        .getOnly()));
        return shortCircuitReturnNull(readValue, onNotNull);
      } else if (java.time.LocalDate.class.isAssignableFrom(type.getRawType())) {
        // Generates the following code:
        //   return java.time.LocalDate.ofEpochDay(value.getMillis() / TimeUnit.DAYS.toMillis(1))
        StackManipulation onNotNull =
            new Compound(
                readValue,
                MethodInvocation.invoke(
                    JODA_READABLE_INSTANT
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getMillis"))
                        .getOnly()),
                LongConstant.forValue(TimeUnit.DAYS.toMillis(1)),
                Division.LONG,
                MethodInvocation.invoke(
                    JAVA_LOCALE_DATE
                        .getDeclaredMethods()
                        .filter(ElementMatchers.isStatic().and(ElementMatchers.named("ofEpochDay")))
                        .getOnly()));
        return shortCircuitReturnNull(readValue, onNotNull);
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
  public static Field toBeamField(org.apache.avro.Schema.Field field) {
    TypeWithNullability nullableType = new TypeWithNullability(field.schema());
    FieldType beamFieldType = toFieldType(nullableType);
    return Field.of(field.name(), beamFieldType);
  }

  /** Get Avro Field from Beam Field. */
  public static org.apache.avro.Schema.Field toAvroField(Field field, String namespace) {
    org.apache.avro.Schema fieldSchema =
        getFieldSchema(field.getType(), field.getName(), namespace);
    return new org.apache.avro.Schema.Field(
        field.getName(), fieldSchema, field.getDescription(), (Object) null);
  }

  private AvroUtils() {}

  /**
   * Converts AVRO schema to Beam row schema.
   *
   * @param clazz avro class
   */
  public static Schema toBeamSchema(Class<?> clazz) {
    ReflectData data = new ReflectData(clazz.getClassLoader());
    return toBeamSchema(data.getSchema(clazz));
  }

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
    for (Field field : beamSchema.getFields()) {
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

    for (Field field : schema.getFields()) {
      Object value = record.get(field.getName());
      org.apache.avro.Schema fieldAvroSchema = avroSchema.getField(field.getName()).schema();
      builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType()));
    }

    return builder.build();
  }

  /**
   * Convert from a Beam Row to an AVRO GenericRecord. The Avro Schema is inferred from the Beam
   * schema on the row.
   */
  public static GenericRecord toGenericRecord(Row row) {
    return toGenericRecord(row, null);
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
      Field field = beamSchema.getField(i);
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
    return GenericRecord.class.equals(clazz)
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

  /** Returns a function mapping encoded AVRO {@link GenericRecord}s to Beam {@link Row}s. */
  public static SimpleFunction<byte[], Row> getAvroBytesToRowFunction(Schema beamSchema) {
    return new AvroBytesToRowFn(beamSchema);
  }

  private static class AvroBytesToRowFn extends SimpleFunction<byte[], Row> {
    private final AvroCoder<GenericRecord> coder;
    private final Schema beamSchema;

    AvroBytesToRowFn(Schema beamSchema) {
      org.apache.avro.Schema avroSchema = toAvroSchema(beamSchema);
      coder = AvroCoder.of(avroSchema);
      this.beamSchema = beamSchema;
    }

    @Override
    public Row apply(byte[] bytes) {
      try {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        GenericRecord record = coder.decode(inputStream);
        return AvroUtils.toBeamRowStrict(record, beamSchema);
      } catch (Exception e) {
        throw new AvroRuntimeException(
            "Could not decode avro record from given bytes "
                + new String(bytes, StandardCharsets.UTF_8),
            e);
      }
    }
  }

  /** Returns a function mapping Beam {@link Row}s to encoded AVRO {@link GenericRecord}s. */
  public static SimpleFunction<Row, byte[]> getRowToAvroBytesFunction(Schema beamSchema) {
    return new RowToAvroBytesFn(beamSchema);
  }

  private static class RowToAvroBytesFn extends SimpleFunction<Row, byte[]> {
    private final org.apache.avro.Schema avroSchema;
    private final AvroCoder<GenericRecord> coder;

    RowToAvroBytesFn(Schema beamSchema) {
      avroSchema = toAvroSchema(beamSchema);
      coder = AvroCoder.of(avroSchema);
    }

    @Override
    public byte[] apply(Row row) {
      try {
        GenericRecord record = toGenericRecord(row, avroSchema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        coder.encode(record, outputStream);
        return outputStream.toByteArray();
      } catch (Exception e) {
        throw new AvroRuntimeException(
            String.format("Could not encode avro from given row: %s", row), e);
      }
    }
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
      return Objects.equals(this.schema, that.schema);
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
      return Objects.equals(this.avroSchema, that.avroSchema);
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
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      throw new RuntimeException("Unexpected call.");
    }

    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor, Schema schema) {
      Map<String, String> mapping = getMapping(schema);
      List<Method> methods = ReflectUtils.getMethods(typeDescriptor.getRawType());
      List<FieldValueTypeInformation> types = Lists.newArrayList();
      for (int i = 0; i < methods.size(); ++i) {
        Method method = methods.get(i);
        if (ReflectUtils.isGetter(method)) {
          FieldValueTypeInformation fieldValueTypeInformation =
              FieldValueTypeInformation.forGetter(method, i);
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
        String fieldName = field.getName();
        String getter;
        if (fieldName.contains("_")) {
          if (Character.isLowerCase(fieldName.charAt(0))) {
            // field_name -> fieldName
            getter = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, fieldName);
          } else {
            // FIELD_NAME -> fIELDNAME
            // must remove underscore and then convert to match compiled Avro schema getter name
            getter = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, fieldName.replace("_", ""));
          }
        } else if (Character.isUpperCase(fieldName.charAt(0))) {
          // FieldName -> fieldName
          getter = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, fieldName);
        } else {
          // If the field is in camel case already, then it's the identity mapping.
          getter = fieldName;
        }
        mapping.put(getter, fieldName);
        // The Avro compiler might add a $ at the end of a getter to disambiguate.
        mapping.put(getter + "$", fieldName);
      }
      return mapping;
    }
  }

  private static final class AvroPojoFieldValueTypeSupplier implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      List<java.lang.reflect.Field> classFields =
          ReflectUtils.getFields(typeDescriptor.getRawType());
      Map<String, FieldValueTypeInformation> types = Maps.newHashMap();
      for (int i = 0; i < classFields.size(); ++i) {
        java.lang.reflect.Field f = classFields.get(i);
        if (!f.isAnnotationPresent(AvroIgnore.class)) {
          FieldValueTypeInformation typeInformation = FieldValueTypeInformation.forField(f, i);
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
  public static <T> List<FieldValueTypeInformation> getFieldTypes(
      TypeDescriptor<T> typeDescriptor, Schema schema) {
    if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return JavaBeanUtils.getFieldTypes(
          typeDescriptor, schema, new AvroSpecificRecordFieldValueTypeSupplier());
    } else {
      return POJOUtils.getFieldTypes(typeDescriptor, schema, new AvroPojoFieldValueTypeSupplier());
    }
  }

  /** Get generated getters for an AVRO-generated SpecificRecord or a POJO. */
  public static <T> List<FieldValueGetter> getGetters(
      TypeDescriptor<T> typeDescriptor, Schema schema) {
    if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return JavaBeanUtils.getGetters(
          typeDescriptor,
          schema,
          new AvroSpecificRecordFieldValueTypeSupplier(),
          new AvroTypeConversionFactory());
    } else {
      return POJOUtils.getGetters(
          typeDescriptor,
          schema,
          new AvroPojoFieldValueTypeSupplier(),
          new AvroTypeConversionFactory());
    }
  }

  /** Get an object creator for an AVRO-generated SpecificRecord. */
  public static <T> SchemaUserTypeCreator getCreator(
      TypeDescriptor<T> typeDescriptor, Schema schema) {
    if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(SpecificRecord.class))) {
      return AvroByteBuddyUtils.getCreator(
          (Class<? extends SpecificRecord>) typeDescriptor.getRawType(), schema);
    } else {
      return POJOUtils.getSetFieldCreator(
          typeDescriptor,
          schema,
          new AvroPojoFieldValueTypeSupplier(),
          new AvroTypeConversionFactory());
    }
  }

  /** Converts AVRO schema to Beam field. */
  private static FieldType toFieldType(TypeWithNullability type) {
    FieldType fieldType = null;
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
          fieldType = FieldType.row(toBeamSchema(avroSchema));
          break;

        case ENUM:
          fieldType = FieldType.logicalType(EnumerationType.create(type.type.getEnumSymbols()));
          break;

        case ARRAY:
          FieldType elementType = toFieldType(new TypeWithNullability(avroSchema.getElementType()));
          fieldType = FieldType.array(elementType);
          break;

        case MAP:
          fieldType =
              FieldType.map(
                  FieldType.STRING,
                  toFieldType(new TypeWithNullability(avroSchema.getValueType())));
          break;

        case FIXED:
          fieldType = FixedBytesField.fromAvroType(type.type).toBeamType();
          break;

        case STRING:
          fieldType = FieldType.STRING;
          break;

        case BYTES:
          fieldType = FieldType.BYTES;
          break;

        case INT:
          fieldType = FieldType.INT32;
          break;

        case LONG:
          fieldType = FieldType.INT64;
          break;

        case FLOAT:
          fieldType = FieldType.FLOAT;
          break;

        case DOUBLE:
          fieldType = FieldType.DOUBLE;
          break;

        case BOOLEAN:
          fieldType = FieldType.BOOLEAN;
          break;

        case UNION:
          fieldType =
              FieldType.logicalType(
                  OneOfType.create(
                      avroSchema.getTypes().stream()
                          .map(x -> Field.of(x.getName(), toFieldType(new TypeWithNullability(x))))
                          .collect(Collectors.toList())));
          break;
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
      FieldType fieldType, String fieldName, String namespace) {
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
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (FixedBytes.IDENTIFIER.equals(identifier)) {
          FixedBytesField fixedBytesField =
              checkNotNull(FixedBytesField.fromBeamFieldType(fieldType));
          baseType = fixedBytesField.toAvroType("fixed", namespace + "." + fieldName);
        } else if (VariableBytes.IDENTIFIER.equals(identifier)) {
          // treat VARBINARY as bytes as that is what avro supports
          baseType = org.apache.avro.Schema.create(Type.BYTES);
        } else if (FixedString.IDENTIFIER.equals(identifier)
            || "CHAR".equals(identifier)
            || "NCHAR".equals(identifier)) {
          baseType =
              buildHiveLogicalTypeSchema("char", (int) fieldType.getLogicalType().getArgument());
        } else if (VariableString.IDENTIFIER.equals(identifier)
            || "NVARCHAR".equals(identifier)
            || "VARCHAR".equals(identifier)
            || "LONGNVARCHAR".equals(identifier)
            || "LONGVARCHAR".equals(identifier)) {
          baseType =
              buildHiveLogicalTypeSchema("varchar", (int) fieldType.getLogicalType().getArgument());
        } else if (EnumerationType.IDENTIFIER.equals(identifier)) {
          EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
          baseType =
              org.apache.avro.Schema.createEnum(fieldName, "", "", enumerationType.getValues());
        } else if (OneOfType.IDENTIFIER.equals(identifier)) {
          OneOfType oneOfType = fieldType.getLogicalType(OneOfType.class);
          baseType =
              org.apache.avro.Schema.createUnion(
                  oneOfType.getOneOfSchema().getFields().stream()
                      .map(x -> getFieldSchema(x.getType(), x.getName(), namespace))
                      .collect(Collectors.toList()));
        } else if ("DATE".equals(identifier) || SqlTypes.DATE.getIdentifier().equals(identifier)) {
          baseType = LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(Type.INT));
        } else if ("TIME".equals(identifier)) {
          baseType = LogicalTypes.timeMillis().addToSchema(org.apache.avro.Schema.create(Type.INT));
        } else {
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
      FieldType fieldType, org.apache.avro.Schema avroSchema, @Nullable Object value) {
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
        String identifier = fieldType.getLogicalType().getIdentifier();
        if (FixedBytes.IDENTIFIER.equals(identifier)) {
          FixedBytesField fixedBytesField =
              checkNotNull(FixedBytesField.fromBeamFieldType(fieldType));
          byte[] byteArray = (byte[]) value;
          if (byteArray.length != fixedBytesField.getSize()) {
            throw new IllegalArgumentException("Incorrectly sized byte array.");
          }
          return GenericData.get().createFixed(null, (byte[]) value, typeWithNullability.type);
        } else if (VariableBytes.IDENTIFIER.equals(identifier)) {
          return GenericData.get().createFixed(null, (byte[]) value, typeWithNullability.type);
        } else if (FixedString.IDENTIFIER.equals(identifier)
            || "CHAR".equals(identifier)
            || "NCHAR".equals(identifier)) {
          return new Utf8((String) value);
        } else if (VariableString.IDENTIFIER.equals(identifier)
            || "NVARCHAR".equals(identifier)
            || "VARCHAR".equals(identifier)
            || "LONGNVARCHAR".equals(identifier)
            || "LONGVARCHAR".equals(identifier)) {
          return new Utf8((String) value);
        } else if (EnumerationType.IDENTIFIER.equals(identifier)) {
          EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
          return GenericData.get()
              .createEnum(
                  enumerationType.toString((EnumerationType.Value) value),
                  typeWithNullability.type);
        } else if (OneOfType.IDENTIFIER.equals(identifier)) {
          OneOfType oneOfType = fieldType.getLogicalType(OneOfType.class);
          OneOfType.Value oneOfValue = (OneOfType.Value) value;
          FieldType innerFieldType = oneOfType.getFieldType(oneOfValue);
          if (typeWithNullability.nullable && oneOfValue.getValue() == null) {
            return null;
          } else {
            return genericFromBeamField(
                innerFieldType.withNullable(false),
                typeWithNullability.type.getTypes().get(oneOfValue.getCaseType().getValue()),
                oneOfValue.getValue());
          }
        } else if ("DATE".equals(identifier)) {
          // "Date" is backed by joda.time.Instant
          return Days.daysBetween(Instant.EPOCH, (Instant) value).getDays();
        } else if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          // portable SqlTypes.DATE is backed by java.time.LocalDate
          return ((java.time.LocalDate) value).toEpochDay();
        } else if ("TIME".equals(identifier)) {
          return (int) ((Instant) value).getMillis();
        } else {
          throw new RuntimeException("Unhandled logical type " + identifier);
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
      @Nonnull FieldType fieldType) {
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
        } else if (value instanceof java.time.LocalDate) {
          return convertDateStrict((int) ((java.time.LocalDate) value).toEpochDay(), fieldType);
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
        return convertUnionStrict(value, type.type, fieldType);

      case NULL:
        throw new IllegalArgumentException("Can't convert 'null' to non-nullable field");

      default:
        throw new AssertionError("Unexpected AVRO Schema.Type: " + type.type.getType());
    }
  }

  private static Object convertRecordStrict(GenericRecord record, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.ROW, "record");
    return toBeamRowStrict(record, fieldType.getRowSchema());
  }

  private static Object convertBytesStrict(ByteBuffer bb, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.BYTES, "bytes");

    byte[] bytes = new byte[bb.remaining()];
    bb.duplicate().get(bytes);
    return bytes;
  }

  private static Object convertFixedStrict(GenericFixed fixed, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.LOGICAL_TYPE, "fixed");
    checkArgument(FixedBytes.IDENTIFIER.equals(fieldType.getLogicalType().getIdentifier()));
    return fixed.bytes().clone(); // clone because GenericFixed is mutable
  }

  private static Object convertStringStrict(CharSequence value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.STRING, "string");
    return value.toString();
  }

  private static Object convertIntStrict(Integer value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.INT32, "int");
    return value;
  }

  private static Object convertLongStrict(Long value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.INT64, "long");
    return value;
  }

  private static Object convertDecimal(BigDecimal value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DECIMAL, "decimal");
    return value;
  }

  private static Object convertDateStrict(Integer epochDays, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DATETIME, "date");
    return Instant.EPOCH.plus(Duration.standardDays(epochDays));
  }

  private static Object convertDateTimeStrict(Long value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DATETIME, "dateTime");
    return new Instant(value);
  }

  private static Object convertFloatStrict(Float value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.FLOAT, "float");
    return value;
  }

  private static Object convertDoubleStrict(Double value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DOUBLE, "double");
    return value;
  }

  private static Object convertBooleanStrict(Boolean value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.BOOLEAN, "boolean");
    return value;
  }

  private static Object convertEnumStrict(Object value, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.LOGICAL_TYPE, "enum");
    checkArgument(fieldType.getLogicalType().getIdentifier().equals(EnumerationType.IDENTIFIER));
    EnumerationType enumerationType = fieldType.getLogicalType(EnumerationType.class);
    return enumerationType.valueOf(value.toString());
  }

  private static Object convertUnionStrict(
      Object value, org.apache.avro.Schema unionAvroSchema, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.LOGICAL_TYPE, "oneOfType");
    checkArgument(fieldType.getLogicalType().getIdentifier().equals(OneOfType.IDENTIFIER));
    OneOfType oneOfType = fieldType.getLogicalType(OneOfType.class);
    int fieldNumber = GenericData.get().resolveUnion(unionAvroSchema, value);
    FieldType baseFieldType = oneOfType.getOneOfSchema().getField(fieldNumber).getType();
    Object convertedValue =
        convertAvroFieldStrict(value, unionAvroSchema.getTypes().get(fieldNumber), baseFieldType);
    return oneOfType.createValue(fieldNumber, convertedValue);
  }

  private static Object convertArrayStrict(
      List<Object> values, org.apache.avro.Schema elemAvroSchema, FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.ARRAY, "array");

    List<Object> ret = new ArrayList<>(values.size());
    FieldType elemFieldType = fieldType.getCollectionElementType();

    for (Object value : values) {
      ret.add(convertAvroFieldStrict(value, elemAvroSchema, elemFieldType));
    }

    return ret;
  }

  private static Object convertMapStrict(
      Map<CharSequence, Object> values,
      org.apache.avro.Schema valueAvroSchema,
      FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.MAP, "map");
    checkNotNull(fieldType.getMapKeyType());
    checkNotNull(fieldType.getMapValueType());

    if (!fieldType.getMapKeyType().equals(FieldType.STRING)) {
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

  private static void checkTypeName(TypeName got, TypeName expected, String label) {
    checkArgument(
        got.equals(expected), "Can't convert '%s' to %s, expected: %s", label, got, expected);
  }

  /**
   * Helper factory to build Avro Logical types schemas for SQL *CHAR types. This method <a
   * href="https://github.com/apache/hive/blob/5d268834a5f5278ea76399f8af0d0ab043ae0b45/serde/src/java/org/apache/hadoop/hive/serde2/avro/TypeInfoToSchema.java#L110-L121">represents
   * the logical as Hive does</a>.
   */
  private static org.apache.avro.Schema buildHiveLogicalTypeSchema(
      String hiveLogicalType, int size) {
    String schemaJson =
        String.format(
            "{\"type\": \"string\", \"logicalType\": \"%s\", \"maxLength\": %s}",
            hiveLogicalType, size);
    return new org.apache.avro.Schema.Parser().parse(schemaJson);
  }
}
