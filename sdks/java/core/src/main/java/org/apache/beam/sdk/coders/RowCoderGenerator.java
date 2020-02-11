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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.modifier.FieldManifestation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.modifier.Ownership;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.modifier.Visibility;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.FixedValue;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.Duplication;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.TypeCreation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.FieldAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * A utility for automatically generating a {@link Coder} for {@link Row} objects corresponding to a
 * specific schema. The resulting coder is loaded into the default ClassLoader and returned.
 *
 * <p>When {@link RowCoderGenerator#generate(Schema)} is called, a new subclass of {@literal
 * Coder<Row>} is generated for the specified schema. This class is generated using low-level
 * bytecode generation, and hardcodes encodings for all fields of the Schema. Empirically, this is
 * 30-40% faster than a coder that introspects the schema.
 *
 * <p>The generated class corresponds to the following Java class:
 *
 * <pre><code>
 * class SchemaRowCoder extends{@literal Coder<Row>} {
 *   // Generated array containing a coder for each field in the Schema.
 *   private static final Coder[] FIELD_CODERS;
 *
 *   // Generated method to return the schema this class corresponds to. Used during code
 *   // generation.
 *   private static getSchema() {
 *     return schema;
 *   }
 *
 *  {@literal @}Override
 *   public void encode(T value, OutputStream outStream) {
 *     // Delegate to a method that evaluates each coder in the static array.
 *     encodeDelegate(FIELD_CODERS, value, outStream);
 *   }
 *
 *  {@literal @}Overide
 *   public abstract T decode(InputStream inStream) {
 *     // Delegate to a method that evaluates each coder in the static array.
 *     return decodeDelegate(FIELD_CODERS, inStream);
 *   }
 * }
 * </code></pre>
 */
@Experimental(Kind.SCHEMAS)
public abstract class RowCoderGenerator {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final ForLoadedType CODER_TYPE = new ForLoadedType(Coder.class);
  private static final ForLoadedType LIST_CODER_TYPE = new ForLoadedType(ListCoder.class);
  private static final ForLoadedType ITERABLE_CODER_TYPE = new ForLoadedType(IterableCoder.class);
  private static final ForLoadedType MAP_CODER_TYPE = new ForLoadedType(MapCoder.class);
  private static final BitSetCoder NULL_LIST_CODER = BitSetCoder.of();
  private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();
  private static final ForLoadedType NULLABLE_CODER = new ForLoadedType(NullableCoder.class);

  private static final String CODERS_FIELD_NAME = "FIELD_CODERS";

  // A map of primitive types -> StackManipulations to create their coders.
  private static final Map<TypeName, StackManipulation> CODER_MAP;

  // Cache for Coder class that are already generated.
  private static Map<UUID, Coder<Row>> generatedCoders = Maps.newConcurrentMap();

  static {
    // Initialize the CODER_MAP with the StackManipulations to create the primitive coders.
    // Assumes that each class contains a static of() constructor method.
    CODER_MAP = Maps.newHashMap();
    for (Map.Entry<TypeName, Coder> entry : SchemaCoder.CODER_MAP.entrySet()) {
      StackManipulation stackManipulation =
          MethodInvocation.invoke(
              new ForLoadedType(entry.getValue().getClass())
                  .getDeclaredMethods()
                  .filter(ElementMatchers.named("of"))
                  .getOnly());
      CODER_MAP.putIfAbsent(entry.getKey(), stackManipulation);
    }
  }

  @SuppressWarnings("unchecked")
  public static Coder<Row> generate(Schema schema) {
    // Using ConcurrentHashMap::computeIfAbsent here would deadlock in case of nested
    // coders. Using HashMap::computeIfAbsent generates ConcurrentModificationExceptions in Java 11.
    Coder<Row> rowCoder = generatedCoders.get(schema.getUUID());
    if (rowCoder == null) {
      TypeDescription.Generic coderType =
          TypeDescription.Generic.Builder.parameterizedType(Coder.class, Row.class).build();
      DynamicType.Builder<Coder> builder =
          (DynamicType.Builder<Coder>) BYTE_BUDDY.subclass(coderType);
      builder = createComponentCoders(schema, builder);
      builder = implementMethods(schema, builder);
      try {
        rowCoder =
            builder
                .make()
                .load(Coder.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded()
                .getDeclaredConstructor()
                .newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        throw new RuntimeException("Unable to generate coder for schema " + schema);
      }
      generatedCoders.put(schema.getUUID(), rowCoder);
    }
    return rowCoder;
  }

  private static DynamicType.Builder<Coder> implementMethods(
      Schema schema, DynamicType.Builder<Coder> builder) {
    boolean hasNullableFields =
        schema.getFields().stream().map(Field::getType).anyMatch(FieldType::getNullable);
    return builder
        .defineMethod("getSchema", Schema.class, Visibility.PRIVATE, Ownership.STATIC)
        .intercept(FixedValue.reference(schema))
        .defineMethod("hasNullableFields", boolean.class, Visibility.PRIVATE, Ownership.STATIC)
        .intercept(FixedValue.reference(hasNullableFields))
        .method(ElementMatchers.named("encode"))
        .intercept(new EncodeInstruction())
        .method(ElementMatchers.named("decode"))
        .intercept(new DecodeInstruction());
  }

  private static class EncodeInstruction implements Implementation {
    static final ForLoadedType LOADED_TYPE = new ForLoadedType(EncodeInstruction.class);

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        StackManipulation manipulation =
            new StackManipulation.Compound(
                // Array of coders.
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                            .getOnly())
                    .read(),
                // Element to encode. (offset 1, as offset 0 is always "this").
                MethodVariableAccess.REFERENCE.loadFrom(1),
                // OutputStream.
                MethodVariableAccess.REFERENCE.loadFrom(2),
                // hasNullableFields
                MethodInvocation.invoke(
                    implementationContext
                        .getInstrumentedType()
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("hasNullableFields"))
                        .getOnly()),
                // Call EncodeInstruction.encodeDelegate
                MethodInvocation.invoke(
                    LOADED_TYPE
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic().and(ElementMatchers.named("encodeDelegate")))
                        .getOnly()),
                MethodReturn.VOID);
        StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
      };
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    // The encode method of the generated Coder delegates to this method to evaluate all of the
    // per-field Coders.
    @SuppressWarnings("unchecked")
    static void encodeDelegate(
        Coder[] coders, Row value, OutputStream outputStream, boolean hasNullableFields)
        throws IOException {
      checkState(value.getFieldCount() == value.getSchema().getFieldCount());

      // Encode the field count. This allows us to handle compatible schema changes.
      VAR_INT_CODER.encode(value.getFieldCount(), outputStream);
      // Encode a bitmap for the null fields to save having to encode a bunch of nulls.
      NULL_LIST_CODER.encode(scanNullFields(value, hasNullableFields), outputStream);
      for (int idx = 0; idx < value.getFieldCount(); ++idx) {
        Object fieldValue = value.getValue(idx);
        if (value.getValue(idx) != null) {
          coders[idx].encode(fieldValue, outputStream);
        }
      }
    }

    // Figure out which fields of the Row are null, and returns a BitSet. This allows us to save
    // on encoding each null field separately.
    private static BitSet scanNullFields(Row row, boolean hasNullableFields) {
      BitSet nullFields = new BitSet(row.getFieldCount());
      if (hasNullableFields) {
        for (int idx = 0; idx < row.getFieldCount(); ++idx) {
          if (row.getValue(idx) == null) {
            nullFields.set(idx);
          }
        }
      }
      return nullFields;
    }
  }

  private static class DecodeInstruction implements Implementation {
    static final ForLoadedType LOADED_TYPE = new ForLoadedType(DecodeInstruction.class);

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        StackManipulation manipulation =
            new StackManipulation.Compound(
                // Schema. Used in generation of DecodeInstruction.
                MethodInvocation.invoke(
                    implementationContext
                        .getInstrumentedType()
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("getSchema"))
                        .getOnly()),
                // Array of coders.
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                            .getOnly())
                    .read(),
                // read the InputStream. (offset 1, as offset 0 is always "this").
                MethodVariableAccess.REFERENCE.loadFrom(1),
                MethodInvocation.invoke(
                    LOADED_TYPE
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isStatic().and(ElementMatchers.named("decodeDelegate")))
                        .getOnly()),
                MethodReturn.REFERENCE);
        StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
      };
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    // The decode method of the generated Coder delegates to this method to evaluate all of the
    // per-field Coders.
    static Row decodeDelegate(Schema schema, Coder[] coders, InputStream inputStream)
        throws IOException {
      int fieldCount = VAR_INT_CODER.decode(inputStream);

      BitSet nullFields = NULL_LIST_CODER.decode(inputStream);
      List<Object> fieldValues = Lists.newArrayListWithCapacity(coders.length);
      for (int i = 0; i < fieldCount; ++i) {
        // In the case of a schema change going backwards, fieldCount might be > coders.length,
        // in which case we drop the extra fields.
        if (i < coders.length) {
          if (nullFields.get(i)) {
            fieldValues.add(null);
          } else {
            fieldValues.add(coders[i].decode(inputStream));
          }
        }
      }
      // If the schema was evolved to contain more fields, we fill them in with nulls.
      for (int i = fieldCount; i < coders.length; i++) {
        fieldValues.add(null);
      }
      // We call attachValues instead of setValues. setValues validates every element in the list
      // is of the proper type, potentially converts to the internal type Row stores, and copies
      // all values. Since we assume that decode is always being called on a previously-encoded
      // Row, the values should already be validated and of the correct type. So, we can save
      // some processing by simply transferring ownership of the list to the Row.
      return Row.withSchema(schema).attachValues(fieldValues).build();
    }
  }

  private static DynamicType.Builder<Coder> createComponentCoders(
      Schema schema, DynamicType.Builder<Coder> builder) {
    List<StackManipulation> componentCoders =
        Lists.newArrayListWithCapacity(schema.getFieldCount());
    for (int i = 0; i < schema.getFieldCount(); i++) {
      // We use withNullable(false) as nulls are handled by the RowCoder and the individual
      // component coders therefore do not need to handle nulls.
      componentCoders.add(getCoder(schema.getField(i).getType().withNullable(false)));
    }

    return builder
        // private static final Coder[] FIELD_CODERS;
        .defineField(
            CODERS_FIELD_NAME,
            Coder[].class,
            Visibility.PRIVATE,
            Ownership.STATIC,
            FieldManifestation.FINAL)
        // Static initializer.
        .initializer(
            (methodVisitor, implementationContext, instrumentedMethod) -> {
              StackManipulation manipulation =
                  new StackManipulation.Compound(
                      // Initialize the array of coders.
                      ArrayFactory.forType(CODER_TYPE.asGenericType()).withValues(componentCoders),
                      FieldAccess.forField(
                              implementationContext
                                  .getInstrumentedType()
                                  .getDeclaredFields()
                                  .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                                  .getOnly())
                          .write());
              StackManipulation.Size size =
                  manipulation.apply(methodVisitor, implementationContext);
              return new ByteCodeAppender.Size(
                  size.getMaximalSize(), instrumentedMethod.getStackSize());
            });
  }

  private static StackManipulation getCoder(Schema.FieldType fieldType) {
    if (TypeName.LOGICAL_TYPE.equals(fieldType.getTypeName())) {
      return getCoder(fieldType.getLogicalType().getBaseType());
    } else if (TypeName.ARRAY.equals(fieldType.getTypeName())) {
      return listCoder(fieldType.getCollectionElementType());
    } else if (TypeName.ITERABLE.equals(fieldType.getTypeName())) {
      return iterableCoder(fieldType.getCollectionElementType());
    }
    if (TypeName.MAP.equals(fieldType.getTypeName())) {;
      return mapCoder(fieldType.getMapKeyType(), fieldType.getMapValueType());
    } else if (TypeName.ROW.equals(fieldType.getTypeName())) {
      checkState(fieldType.getRowSchema().getUUID() != null);
      Coder<Row> nestedCoder = generate(fieldType.getRowSchema());
      return rowCoder(nestedCoder.getClass());
    } else {
      StackManipulation primitiveCoder = coderForPrimitiveType(fieldType.getTypeName());

      if (fieldType.getNullable()) {
        primitiveCoder =
            new Compound(
                primitiveCoder,
                MethodInvocation.invoke(
                    NULLABLE_CODER
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("of"))
                        .getOnly()));
      }

      return primitiveCoder;
    }
  }

  private static StackManipulation listCoder(Schema.FieldType fieldType) {
    StackManipulation componentCoder = getCoder(fieldType);
    return new Compound(
        componentCoder,
        MethodInvocation.invoke(
            LIST_CODER_TYPE.getDeclaredMethods().filter(ElementMatchers.named("of")).getOnly()));
  }

  private static StackManipulation iterableCoder(Schema.FieldType fieldType) {
    StackManipulation componentCoder = getCoder(fieldType);
    return new Compound(
        componentCoder,
        MethodInvocation.invoke(
            ITERABLE_CODER_TYPE
                .getDeclaredMethods()
                .filter(ElementMatchers.named("of"))
                .getOnly()));
  }

  static StackManipulation coderForPrimitiveType(Schema.TypeName typeName) {
    return CODER_MAP.get(typeName);
  }

  static StackManipulation mapCoder(Schema.FieldType keyType, Schema.FieldType valueType) {
    StackManipulation keyCoder = getCoder(keyType);
    StackManipulation valueCoder = getCoder(valueType);
    return new Compound(
        keyCoder,
        valueCoder,
        MethodInvocation.invoke(
            MAP_CODER_TYPE.getDeclaredMethods().filter(ElementMatchers.named("of")).getOnly()));
  }

  static StackManipulation rowCoder(Class coderClass) {
    ForLoadedType loadedType = new ForLoadedType(coderClass);
    return new Compound(
        TypeCreation.of(loadedType),
        Duplication.SINGLE,
        MethodInvocation.invoke(
            loadedType
                .getDeclaredMethods()
                .filter(ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                .getOnly()));
  }
}
