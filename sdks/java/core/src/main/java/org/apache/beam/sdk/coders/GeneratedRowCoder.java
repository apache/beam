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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import net.bytebuddy.implementation.bytecode.TypeCreation;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;

/**
 * A utility for automatically generating a {@link Coder} for {@link Row} objects corresponding to
 * a specific schema. The resulting coder is loaded into the default ClassLoader and returned.
 *
 */
public abstract class GeneratedRowCoder {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final ForLoadedType CODER_TYPE = new ForLoadedType(Coder.class);
  private static final ForLoadedType LIST_CODER_TYPE = new ForLoadedType(ListCoder.class);
  private static final ForLoadedType MAP_CODER_TYPE = new ForLoadedType(MapCoder.class);
  private static final BitSetCoder NULL_LIST_CODER = BitSetCoder.of();

  public static final String CODERS_FIELD_NAME = "FIELD_CODERS";

  // A map of primitive types -> StackManipulations to create their coders.
  private static final Map<TypeName, StackManipulation> CODER_MAP;

  // Cache for Coder class that are already generated.
  private static Map<UUID, Coder<Row>> generatedCoders = Maps.newHashMap();

  static {
    // Initialize the CODER_MAP with the StackManipulations to create the primitive coders.
    // Assumes that each class contains a static of() constructor method.
    CODER_MAP = Maps.newHashMap();
    for (Map.Entry<TypeName, Coder> entry : RowCoder.CODER_MAP.entrySet()) {
      StackManipulation stackManipulation = MethodInvocation.invoke(
          new ForLoadedType(entry.getValue().getClass()).getDeclaredMethods()
              .filter(ElementMatchers.named(("of")))
              .getOnly());
      CODER_MAP.putIfAbsent(entry.getKey(), stackManipulation);
    }
  }

  public static Coder<Row> of(Schema schema, UUID coderId) {
    return generatedCoders.computeIfAbsent(coderId,
        h -> {
          DynamicType.Builder<Coder> builder = BYTE_BUDDY.subclass(Coder.class);
          builder = createComponentCoders(schema, builder);
          builder = implementMethods(builder, schema);
          try {
            return builder
                .make()
                .load(
                    Coder.class.getClassLoader(),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded()
                .getDeclaredConstructor().newInstance();
          } catch (InstantiationException | IllegalAccessException
              | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Unable to generate coder for schema " + schema);
          }
        });
  }

  static DynamicType.Builder<Coder> implementMethods(
      DynamicType.Builder<Coder> builder, Schema schema) {
    return builder
        .defineMethod("getSchema", Schema.class, Visibility.PRIVATE, Ownership.STATIC)
        .intercept(FixedValue.reference(schema))
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
        StackManipulation manipulation = new StackManipulation.Compound(
            // Array of coders.
            FieldAccess.forField(implementationContext
                .getInstrumentedType()
                .getDeclaredFields()
                .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                .getOnly()).read(),
            // Element to encode.
            MethodVariableAccess.REFERENCE.loadFrom(1),
            TypeCasting.to(new TypeDescription.ForLoadedType(Row.class)),
            // OutputStream.
            MethodVariableAccess.REFERENCE.loadFrom(2),
            // Call EncodeInstruction.encodeDelegate
            MethodInvocation.invoke(LOADED_TYPE
                .getDeclaredMethods()
                .filter(ElementMatchers.isStatic()
                    .and(ElementMatchers.named("encodeDelegate")))
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
    static void encodeDelegate(
        Coder[] coders, Row value, OutputStream outputStream) throws IOException {
      NULL_LIST_CODER.encode(scanNullFields(value), outputStream);
      for (int idx = 0; idx < value.getFieldCount(); ++idx) {
        Object fieldValue = value.getValue(idx);
        if (value.getValue(idx) != null) {
          coders[idx].encode(fieldValue, outputStream);
        }
      }
    }

    // Figure out which fields of the Row are null, and returns a BitSet. This allows us to save
    // on encoding each null field separately.
    private static BitSet scanNullFields(Row row) {
      BitSet nullFields = new BitSet(row.getFieldCount());
      for (int idx = 0; idx < row.getFieldCount(); ++idx) {
        if (row.getValue(idx) == null) {
          nullFields.set(idx);
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
        StackManipulation manipulation = new StackManipulation.Compound(
            // Schema.
            MethodInvocation.invoke(implementationContext
                .getInstrumentedType()
                .getDeclaredMethods()
                .filter(ElementMatchers.named("getSchema"))
                .getOnly()),
            // Array of coders.
            FieldAccess.forField(implementationContext
                .getInstrumentedType()
                .getDeclaredFields()
                .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                .getOnly()).read(),
            // InputStream.
            MethodVariableAccess.REFERENCE.loadFrom(1),
            MethodInvocation.invoke(LOADED_TYPE
                .getDeclaredMethods()
                .filter(ElementMatchers.isStatic()
                    .and(ElementMatchers.named("decodeDelegate")))
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
    static Row decodeDelegate(
        Schema schema, Coder[] coders, InputStream inputStream) throws IOException {
      BitSet nullFields = NULL_LIST_CODER.decode(inputStream);
      List<Object> fieldValues = Lists.newArrayListWithCapacity(coders.length);
      for (int i = 0; i < coders.length; ++i) {
        if (nullFields.get(i)) {
          fieldValues.add(null);
        } else {
          fieldValues.add(coders[i].decode(inputStream));
        }
      }
      return Row.withSchema(schema).attachValues(fieldValues).build();
    }
  }

  static DynamicType.Builder<Coder> createComponentCoders(
      Schema schema, DynamicType.Builder<Coder> builder) {
    List<StackManipulation> componentCoders = Lists.newArrayListWithCapacity(
        schema.getFieldCount());
    for (int i = 0; i < schema.getFieldCount(); i++) {
      componentCoders.add(getCoder(schema.getField(i).getType()));
    }

    return builder
        // private static final Coder[] FIELD_CODERS;
        .defineField(CODERS_FIELD_NAME, Coder[].class,
            Visibility.PRIVATE, Ownership.STATIC, FieldManifestation.FINAL)
        // Static initializer.
        .initializer((methodVisitor, implementationContext, instrumentedMethod) -> {
          StackManipulation manipulation = new StackManipulation.Compound(
              // Initialize the array of coders.
              ArrayFactory.forType(CODER_TYPE.asGenericType()).withValues(componentCoders),
              FieldAccess.forField(implementationContext
                  .getInstrumentedType()
                  .getDeclaredFields()
                  .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                  .getOnly()).write());
          StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
          return new ByteCodeAppender.Size(
              size.getMaximalSize(), instrumentedMethod.getStackSize());
        });
    }

    static StackManipulation getCoder(Schema.FieldType fieldType) {
    if (TypeName.ARRAY.equals(fieldType.getTypeName())) {
        return listCoder(fieldType.getCollectionElementType());
      } else if (TypeName.MAP.equals(fieldType.getTypeName())) {
        return mapCoder(fieldType.getMapKeyType(), fieldType.getMapValueType());
      } else if (TypeName.ROW.equals((fieldType.getTypeName()))) {
        Coder<Row> nestedCoder = of(fieldType.getRowSchema(), UUID.randomUUID());
        return rowCoder(nestedCoder.getClass());
      } else {
        return coderForPrimitiveType(fieldType.getTypeName());
      }
    }

    static StackManipulation listCoder(Schema.FieldType fieldType) {
      StackManipulation componentCoder = getCoder(fieldType);
      return new Compound(
          componentCoder,
          MethodInvocation.invoke(LIST_CODER_TYPE.getDeclaredMethods()
              .filter(ElementMatchers.named("of"))
              .getOnly()));
    }

  static StackManipulation coderForPrimitiveType(Schema.TypeName typeName) {
    return CODER_MAP.get(typeName);
  }

  static StackManipulation mapCoder(Schema.FieldType keyType, Schema.FieldType valueType) {
    StackManipulation keyCoder = coderForPrimitiveType(keyType.getTypeName());
    StackManipulation valueCoder = getCoder(valueType);
    return new Compound(
        keyCoder,
        valueCoder,
        MethodInvocation.invoke(MAP_CODER_TYPE.getDeclaredMethods()
            .filter(ElementMatchers.named("of"))
            .getOnly()));
  }

  static StackManipulation rowCoder(Class coderClass) {
    ForLoadedType loadedType = new ForLoadedType(coderClass);
    return new Compound(
        TypeCreation.of(loadedType),
        Duplication.SINGLE,
        MethodInvocation.invoke(loadedType.getDeclaredMethods()
            .filter(ElementMatchers.isConstructor()
                .and(ElementMatchers.takesArguments(0)))
            .getOnly()));
  }
}
