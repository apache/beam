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

import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

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
 *  {@literal @}Override
 *   public abstract T decode(InputStream inStream) {
 *     // Delegate to a method that evaluates each coder in the static array.
 *     return decodeDelegate(FIELD_CODERS, inStream);
 *   }
 * }
 * </code></pre>
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class RowCoderGenerator {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final BitSetCoder NULL_LIST_CODER = BitSetCoder.of();
  private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();
  // BitSet.get(n) will return false for any n >= nbits, so a BitSet with 0 bits will return false
  // for all calls to get.
  private static final BitSet EMPTY_BIT_SET = new BitSet(0);

  private static final String CODERS_FIELD_NAME = "FIELD_CODERS";
  private static final String POSITIONS_FIELD_NAME = "FIELD_ENCODING_POSITIONS";

  // Cache for Coder class that are already generated.
  private static final Map<UUID, Coder<Row>> GENERATED_CODERS = Maps.newConcurrentMap();
  private static final Map<UUID, Map<String, Integer>> ENCODING_POSITION_OVERRIDES =
      Maps.newConcurrentMap();

  public static void overrideEncodingPositions(UUID uuid, Map<String, Integer> encodingPositions) {
    ENCODING_POSITION_OVERRIDES.put(uuid, encodingPositions);
  }

  @SuppressWarnings("unchecked")
  public static Coder<Row> generate(Schema schema) {
    // Using ConcurrentHashMap::computeIfAbsent here would deadlock in case of nested
    // coders. Using HashMap::computeIfAbsent generates ConcurrentModificationExceptions in Java 11.
    Coder<Row> rowCoder = GENERATED_CODERS.get(schema.getUUID());
    if (rowCoder == null) {
      TypeDescription.Generic coderType =
          TypeDescription.Generic.Builder.parameterizedType(Coder.class, Row.class).build();
      DynamicType.Builder<Coder> builder =
          (DynamicType.Builder<Coder>) BYTE_BUDDY.subclass(coderType);
      builder = implementMethods(schema, builder);

      int[] encodingPosToRowIndex = new int[schema.getFieldCount()];
      Map<String, Integer> encodingPositions =
          ENCODING_POSITION_OVERRIDES.getOrDefault(schema.getUUID(), schema.getEncodingPositions());
      for (int recordIndex = 0; recordIndex < schema.getFieldCount(); ++recordIndex) {
        String name = schema.getField(recordIndex).getName();
        int encodingPosition = encodingPositions.get(name);
        encodingPosToRowIndex[encodingPosition] = recordIndex;
      }
      // There should never be duplicate encoding positions.
      Preconditions.checkState(
          schema.getFieldCount() == Arrays.stream(encodingPosToRowIndex).distinct().count());

      // Component coders are ordered by encoding position, but may encode a field with a different
      // row index.
      Coder[] componentCoders = new Coder[schema.getFieldCount()];
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        int rowIndex = encodingPosToRowIndex[i];
        // We use withNullable(false) as nulls are handled by the RowCoder and the individual
        // component coders therefore do not need to handle nulls.
        componentCoders[i] =
            SchemaCoder.coderForFieldType(schema.getField(rowIndex).getType().withNullable(false));
      }

      builder =
          builder
              .defineField(
                  CODERS_FIELD_NAME, Coder[].class, Visibility.PRIVATE, FieldManifestation.FINAL)
              .defineField(
                  POSITIONS_FIELD_NAME, int[].class, Visibility.PRIVATE, FieldManifestation.FINAL)
              .defineConstructor(Modifier.PUBLIC)
              .withParameters(Coder[].class, int[].class)
              .intercept(new GeneratedCoderConstructor());

      try {
        rowCoder =
            builder
                .make()
                .load(
                    ReflectHelpers.findClassLoader(Coder.class.getClassLoader()),
                    getClassLoadingStrategy(Coder.class))
                .getLoaded()
                .getDeclaredConstructor(Coder[].class, int[].class)
                .newInstance((Object) componentCoders, (Object) encodingPosToRowIndex);
      } catch (InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        throw new RuntimeException("Unable to generate coder for schema " + schema, e);
      }
      GENERATED_CODERS.put(schema.getUUID(), rowCoder);
    }
    return rowCoder;
  }

  private static class GeneratedCoderConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                // Call the base constructor.
                MethodVariableAccess.loadThis(),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    new ForLoadedType(Coder.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                Duplication.SINGLE,
                // Store the list of Coders as a member variable.
                MethodVariableAccess.REFERENCE.loadFrom(1),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                            .getOnly())
                    .write(),
                // Store the list of encoding offsets as a member variable.
                MethodVariableAccess.REFERENCE.loadFrom(2),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(POSITIONS_FIELD_NAME))
                            .getOnly())
                    .write(),
                MethodReturn.VOID);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
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
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                            .getOnly())
                    .read(),
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(POSITIONS_FIELD_NAME))
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
        Coder[] coders,
        int[] encodingPosToIndex,
        Row value,
        OutputStream outputStream,
        boolean hasNullableFields)
        throws IOException {
      checkState(value.getFieldCount() == value.getSchema().getFieldCount());
      checkState(encodingPosToIndex.length == value.getFieldCount());

      // Encode the field count. This allows us to handle compatible schema changes.
      VAR_INT_CODER.encode(value.getFieldCount(), outputStream);

      if (hasNullableFields) {
        // If the row has null fields, extract the values out once so that both scanNullFields and
        // the encoding can share it and avoid having to extract them twice.

        Object[] fieldValues = new Object[value.getFieldCount()];
        for (int idx = 0; idx < fieldValues.length; ++idx) {
          fieldValues[idx] = value.getValue(idx);
        }

        // Encode a bitmap for the null fields to save having to encode a bunch of nulls.
        NULL_LIST_CODER.encode(scanNullFields(fieldValues), outputStream);
        for (int encodingPos = 0; encodingPos < fieldValues.length; ++encodingPos) {
          @Nullable Object fieldValue = fieldValues[encodingPosToIndex[encodingPos]];
          if (fieldValue != null) {
            coders[encodingPos].encode(fieldValue, outputStream);
          }
        }
      } else {
        // Otherwise, we know all fields are non-null, so the null list is always empty.

        NULL_LIST_CODER.encode(EMPTY_BIT_SET, outputStream);
        for (int encodingPos = 0; encodingPos < value.getFieldCount(); ++encodingPos) {
          @Nullable Object fieldValue = value.getValue(encodingPosToIndex[encodingPos]);
          if (fieldValue != null) {
            coders[encodingPos].encode(fieldValue, outputStream);
          }
        }
      }
    }

    // Figure out which fields of the Row are null, and returns a BitSet. This allows us to save
    // on encoding each null field separately.
    private static BitSet scanNullFields(Object[] fieldValues) {
      BitSet nullFields = new BitSet(fieldValues.length);
      for (int idx = 0; idx < fieldValues.length; ++idx) {
        if (fieldValues[idx] == null) {
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
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(CODERS_FIELD_NAME))
                            .getOnly())
                    .read(),
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationContext
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(POSITIONS_FIELD_NAME))
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
    static Row decodeDelegate(
        Schema schema, Coder[] coders, int[] encodingPosToIndex, InputStream inputStream)
        throws IOException {
      int fieldCount = VAR_INT_CODER.decode(inputStream);

      BitSet nullFields = NULL_LIST_CODER.decode(inputStream);
      Object[] fieldValues = new Object[coders.length];
      for (int encodingPos = 0; encodingPos < fieldCount; ++encodingPos) {
        // In the case of a schema change going backwards, fieldCount might be > coders.length,
        // in which case we drop the extra fields.
        if (encodingPos < coders.length) {
          int rowIndex = encodingPosToIndex[encodingPos];
          if (nullFields.get(rowIndex)) {
            fieldValues[rowIndex] = null;
          } else {
            Object fieldValue = coders[encodingPos].decode(inputStream);
            fieldValues[rowIndex] = fieldValue;
          }
        }
      }
      // If the schema was evolved to contain more fields, we fill them in with nulls.
      for (int encodingPos = fieldCount; encodingPos < coders.length; encodingPos++) {
        int rowIndex = encodingPosToIndex[encodingPos];
        fieldValues[rowIndex] = null;
      }
      // We call attachValues instead of setValues. setValues validates every element in the list
      // is of the proper type, potentially converts to the internal type Row stores, and copies
      // all values. Since we assume that decode is always being called on a previously-encoded
      // Row, the values should already be validated and of the correct type. So, we can save
      // some processing by simply transferring ownership of the list to the Row.
      return Row.withSchema(schema).attachValues(fieldValues);
    }
  }
}
