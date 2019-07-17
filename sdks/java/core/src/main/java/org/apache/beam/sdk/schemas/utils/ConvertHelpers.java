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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.JavaFieldSchema.JavaFieldTypeSupplier;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertValueForSetter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Primitives;

/** Helper functions for converting between equivalent schema types. */
public class ConvertHelpers {
  /** Return value after converting a schema. */
  public static class ConvertedSchemaInformation<T> implements Serializable {
    // If the output type is a composite type, this is the schema coder.
    @Nullable public final SchemaCoder<T> outputSchemaCoder;
    // If the input schema has a single field and the output type's schema matches that field, this
    // is the output type.
    @Nullable public final FieldType unboxedType;

    public ConvertedSchemaInformation(
        @Nullable SchemaCoder<T> outputSchemaCoder, @Nullable FieldType unboxedType) {
      assert outputSchemaCoder != null || unboxedType != null;

      this.outputSchemaCoder = outputSchemaCoder;
      this.unboxedType = unboxedType;
    }
  }

  /** Get the coder used for converting from an inputSchema to a given type. */
  public static <T> ConvertedSchemaInformation<T> getConvertedSchemaInformation(
      Schema inputSchema, TypeDescriptor<T> outputType, SchemaRegistry schemaRegistry) {
    ConvertedSchemaInformation<T> convertedSchema = null;
    boolean toRow = outputType.equals(TypeDescriptor.of(Row.class));
    if (toRow) {
      // If the output is of type Row, then just forward the schema of the input type to the
      // output.
      convertedSchema =
          new ConvertedSchemaInformation<>(
              (SchemaCoder<T>)
                  SchemaCoder.of(
                      inputSchema,
                      SerializableFunctions.identity(),
                      SerializableFunctions.identity()),
              null);
    } else {
      // Otherwise, try to find a schema for the output type in the schema registry.
      Schema outputSchema = null;
      SchemaCoder<T> outputSchemaCoder = null;
      try {
        outputSchema = schemaRegistry.getSchema(outputType);
        outputSchemaCoder =
            SchemaCoder.of(
                outputSchema,
                schemaRegistry.getToRowFunction(outputType),
                schemaRegistry.getFromRowFunction(outputType));
      } catch (NoSuchSchemaException e) {

      }
      FieldType unboxedType = null;
      // TODO: Properly handle nullable.
      if (outputSchema == null || !outputSchema.assignableToIgnoreNullable(inputSchema)) {
        // The schema is not convertible directly. Attempt to unbox it and see if the schema matches
        // then.
        Schema checkedSchema = inputSchema;
        if (inputSchema.getFieldCount() == 1) {
          unboxedType = inputSchema.getField(0).getType();
          if (unboxedType.getTypeName().isCompositeType()
              && !outputSchema.assignableToIgnoreNullable(unboxedType.getRowSchema())) {
            checkedSchema = unboxedType.getRowSchema();
          } else {
            checkedSchema = null;
          }
        }
        if (checkedSchema != null) {
          throw new RuntimeException(
              "Cannot convert between types that don't have equivalent schemas."
                  + " input schema: "
                  + checkedSchema
                  + " output schema: "
                  + outputSchema);
        }
      }
      convertedSchema = new ConvertedSchemaInformation<T>(outputSchemaCoder, unboxedType);
    }
    return convertedSchema;
  }

  /**
   * Returns a function to convert a Row into a primitive type. This only works when the row schema
   * contains a single field, and that field is convertible to the primitive type.
   */
  @SuppressWarnings("unchecked")
  public static <OutputT> SerializableFunction<?, OutputT> getConvertPrimitive(
      FieldType fieldType, TypeDescriptor<?> outputTypeDescriptor) {
    FieldType expectedFieldType =
        StaticSchemaInference.fieldFromType(outputTypeDescriptor, JavaFieldTypeSupplier.INSTANCE);
    if (!expectedFieldType.equals(fieldType)) {
      throw new IllegalArgumentException(
          "Element argument type "
              + outputTypeDescriptor
              + " does not work with expected schema field type "
              + fieldType);
    }

    Type expectedInputType = new ConvertType(true).convert(outputTypeDescriptor);

    TypeDescriptor<?> outputType = outputTypeDescriptor;
    if (outputType.getRawType().isPrimitive()) {
      // A SerializableFunction can only return an Object type, so if the DoFn parameter is a
      // primitive type, then box it for the return. The return type will be unboxed before being
      // forwarded to the DoFn parameter.
      outputType = TypeDescriptor.of(Primitives.wrap(outputType.getRawType()));
    }

    TypeDescription.Generic genericType =
        TypeDescription.Generic.Builder.parameterizedType(
                SerializableFunction.class, expectedInputType, outputType.getType())
            .build();
    DynamicType.Builder<SerializableFunction> builder =
        (DynamicType.Builder<SerializableFunction>) new ByteBuddy().subclass(genericType);
    try {
      return builder
          .method(ElementMatchers.named("apply"))
          .intercept(new ConvertPrimitiveInstruction(outputType))
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  static class ConvertPrimitiveInstruction implements Implementation {
    private final TypeDescriptor<?> outputFieldType;

    public ConvertPrimitiveInstruction(TypeDescriptor<?> outputFieldType) {
      this.outputFieldType = outputFieldType;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        int numLocals = 1 + instrumentedMethod.getParameters().size();

        // Method param is offset 1 (offset 0 is the this parameter).
        StackManipulation readValue = MethodVariableAccess.REFERENCE.loadFrom(1);
        StackManipulation stackManipulation =
            new StackManipulation.Compound(
                new ConvertValueForSetter(readValue).convert(outputFieldType),
                MethodReturn.REFERENCE);

        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }
}
