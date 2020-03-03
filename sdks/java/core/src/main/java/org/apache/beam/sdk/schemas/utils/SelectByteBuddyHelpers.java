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

import com.google.auto.value.AutoValue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.asm.AsmVisitorWrapper;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.FieldManifestation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.Visibility;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.Generic;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Duplication;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Removal;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackSize;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.FieldAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.ClassWriter;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.Label;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.MethodVisitor;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.Opcodes;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

public class SelectByteBuddyHelpers {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final String SELECT_SCHEMA_FIELD_NAME = "OUTPUTSCHEMA";

  private static final ForLoadedType ROW_LOADED_TYPE = new ForLoadedType(Row.class);

  @AutoValue
  abstract static class SchemaAndDescriptor {
    abstract Schema getSchema();

    abstract FieldAccessDescriptor getFieldAccessDecriptor();

    static SchemaAndDescriptor of(Schema schema, FieldAccessDescriptor fieldAccessDescriptor) {
      return new AutoValue_SelectByteBuddyHelpers_SchemaAndDescriptor(
          schema, fieldAccessDescriptor);
    }
  }

  @AutoValue
  abstract static class SizeAndArrayPosition {
    abstract StackManipulation.Size getSize();

    abstract int getArrayPosition();

    static SizeAndArrayPosition of(StackManipulation.Size size, int arrayPosiition) {
      return new AutoValue_SelectByteBuddyHelpers_SizeAndArrayPosition(size, arrayPosiition);
    }
  }

  private static Map<SchemaAndDescriptor, RowSelector> CACHED_SELECTORS = Maps.newConcurrentMap();

  public static RowSelector getRowSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    return CACHED_SELECTORS.computeIfAbsent(
        SchemaAndDescriptor.of(inputSchema, fieldAccessDescriptor),
        SelectByteBuddyHelpers::createRowSelector);
  }

  static RowSelector createRowSelector(SchemaAndDescriptor schemaAndDescriptor) {
    Schema outputSchema =
        SelectHelpers.getOutputSchema(
            schemaAndDescriptor.getSchema(), schemaAndDescriptor.getFieldAccessDecriptor());
    try {
      DynamicType.Builder<RowSelector> builder =
          BYTE_BUDDY
              .subclass(RowSelector.class)
              .method(ElementMatchers.named("select"))
              .intercept(
                  new SelectInstruction(
                      schemaAndDescriptor.getFieldAccessDecriptor(),
                      schemaAndDescriptor.getSchema(),
                      outputSchema))
              .defineField(
                  SELECT_SCHEMA_FIELD_NAME,
                  Schema.class,
                  Visibility.PRIVATE,
                  FieldManifestation.FINAL)
              .defineConstructor(Modifier.PUBLIC)
              .withParameters(Schema.class)
              .intercept(new SelectInstructionConstructor());

      return builder
          .visit(new AsmVisitorWrapper.ForDeclaredMethods().writerFlags(ClassWriter.COMPUTE_FRAMES))
          .make()
          .load(Row.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor(Schema.class)
          .newInstance(outputSchema);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new RuntimeException("Unable to generate");
    }
  }

  private static class SelectInstructionConstructor implements Implementation {
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
                // Call the base constructor for Object.
                MethodVariableAccess.loadThis(),
                Duplication.SINGLE,
                MethodInvocation.invoke(
                    new ForLoadedType(Object.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.isConstructor().and(ElementMatchers.takesArguments(0)))
                        .getOnly()),
                MethodVariableAccess.REFERENCE.loadFrom(1),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(SELECT_SCHEMA_FIELD_NAME))
                            .getOnly())
                    .write(),
                MethodReturn.VOID);
        StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
        return new Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  private static class LocalVariableManager {
    private int nextLocalVariableIndex;

    LocalVariableManager(int numFunctionArgs) {
      nextLocalVariableIndex = numFunctionArgs + 1;
    }

    int createLocalVariable() {
      return nextLocalVariableIndex++;
    }

    StackManipulation readVariable(int variableIndex) {
      checkArgument(variableIndex < nextLocalVariableIndex);
      return MethodVariableAccess.REFERENCE.loadFrom(variableIndex);
    }

    StackManipulation readVariable(int variableIndex, Class<?> type) {
      return new Compound(readVariable(variableIndex), TypeCasting.to(new ForLoadedType(type)));
    }

    StackManipulation writeVariable(int variableIndex) {
      checkArgument(variableIndex < nextLocalVariableIndex);
      return MethodVariableAccess.REFERENCE.storeAt(variableIndex);
    }

    StackManipulation copy(int sourceVariableIndex, int destVariableIndex) {
      return new Compound(readVariable(sourceVariableIndex), writeVariable(destVariableIndex));
    }

    int getTotalNumVariables() {
      return nextLocalVariableIndex;
    }
  }

  private static class SelectInstruction implements Implementation {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private LocalVariableManager localVariables;
    private final StackManipulation.Size sizeDecreaseArrayStore;
    private static final int INPUT_ROW_ARG = 1;
    private final int currentSelectRowArg;
    private final int fieldValueArg;

    private static final int NUM_FUNCTION_ARGS = 2; // Args are this and the row to select.

    public SelectInstruction(
        FieldAccessDescriptor fieldAccessDescriptor, Schema inputSchema, Schema outputSchema) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      this.localVariables = new LocalVariableManager(NUM_FUNCTION_ARGS);
      currentSelectRowArg = localVariables.createLocalVariable();
      fieldValueArg = localVariables.createLocalVariable();
      // Size decreases by index and array reference (2) and array element (1, 2) after each element
      // storage.
      sizeDecreaseArrayStore =
          StackSize.DOUBLE
              .toDecreasingSize()
              .aggregate(Generic.OBJECT.getStackSize().toDecreasingSize());
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        int numLocals = 1 + instrumentedMethod.getParameters().size();
        StackManipulation.Size size = new StackManipulation.Size(0, numLocals);

        // Create the Row builder.
        StackManipulation createRowBuilder =
            new StackManipulation.Compound(
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(SELECT_SCHEMA_FIELD_NAME))
                            .getOnly())
                    .read(),
                MethodInvocation.invoke(
                    new ForLoadedType(Row.class)
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("withSchema"))
                        .getOnly()));
        size = size.aggregate(createRowBuilder.apply(methodVisitor, implementationContext));

        // Create a new object array with one entry for each field in the output schema.
        size =
            size.aggregate(
                IntegerConstant.forValue(outputSchema.getFieldCount())
                    .apply(methodVisitor, implementationContext));
        methodVisitor.visitTypeInsn(
            Opcodes.ANEWARRAY, Generic.OBJECT.asErasure().getInternalName());
        size = size.aggregate(StackSize.ZERO.toDecreasingSize());

        // Store the current input row into a local variable.
        StackManipulation storeRowInLocalVariable =
            localVariables.copy(INPUT_ROW_ARG, currentSelectRowArg);
        size = size.aggregate(storeRowInLocalVariable.apply(methodVisitor, implementationContext));

        // Fill the array values with those selected from the row.
        size =
            size.aggregate(
                selectIntoRow(
                        inputSchema, fieldAccessDescriptor, 0, methodVisitor, implementationContext)
                    .getSize());

        // Return the actual row.
        StackManipulation attachToRow =
            new StackManipulation.Compound(
                MethodInvocation.invoke(
                    new ForLoadedType(Row.Builder.class)
                        .getDeclaredMethods()
                        .filter(
                            ElementMatchers.named("attachValues")
                                .and(ElementMatchers.takesArguments(Object[].class)))
                        .getOnly()),
                MethodInvocation.invoke(
                    new ForLoadedType(Row.Builder.class)
                        .getDeclaredMethods()
                        .filter(ElementMatchers.named("build"))
                        .getOnly()),
                MethodReturn.REFERENCE);
        size = size.aggregate(attachToRow.apply(methodVisitor, implementationContext));
        return new Size(size.getMaximalSize(), localVariables.getTotalNumVariables());
      };
    }

    // Selects a field from the current row being selected (the one stored in
    // currentSelectRowArg).
    private StackManipulation getCurrentRowFieldValue(int i) {
      return new StackManipulation.Compound(
          localVariables.readVariable(currentSelectRowArg, Row.class),
          IntegerConstant.forValue(i),
          MethodInvocation.invoke(
              ROW_LOADED_TYPE
                  .getDeclaredMethods()
                  .filter(
                      ElementMatchers.named("getValue")
                          .and(ElementMatchers.takesArguments(int.class)))
                  .getOnly()));
    }

    private StackManipulation.Size storeIntoArray(
        int arrayIndexToWrite,
        StackManipulation valueToWrite,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation stackManipulation =
          new StackManipulation.Compound(
              Duplication.SINGLE, // Duplicate the array reference
              IntegerConstant.forValue(arrayIndexToWrite),
              valueToWrite);
      StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
      methodVisitor.visitInsn(Opcodes.AASTORE);
      size = size.aggregate(sizeDecreaseArrayStore);
      return size;
    }

    // Generate bytecode to select all specified fields from the Row. The current row being selected
    // is stored
    // in the local variable at position 2. The current array being written to is at the top of the
    // stack.
    SizeAndArrayPosition selectIntoRow(
        Schema inputSchema,
        FieldAccessDescriptor fieldAccessDescriptor,
        int currentArrayField,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      if (fieldAccessDescriptor.getAllFields()) {
        for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
          size =
              size.aggregate(
                  storeIntoArray(
                      currentArrayField,
                      getCurrentRowFieldValue(i),
                      methodVisitor,
                      implementationContext));
          currentArrayField++;
        }
        return SizeAndArrayPosition.of(size, currentArrayField);
      }

      for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
        // TODO: Once we support specific qualifiers (like array slices), extract them here.
        size =
            size.aggregate(
                storeIntoArray(
                    currentArrayField,
                    getCurrentRowFieldValue(fieldId),
                    methodVisitor,
                    implementationContext));
        currentArrayField++;
      }

      for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
          fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
        FieldDescriptor field = nested.getKey();
        FieldAccessDescriptor nestedAccess = nested.getValue();
        FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
        FieldType nestedOutputType = outputSchema.getField(currentArrayField).getType();

        SizeAndArrayPosition sizeAndArrayPosition =
            selectIntoRowWithQualifiers(
                field.getQualifiers(),
                0,
                field.getFieldId(),
                nestedAccess,
                nestedInputType,
                nestedOutputType,
                currentArrayField,
                methodVisitor,
                implementationContext);
        size = size.aggregate(sizeAndArrayPosition.getSize());
        currentArrayField = sizeAndArrayPosition.getArrayPosition();
      }
      return SizeAndArrayPosition.of(size, currentArrayField);
    }

    // Load the current field value to process. If a fieldId is specified, then we assume that we
    // need to select
    // a field from the Row stored in currentSelectRowArg. Otherwise we assume that the value has
    // already been stored
    // in fieldValueArg, and we read from there.
    private StackManipulation loadFieldValue(int fieldId) {
      if (fieldId != -1) {
        // If a field id was specified, then load it.
        return getCurrentRowFieldValue(fieldId);
      } else {
        // If not specified, then assume it was stored in this member variable.
        return localVariables.readVariable(fieldValueArg);
      }
    }

    private SizeAndArrayPosition selectIntoRowWithQualifiers(
        List<Qualifier> qualifiers,
        int qualifierPosition,
        int fieldId,
        FieldAccessDescriptor fieldAccessDescriptor,
        FieldType inputType,
        FieldType outputType,
        int currentArrayField,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);

      if (qualifierPosition >= qualifiers.size()) {
        // We have already constructed all arrays and maps. What remains must be a Row.
        int tempVariable = localVariables.createLocalVariable();
        StackManipulation updateLocalVariable =
            new StackManipulation.Compound(
                loadFieldValue(fieldId),
                // Save the current value of the CURRENT_SELECT_ROW_ARG variable into the temp
                // variable.
                localVariables.copy(currentSelectRowArg, tempVariable),
                // Update the row local with the current stack value.
                localVariables.writeVariable(currentSelectRowArg));
        size = size.aggregate(updateLocalVariable.apply(methodVisitor, implementationContext));

        SizeAndArrayPosition sizeAndArrayPosition =
            selectIntoRow(
                inputType.getRowSchema(),
                fieldAccessDescriptor,
                currentArrayField,
                methodVisitor,
                implementationContext);
        size = size.aggregate(sizeAndArrayPosition.getSize());
        currentArrayField = sizeAndArrayPosition.getArrayPosition();
        // Restore the value of currentSelectRowArg from the temp variable.
        size =
            size.aggregate(
                localVariables
                    .copy(tempVariable, currentSelectRowArg)
                    .apply(methodVisitor, implementationContext));
        return SizeAndArrayPosition.of(size, currentArrayField);
      }

      Qualifier qualifier = qualifiers.get(qualifierPosition);
      switch (qualifier.getKind()) {
        case LIST:
          {
            FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
            FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());

            // When selecting multiple subelements under a list, we distribute the select
            // resulting in multiple lists. For example, if there is a field "list" with type
            // {a: string, b: int}[], selecting list.a, list.b results in a schema of type
            // {a: string[], b: int[]}. This preserves the invariant that the name selected always
            // appears in the top-level schema.
            Schema tempSchema = Schema.builder().addField("a", nestedInputType).build();
            FieldAccessDescriptor tempAccessDescriptor =
                FieldAccessDescriptor.create()
                    .withNestedField("a", fieldAccessDescriptor)
                    .resolve(tempSchema);
            Schema nestedSchema = SelectHelpers.getOutputSchema(tempSchema, tempAccessDescriptor);

            // We create temp local variables to store all the arrays we create. Each field in
            // nestedSchema
            // corresponds to a separate array in the output.
            int[] localVariablesForArrays = new int[nestedSchema.getFieldCount()];
            // Each field returned in nestedSchema will become it's own list in the output. So let's
            // iterate and create arrays and store each one in the output.
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              // Remember which local variable stores this array.
              localVariablesForArrays[i] = localVariables.createLocalVariable();

              // Creates an array list, stores in local variable, and leaves array list on stack.
              StackManipulation createArrayList =
                  new StackManipulation.Compound(
                      MethodInvocation.invoke(
                          new ForLoadedType(Lists.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.named("newArrayList")
                                      .and(ElementMatchers.takesArguments(0)))
                              .getOnly()),
                      // Store the ArrayList in a local variable.
                      Duplication.SINGLE,
                      localVariables.writeVariable(localVariablesForArrays[i]));
              // Also store the ArrayList into the the output array that this function is
              // generating.
              size =
                  size.aggregate(
                      storeIntoArray(
                          currentArrayField,
                          createArrayList,
                          methodVisitor,
                          implementationContext));
              currentArrayField++;
            }

            // Now iterate over the value, selecting from each element.
            StackManipulation readListIterator =
                new StackManipulation.Compound(
                    loadFieldValue(fieldId),
                    TypeCasting.to(new ForLoadedType(Iterable.class)),
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterable.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("iterator"))
                            .getOnly()));
            size = size.aggregate(readListIterator.apply(methodVisitor, implementationContext));

            // Loop over the entire iterable.
            Label startLoopLabel = new Label();
            Label exitLoopLabel = new Label();
            methodVisitor.visitLabel(startLoopLabel);

            StackManipulation checkTerminationCondition =
                new StackManipulation.Compound(
                    Duplication.SINGLE,
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterator.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("hasNext"))
                            .getOnly()));
            size =
                size.aggregate(
                    checkTerminationCondition.apply(methodVisitor, implementationContext));
            methodVisitor.visitJumpInsn(
                Opcodes.IFEQ, exitLoopLabel); // Exit the loop if !hasNext().
            size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

            // Read the next value in the iterator.
            StackManipulation getNext =
                new StackManipulation.Compound(
                    Duplication.SINGLE,
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterator.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("next"))
                            .getOnly()));
            size = size.aggregate(getNext.apply(methodVisitor, implementationContext));

            int fieldValueTempVariable = 0;
            if (fieldId == -1) {
              // Save the field value arg before overwriting it, as we need it in subsequent
              // iterations of the
              // loop.
              fieldValueTempVariable = localVariables.createLocalVariable();
              StackManipulation backupFieldValue =
                  localVariables.copy(fieldValueArg, fieldValueTempVariable);
              size = size.aggregate(backupFieldValue.apply(methodVisitor, implementationContext));
            }

            // Recursively generate select with one qualifier consumed. Since we pass in -1 as the
            // field id, the iterator.next() value will be consumed as the value to select instead
            // of accessing the fieldId.
            size =
                size.aggregate(
                    localVariables
                        .writeVariable(fieldValueArg)
                        .apply(methodVisitor, implementationContext));

            // Select the fields of interest from this row.
            // Create a new object array with one entry for each field selected from this row.
            size =
                size.aggregate(
                    IntegerConstant.forValue(nestedSchema.getFieldCount())
                        .apply(methodVisitor, implementationContext));
            methodVisitor.visitTypeInsn(
                Opcodes.ANEWARRAY, Generic.OBJECT.asErasure().getInternalName());
            size = size.aggregate(StackSize.ZERO.toDecreasingSize());

            size =
                size.aggregate(
                    selectIntoRowWithQualifiers(
                            qualifiers,
                            qualifierPosition + 1,
                            -1,
                            fieldAccessDescriptor,
                            nestedInputType,
                            nestedOutputType,
                            0, // Start at zero because we're selecting into a fresh array.
                            methodVisitor,
                            implementationContext)
                        .getSize());

            if (fieldId == -1) {
              // Restore the field value.
              StackManipulation restoreFieldValue =
                  localVariables.copy(fieldValueTempVariable, fieldValueArg);
              size = size.aggregate(restoreFieldValue.apply(methodVisitor, implementationContext));
            }

            // Now the top of the stack holds an array containing all the fields selected from the
            // row.
            // Now we need to distribute these fields into the separate arrays we created in the
            // result.
            // That is: if this select returned {a, b}, our final resulting schema will contain two
            // lists,
            // so we must add a to the first and b to the second list.

            int tempVariableForField = localVariables.createLocalVariable();
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              // Extract the field and store it in a temp variable.
              StackManipulation extractField =
                  new StackManipulation.Compound(
                      Duplication.SINGLE,
                      IntegerConstant.forValue(i),
                      ArrayAccess.REFERENCE.load(),
                      localVariables.writeVariable(tempVariableForField));
              size = size.aggregate(extractField.apply(methodVisitor, implementationContext));

              StackManipulation addItemToList =
                  new StackManipulation.Compound(
                      localVariables.readVariable(localVariablesForArrays[i]),
                      localVariables.readVariable(tempVariableForField),
                      MethodInvocation.invoke(
                          new ForLoadedType(List.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.named("add")
                                      .and(ElementMatchers.takesArguments(1)))
                              .getOnly()),
                      // Ignore return value from add().
                      Removal.SINGLE);
              size = size.aggregate(addItemToList.apply(methodVisitor, implementationContext));
            }

            // Pop the created array from the top of the stack.
            size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

            // Go back to the beginning of the loop.
            methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
            methodVisitor.visitLabel(exitLoopLabel);
            // Remove the iterator from the top of the stack.
            size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));
            return SizeAndArrayPosition.of(size, currentArrayField);
          }
        case MAP:
          {
            FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
            FieldType nestedOutputType = checkNotNull(outputType.getMapValueType());

            // When selecting multiple subelements under a map, we distribute the select
            // resulting in multiple maps. The semantics are the same as for lists above (except we
            // only support subelement select for map values, not for map keys).
            Schema tempSchema = Schema.builder().addField("a", nestedInputType).build();
            FieldAccessDescriptor tempAccessDescriptor =
                FieldAccessDescriptor.create()
                    .withNestedField("a", fieldAccessDescriptor)
                    .resolve(tempSchema);
            Schema nestedSchema = SelectHelpers.getOutputSchema(tempSchema, tempAccessDescriptor);

            // We create temp local variables to store all the maps we create. Each field in
            // nestedSchema
            // corresponds to a separate array in the output.
            int[] localVariablesForMaps = new int[nestedSchema.getFieldCount()];
            // Each field returned in nestedSchema will become it's own map in the output. So let's
            // iterate and create arrays and store each one in the output.
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              // Remember which local variable stores this array.
              localVariablesForMaps[i] = localVariables.createLocalVariable();

              // Creates a map, stores in local variable, and leaves array list on stack.
              StackManipulation createHashMap =
                  new StackManipulation.Compound(
                      MethodInvocation.invoke(
                          new ForLoadedType(Maps.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.named("newHashMap")
                                      .and(ElementMatchers.takesArguments(0)))
                              .getOnly()),
                      // Store the Map in a local variable.
                      Duplication.SINGLE,
                      localVariables.writeVariable(localVariablesForMaps[i]));
              // Also store the Map into the the output array that this function is generating.
              size =
                  size.aggregate(
                      storeIntoArray(
                          currentArrayField++,
                          createHashMap,
                          methodVisitor,
                          implementationContext));
            }

            // Now iterate over the value, selecting from each element.
            StackManipulation readMapEntriesIterator =
                new StackManipulation.Compound(
                    loadFieldValue(fieldId),
                    TypeCasting.to(new ForLoadedType(Map.class)),
                    MethodInvocation.invoke(
                        new ForLoadedType(Map.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("entrySet"))
                            .getOnly()),
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterable.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("iterator"))
                            .getOnly()));
            size =
                size.aggregate(readMapEntriesIterator.apply(methodVisitor, implementationContext));

            // Loop over the entire entrySet iterable.
            Label startLoopLabel = new Label();
            Label exitLoopLabel = new Label();
            methodVisitor.visitLabel(startLoopLabel);

            StackManipulation checkTerminationCondition =
                new StackManipulation.Compound(
                    Duplication.SINGLE,
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterator.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("hasNext"))
                            .getOnly()));
            size =
                size.aggregate(
                    checkTerminationCondition.apply(methodVisitor, implementationContext));
            methodVisitor.visitJumpInsn(
                Opcodes.IFEQ, exitLoopLabel); // Exit the loop if !hasNext().
            size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

            int keyVariable = localVariables.createLocalVariable();
            // Read the next value in the iterator.
            StackManipulation getNext =
                new StackManipulation.Compound(
                    Duplication.SINGLE,
                    MethodInvocation.invoke(
                        new ForLoadedType(Iterator.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("next"))
                            .getOnly()),
                    // Get the key and store it in the keyVariable.
                    Duplication.SINGLE,
                    MethodInvocation.invoke(
                        new ForLoadedType(Map.Entry.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("getKey"))
                            .getOnly()),
                    localVariables.writeVariable(keyVariable),
                    // Get the value and leave it on the stack.
                    MethodInvocation.invoke(
                        new ForLoadedType(Map.Entry.class)
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("getValue"))
                            .getOnly()));
            size = size.aggregate(getNext.apply(methodVisitor, implementationContext));

            int fieldValueTempVariable = 0;
            if (fieldId == -1) {
              // Save the field value arg before overwriting it, as we need it in subsequent
              // iterations of the
              // loop.
              fieldValueTempVariable = localVariables.createLocalVariable();
              StackManipulation backupFieldValue =
                  localVariables.copy(fieldValueArg, fieldValueTempVariable);
              size = size.aggregate(backupFieldValue.apply(methodVisitor, implementationContext));
            }

            // Recursively generate select with one qualifier consumed. Since we pass in -1 as the
            // field id, the iterator.next() value will be consumed as the value to select instead
            // of accessing the fieldId.
            size =
                size.aggregate(
                    localVariables
                        .writeVariable(fieldValueArg)
                        .apply(methodVisitor, implementationContext));

            // Select the fields of interest from this row.
            // Create a new object array with one entry for each field selected from this row.
            size =
                size.aggregate(
                    IntegerConstant.forValue(nestedSchema.getFieldCount())
                        .apply(methodVisitor, implementationContext));
            methodVisitor.visitTypeInsn(
                Opcodes.ANEWARRAY, Generic.OBJECT.asErasure().getInternalName());
            size = size.aggregate(StackSize.ZERO.toDecreasingSize());

            size =
                size.aggregate(
                    selectIntoRowWithQualifiers(
                            qualifiers,
                            qualifierPosition + 1,
                            -1,
                            fieldAccessDescriptor,
                            nestedInputType,
                            nestedOutputType,
                            0, // Start at zero because we're selecting into a fresh array.
                            methodVisitor,
                            implementationContext)
                        .getSize());

            if (fieldId == -1) {
              // Restore the field value.
              StackManipulation restoreFieldValue =
                  localVariables.copy(fieldValueTempVariable, fieldValueArg);
              size = size.aggregate(restoreFieldValue.apply(methodVisitor, implementationContext));
            }

            // Now the top of the stack holds an array containing all the fields selected from the
            // row.
            // Now we need to distribute these fields into the separate arrays we created in the
            // result.
            // That is: if this select returned {a, b}, our final resulting schema will contain two
            // maps,
            // so we must add a to the first and b to the second map.

            int tempVariableForField = localVariables.createLocalVariable();
            for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
              // Extract the field and store it in a temp variable.
              StackManipulation extractField =
                  new StackManipulation.Compound(
                      Duplication.SINGLE,
                      IntegerConstant.forValue(i),
                      ArrayAccess.REFERENCE.load(),
                      localVariables.writeVariable(tempVariableForField));
              size = size.aggregate(extractField.apply(methodVisitor, implementationContext));

              StackManipulation addItemToMap =
                  new StackManipulation.Compound(
                      localVariables.readVariable(localVariablesForMaps[i]),
                      localVariables.readVariable(keyVariable),
                      localVariables.readVariable(tempVariableForField),
                      MethodInvocation.invoke(
                          new ForLoadedType(Map.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.named("put")
                                      .and(ElementMatchers.takesArguments(2)))
                              .getOnly()),
                      // Ignore return value from add().
                      Removal.SINGLE);
              size = size.aggregate(addItemToMap.apply(methodVisitor, implementationContext));
            }

            // Pop the created array from the top of the stack.
            size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

            // Go back to the beginning of the loop.
            methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
            methodVisitor.visitLabel(exitLoopLabel);
            // Remove the iterator from the top of the stack.
            size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));
            return SizeAndArrayPosition.of(size, currentArrayField);
          }
        default:
          throw new RuntimeException("Unexpected type " + qualifier.getKind());
      }
    }
  }
}
