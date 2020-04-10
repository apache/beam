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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.IfNullElse;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ShortCircuitReturnNull;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.asm.AsmVisitorWrapper;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.method.MethodDescription;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.FieldManifestation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.Visibility;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.Generic;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.Implementation.Context;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Duplication;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.Removal;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackSize;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.constant.NullConstant;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.FieldAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.ClassWriter;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.Label;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.MethodVisitor;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.jar.asm.Opcodes;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

class SelectByteBuddyHelpers {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final String SELECT_SCHEMA_FIELD_NAME = "OUTPUTSCHEMA";

  private static final ForLoadedType ROW_LOADED_TYPE = new ForLoadedType(Row.class);
  private static final ForLoadedType LIST_LOADED_TYPE = new ForLoadedType(List.class);
  private static final ForLoadedType LISTS_LOADED_TYPE = new ForLoadedType(Lists.class);
  private static final ForLoadedType MAP_LOADED_TYPE = new ForLoadedType(Map.class);
  private static final ForLoadedType MAPS_LOADED_TYPE = new ForLoadedType(Maps.class);
  private static final ForLoadedType MAPENTRY_LOADED_TYPE = new ForLoadedType(Map.Entry.class);
  private static final ForLoadedType ITERABLE_LOADED_TYPE = new ForLoadedType(Iterable.class);
  private static final ForLoadedType ITERATOR_LOADED_TYPE = new ForLoadedType(Iterator.class);

  private static final MethodDescription LIST_ADD =
      LIST_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("add").and(ElementMatchers.takesArguments(1)))
          .getOnly();

  private static final MethodDescription LISTS_NEW_ARRAYLIST =
      LISTS_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("newArrayList").and(ElementMatchers.takesArguments(0)))
          .getOnly();

  private static final MethodDescription MAP_ENTRYSET =
      MAP_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("entrySet")).getOnly();

  private static final MethodDescription MAP_PUT =
      MAP_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("put").and(ElementMatchers.takesArguments(2)))
          .getOnly();

  private static final MethodDescription MAPS_NEW_HASHMAP =
      MAPS_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("newHashMap").and(ElementMatchers.takesArguments(0)))
          .getOnly();

  private static final MethodDescription ITERABLE_ITERATOR =
      ITERABLE_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("iterator")).getOnly();

  private static final MethodDescription ITERATOR_HASNEXT =
      ITERATOR_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("hasNext")).getOnly();

  private static final MethodDescription ITERATOR_NEXT =
      ITERATOR_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("next")).getOnly();

  private static final MethodDescription MAPENTRY_GETKEY =
      MAPENTRY_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("getKey")).getOnly();

  private static final MethodDescription MAPENTRY_GETVALUE =
      MAPENTRY_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("getValue")).getOnly();

  @AutoValue
  abstract static class SchemaAndDescriptor {
    abstract Schema getSchema();

    abstract FieldAccessDescriptor getFieldAccessDecriptor();

    static SchemaAndDescriptor of(Schema schema, FieldAccessDescriptor fieldAccessDescriptor) {
      return new AutoValue_SelectByteBuddyHelpers_SchemaAndDescriptor(
          schema, fieldAccessDescriptor);
    }
  }

  private static final Map<SchemaAndDescriptor, RowSelector> CACHED_SELECTORS =
      Maps.newConcurrentMap();

  static RowSelector getRowSelector(
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

  // Manage array creation and appending.
  private static class ArrayManager {
    private final StackManipulation.Size sizeDecreaseArrayStore;
    private final int arraySize;
    int currentArrayField = 0;

    ArrayManager(int arraySize) {
      // Size decreases by index and array reference (2) and array element (1, 2) after each element
      // storage.
      this.sizeDecreaseArrayStore =
          StackSize.DOUBLE
              .toDecreasingSize()
              .aggregate(Generic.OBJECT.getStackSize().toDecreasingSize());
      this.arraySize = arraySize;
    }

    StackManipulation createArray() {
      return new StackManipulation() {
        @Override
        public boolean isValid() {
          return true;
        }

        @Override
        public Size apply(MethodVisitor methodVisitor, Context context) {
          Size size = IntegerConstant.forValue(arraySize).apply(methodVisitor, context);
          methodVisitor.visitTypeInsn(
              Opcodes.ANEWARRAY, Generic.OBJECT.asErasure().getInternalName());
          size = size.aggregate(StackSize.ZERO.toDecreasingSize());
          return size;
        }
      };
    }

    StackManipulation append(StackManipulation valueToWrite) {
      return store(currentArrayField++, valueToWrite);
    }

    int reserveSlot() {
      return currentArrayField++;
    }

    StackManipulation store(int arrayIndexToWrite, StackManipulation valueToWrite) {
      Preconditions.checkArgument(arrayIndexToWrite < arraySize);
      return new StackManipulation() {
        @Override
        public boolean isValid() {
          return true;
        }

        @Override
        public Size apply(MethodVisitor methodVisitor, Context context) {
          StackManipulation stackManipulation =
              new StackManipulation.Compound(
                  Duplication.SINGLE, // Duplicate the array reference
                  IntegerConstant.forValue(arrayIndexToWrite),
                  valueToWrite);
          StackManipulation.Size size = stackManipulation.apply(methodVisitor, context);
          methodVisitor.visitInsn(Opcodes.AASTORE);
          size = size.aggregate(sizeDecreaseArrayStore);
          return size;
        }
      };
    }
  }

  private static class SelectInstruction implements Implementation {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private ByteBuddyLocalVariableManager localVariables;
    private static final int INPUT_ROW_ARG = 1;
    private final int currentSelectRowArg;
    private final int fieldValueArg;

    private static final int NUM_FUNCTION_ARGS = 2; // Args are this and the row to select.

    public SelectInstruction(
        FieldAccessDescriptor fieldAccessDescriptor, Schema inputSchema, Schema outputSchema) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      this.localVariables = new ByteBuddyLocalVariableManager(NUM_FUNCTION_ARGS);
      currentSelectRowArg = localVariables.createVariable();
      fieldValueArg = localVariables.createVariable();
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
        ArrayManager arrayManager = new ArrayManager(outputSchema.getFieldCount());
        size =
            size.aggregate(arrayManager.createArray().apply(methodVisitor, implementationContext));

        // Store the current input row into a local variable.
        StackManipulation storeRowInLocalVariable =
            localVariables.copy(INPUT_ROW_ARG, currentSelectRowArg);
        size = size.aggregate(storeRowInLocalVariable.apply(methodVisitor, implementationContext));

        // Fill the array values with those selected from the row.
        size =
            size.aggregate(
                selectIntoArray(
                    inputSchema,
                    fieldAccessDescriptor,
                    arrayManager,
                    methodVisitor,
                    implementationContext));

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
                MethodReturn.REFERENCE);
        size = size.aggregate(attachToRow.apply(methodVisitor, implementationContext));
        return new Size(size.getMaximalSize(), localVariables.getTotalNumVariables());
      };
    }

    // Selects a field from the current row being selected (the one stored in
    // currentSelectRowArg).
    private StackManipulation getCurrentRowFieldValue(int i) {
      StackManipulation readRow = localVariables.readVariable(currentSelectRowArg, Row.class);
      StackManipulation getValue =
          new StackManipulation.Compound(
              localVariables.readVariable(currentSelectRowArg, Row.class),
              IntegerConstant.forValue(i),
              MethodInvocation.invoke(
                  ROW_LOADED_TYPE
                      .getDeclaredMethods()
                      .filter(
                          ElementMatchers.named("getValue")
                              .and(ElementMatchers.takesArguments(int.class)))
                      .getOnly()));

      return new ShortCircuitReturnNull(readRow, getValue);
    }

    // Generate bytecode to select all specified fields from the Row. The current row being selected
    // is stored
    // in the local variable at position 2. The current array being written to is at the top of the
    // stack.
    StackManipulation.Size selectIntoArray(
        Schema inputSchema,
        FieldAccessDescriptor fieldAccessDescriptor,
        ArrayManager arrayManager,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      if (fieldAccessDescriptor.getAllFields()) {
        StackManipulation storeAllValues =
            new StackManipulation.Compound(
                IntStream.range(0, inputSchema.getFieldCount())
                    .mapToObj(i -> arrayManager.append(getCurrentRowFieldValue(i)))
                    .collect(Collectors.toList()));
        return size.aggregate(storeAllValues.apply(methodVisitor, implementationContext));
      }

      StackManipulation storeAllValues =
          new StackManipulation.Compound(
              fieldAccessDescriptor.fieldIdsAccessed().stream()
                  .map(i -> arrayManager.append(getCurrentRowFieldValue(i)))
                  .collect(Collectors.toList()));
      size = size.aggregate(storeAllValues.apply(methodVisitor, implementationContext));

      for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
          fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
        FieldDescriptor field = nested.getKey();
        FieldAccessDescriptor nestedAccess = nested.getValue();
        FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();

        StackManipulation.Size subSelectSize =
            selectIntoArrayHelper(
                field.getQualifiers(),
                0,
                field.getFieldId(),
                nestedAccess,
                nestedInputType,
                arrayManager,
                methodVisitor,
                implementationContext);
        size = size.aggregate(subSelectSize);
      }
      return size;
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

    private StackManipulation.Size selectIntoArrayHelper(
        List<Qualifier> qualifiers,
        int qualifierPosition,
        int fieldId,
        FieldAccessDescriptor fieldAccessDescriptor,
        FieldType inputType,
        ArrayManager arrayManager,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);

      if (qualifierPosition >= qualifiers.size()) {
        // We have already constructed all arrays and maps. What remains must be a Row.
        ByteBuddyLocalVariableManager.BackupLocalVariable backup =
            localVariables.backupVariable(currentSelectRowArg);
        StackManipulation updateLocalVariable =
            new StackManipulation.Compound(
                loadFieldValue(fieldId),
                // Backup the current value of the currentSelectRowArg variable.
                backup.backup(),
                // Update the row local with the current stack value.
                localVariables.writeVariable(currentSelectRowArg));
        size = size.aggregate(updateLocalVariable.apply(methodVisitor, implementationContext));

        size =
            size.aggregate(
                selectIntoArray(
                    inputType.getRowSchema(),
                    fieldAccessDescriptor,
                    arrayManager,
                    methodVisitor,
                    implementationContext));
        // Restore the value of currentSelectRowArg from the temp variable.
        size = size.aggregate(backup.restore().apply(methodVisitor, implementationContext));
        return size;
      }

      Qualifier qualifier = qualifiers.get(qualifierPosition);
      switch (qualifier.getKind()) {
        case LIST:
          return size.aggregate(
              processList(
                  inputType,
                  fieldAccessDescriptor,
                  qualifiers,
                  qualifierPosition,
                  fieldId,
                  arrayManager,
                  methodVisitor,
                  implementationContext));

        case MAP:
          return size.aggregate(
              processMap(
                  inputType,
                  fieldAccessDescriptor,
                  qualifiers,
                  qualifierPosition,
                  fieldId,
                  arrayManager,
                  methodVisitor,
                  implementationContext));

        default:
          throw new RuntimeException("Unexpected type " + qualifier.getKind());
      }
    }

    private StackManipulation.Size processList(
        FieldType inputType,
        FieldAccessDescriptor fieldAccessDescriptor,
        List<Qualifier> qualifiers,
        int qualifierPosition,
        int fieldId,
        ArrayManager arrayManager,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
      Schema nestedSchema = getNestedSchema(nestedInputType, fieldAccessDescriptor);

      // We create temp local variables to store all the arrays we create. Each field in
      // nestedSchema corresponds to a separate array in the output.
      int[] localVariablesForArrays =
          IntStream.range(0, nestedSchema.getFieldCount())
              .map(i -> localVariables.createVariable())
              .toArray();
      // Each field returned in nestedSchema will become it's own list in the output. So let's
      // iterate and create arrays and store each one in the output.
      StackManipulation createAllArrayLists =
          new StackManipulation.Compound(
              Arrays.stream(localVariablesForArrays)
                  .mapToObj(
                      v -> {
                        StackManipulation createArrayList =
                            new StackManipulation.Compound(
                                MethodInvocation.invoke(LISTS_NEW_ARRAYLIST),
                                // Store the ArrayList in a local variable.
                                Duplication.SINGLE,
                                localVariables.writeVariable(v));
                        StackManipulation storeNull =
                            new StackManipulation.Compound(
                                NullConstant.INSTANCE,
                                localVariables.writeVariable(v),
                                NullConstant.INSTANCE);

                        // Create the array only if the input isn't null. Otherwise store a null
                        // value into the output
                        // array.
                        int arraySlot = arrayManager.reserveSlot();
                        return new IfNullElse(
                            loadFieldValue(fieldId),
                            arrayManager.store(arraySlot, storeNull),
                            arrayManager.store(arraySlot, createArrayList));
                      })
                  .collect(Collectors.toList()));
      size = size.aggregate(createAllArrayLists.apply(methodVisitor, implementationContext));

      // If the input variable is null, then don't try and iterate over it.
      Label onNullLabel = new Label();
      size = size.aggregate(loadFieldValue(fieldId).apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFNULL, onNullLabel);
      size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

      // Now iterate over the value, selecting from each element.
      StackManipulation readListIterator =
          new StackManipulation.Compound(
              loadFieldValue(fieldId),
              TypeCasting.to(new ForLoadedType(Iterable.class)),
              MethodInvocation.invoke(ITERABLE_ITERATOR));
      size = size.aggregate(readListIterator.apply(methodVisitor, implementationContext));

      Label startLoopLabel = new Label();
      Label exitLoopLabel = new Label();
      // Loop over the entire iterable.
      methodVisitor.visitLabel(startLoopLabel);

      StackManipulation checkTerminationCondition =
          new StackManipulation.Compound(
              Duplication.SINGLE, MethodInvocation.invoke(ITERATOR_HASNEXT));
      size = size.aggregate(checkTerminationCondition.apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFEQ, exitLoopLabel); // Exit the loop if !hasNext().
      size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

      // Read the next value in the iterator.
      StackManipulation getNext =
          new StackManipulation.Compound(
              Duplication.SINGLE, MethodInvocation.invoke(ITERATOR_NEXT));
      size = size.aggregate(getNext.apply(methodVisitor, implementationContext));

      ByteBuddyLocalVariableManager.BackupLocalVariable backupFieldValue = null;
      if (fieldId == -1) {
        // Save the field value arg before overwriting it, as we need it in subsequent
        // iterations of the
        // loop.
        backupFieldValue = localVariables.backupVariable(fieldValueArg);
        size =
            size.aggregate(backupFieldValue.backup().apply(methodVisitor, implementationContext));
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
      ArrayManager nestedArrayManager = new ArrayManager(nestedSchema.getFieldCount());
      size =
          size.aggregate(
              nestedArrayManager.createArray().apply(methodVisitor, implementationContext));

      size =
          size.aggregate(
              selectIntoArrayHelper(
                  qualifiers,
                  qualifierPosition + 1,
                  -1,
                  fieldAccessDescriptor,
                  nestedInputType,
                  nestedArrayManager,
                  methodVisitor,
                  implementationContext));

      if (backupFieldValue != null) {
        // Restore the field value.
        size =
            size.aggregate(backupFieldValue.restore().apply(methodVisitor, implementationContext));
      }

      // Now the top of the stack holds an array containing all the fields selected from the
      // row.
      // Now we need to distribute these fields into the separate arrays we created in the
      // result.
      // That is: if this select returned {a, b}, our final resulting schema will contain two
      // lists,
      // so we must add a to the first and b to the second list.

      int tempVariableForField = localVariables.createVariable();
      for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
        // Extract the field and store it in a temp variable.
        StackManipulation extractField =
            new StackManipulation.Compound(
                Duplication.SINGLE,
                IntegerConstant.forValue(i),
                ArrayAccess.REFERENCE.load(),
                localVariables.writeVariable(tempVariableForField));
        StackManipulation addItemToList =
            new StackManipulation.Compound(
                localVariables.readVariable(localVariablesForArrays[i]),
                localVariables.readVariable(tempVariableForField),
                MethodInvocation.invoke(LIST_ADD),
                // Ignore return value from add().
                Removal.SINGLE);
        size =
            size.aggregate(
                new StackManipulation.Compound(extractField, addItemToList)
                    .apply(methodVisitor, implementationContext));
      }

      // Pop the created array from the top of the stack.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      // Go back to the beginning of the loop.
      methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
      methodVisitor.visitLabel(exitLoopLabel);
      // Remove the iterator from the top of the stack.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));
      methodVisitor.visitLabel(onNullLabel);
      return size;
    }

    private StackManipulation.Size processMap(
        FieldType inputType,
        FieldAccessDescriptor fieldAccessDescriptor,
        List<Qualifier> qualifiers,
        int qualifierPosition,
        int fieldId,
        ArrayManager arrayManager,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
      Schema nestedSchema = getNestedSchema(nestedInputType, fieldAccessDescriptor);

      // We create temp local variables to store all the maps we create. Each field in
      // nestedSchema corresponds to a separate array in the output.
      int[] localVariablesForMaps =
          IntStream.range(0, nestedSchema.getFieldCount())
              .map(i -> localVariables.createVariable())
              .toArray();

      // Each field returned in nestedSchema will become it's own map in the output. So let's
      // iterate and create arrays and store each one in the output.
      StackManipulation createAllHashMaps =
          new StackManipulation.Compound(
              Arrays.stream(localVariablesForMaps)
                  .mapToObj(
                      v -> {
                        StackManipulation createHashMap =
                            new StackManipulation.Compound(
                                MethodInvocation.invoke(MAPS_NEW_HASHMAP),
                                // Store the HashMap in a local variable.
                                Duplication.SINGLE,
                                localVariables.writeVariable(v));
                        StackManipulation storeNull =
                            new StackManipulation.Compound(
                                NullConstant.INSTANCE,
                                localVariables.writeVariable(v),
                                NullConstant.INSTANCE);
                        int arraySlot = arrayManager.reserveSlot();
                        return new IfNullElse(
                            loadFieldValue(fieldId),
                            arrayManager.store(arraySlot, storeNull),
                            arrayManager.store(arraySlot, createHashMap));
                      })
                  .collect(Collectors.toList()));
      size = size.aggregate(createAllHashMaps.apply(methodVisitor, implementationContext));

      // If the input variable is null, then don't try and iterate over it.
      Label onNullLabel = new Label();
      size = size.aggregate(loadFieldValue(fieldId).apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFNULL, onNullLabel);
      size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

      // Now iterate over the value, selecting from each element.
      StackManipulation readMapEntriesIterator =
          new StackManipulation.Compound(
              loadFieldValue(fieldId),
              TypeCasting.to(new ForLoadedType(Map.class)),
              MethodInvocation.invoke(MAP_ENTRYSET),
              MethodInvocation.invoke(ITERABLE_ITERATOR));
      size = size.aggregate(readMapEntriesIterator.apply(methodVisitor, implementationContext));

      // Loop over the entire entrySet iterable.
      Label startLoopLabel = new Label();
      Label exitLoopLabel = new Label();
      methodVisitor.visitLabel(startLoopLabel);

      StackManipulation checkTerminationCondition =
          new StackManipulation.Compound(
              Duplication.SINGLE, MethodInvocation.invoke(ITERATOR_HASNEXT));
      size = size.aggregate(checkTerminationCondition.apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFEQ, exitLoopLabel); // Exit the loop if !hasNext().
      size = size.aggregate(StackSize.SINGLE.toDecreasingSize());

      int keyVariable = localVariables.createVariable();
      // Read the next value in the iterator.
      StackManipulation getNext =
          new StackManipulation.Compound(
              Duplication.SINGLE,
              MethodInvocation.invoke(ITERATOR_NEXT),
              // Get the key and store it in the keyVariable.
              Duplication.SINGLE,
              MethodInvocation.invoke(MAPENTRY_GETKEY),
              localVariables.writeVariable(keyVariable),
              // Get the value and leave it on the stack.
              MethodInvocation.invoke(MAPENTRY_GETVALUE));
      size = size.aggregate(getNext.apply(methodVisitor, implementationContext));

      ByteBuddyLocalVariableManager.BackupLocalVariable backupFieldValue = null;
      if (fieldId == -1) {
        // Save the field value arg before overwriting it, as we need it in subsequent
        // iterations of the
        // loop.
        backupFieldValue = localVariables.backupVariable(fieldValueArg);
        size =
            size.aggregate(backupFieldValue.backup().apply(methodVisitor, implementationContext));
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
      ArrayManager nestedArrayManager = new ArrayManager(nestedSchema.getFieldCount());
      size =
          size.aggregate(
              nestedArrayManager.createArray().apply(methodVisitor, implementationContext));

      size =
          size.aggregate(
              selectIntoArrayHelper(
                  qualifiers,
                  qualifierPosition + 1,
                  -1,
                  fieldAccessDescriptor,
                  nestedInputType,
                  nestedArrayManager,
                  methodVisitor,
                  implementationContext));

      if (backupFieldValue != null) {
        // Restore the field value.
        size =
            size.aggregate(backupFieldValue.restore().apply(methodVisitor, implementationContext));
      }

      // Now the top of the stack holds an array containing all the fields selected from the
      // row.
      // Now we need to distribute these fields into the separate arrays we created in the
      // result.
      // That is: if this select returned {a, b}, our final resulting schema will contain two
      // maps,
      // so we must add a to the first and b to the second map.

      int tempVariableForField = localVariables.createVariable();
      for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
        // Extract the field and store it in a temp variable.
        StackManipulation extractField =
            new StackManipulation.Compound(
                Duplication.SINGLE,
                IntegerConstant.forValue(i),
                ArrayAccess.REFERENCE.load(),
                localVariables.writeVariable(tempVariableForField));
        StackManipulation addItemToMap =
            new StackManipulation.Compound(
                localVariables.readVariable(localVariablesForMaps[i]),
                localVariables.readVariable(keyVariable),
                localVariables.readVariable(tempVariableForField),
                MethodInvocation.invoke(MAP_PUT),
                // Ignore return value from add().
                Removal.SINGLE);
        size =
            size.aggregate(
                new StackManipulation.Compound(extractField, addItemToMap)
                    .apply(methodVisitor, implementationContext));
      }

      // Pop the created array from the top of the stack.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      // Go back to the beginning of the loop.
      methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
      methodVisitor.visitLabel(exitLoopLabel);
      // Remove the iterator from the top of the stack.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));
      methodVisitor.visitLabel(onNullLabel);
      return size;
    }

    private Schema getNestedSchema(
        FieldType nestedInputType, FieldAccessDescriptor fieldAccessDescriptor) {
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
      return SelectHelpers.getOutputSchema(tempSchema, tempAccessDescriptor);
    }
  }
}
