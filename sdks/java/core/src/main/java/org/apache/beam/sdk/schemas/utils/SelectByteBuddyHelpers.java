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

import static org.apache.beam.sdk.util.ByteBuddyUtils.getClassLoadingStrategy;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Context;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.Removal;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.StackSize;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.collection.ArrayAccess;
import net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "ReferenceEquality"
})
class SelectByteBuddyHelpers {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
  private static final String SELECT_SCHEMA_FIELD_NAME = "OUTPUTSCHEMA";

  /**
   * Maximum estimated bytecode size (bytes) of a generated {@code select()} method before falling
   * back to loop-based selection to stay well below HotSpot C2's default 8KB {@code
   * HugeMethodLimit} and JVM 64KB bytecode limit.
   */
  private static final int MAX_ESTIMATED_BYTECODE_BYTES = 6000;

  private static final int MAX_GENERATED_SELECTOR_CLASSES = 512;

  private static final ForLoadedType ROW_LOADED_TYPE = new ForLoadedType(Row.class);
  private static final ForLoadedType ROW_WITH_STORAGE_TYPE =
      new ForLoadedType(RowWithStorage.class);
  private static final ForLoadedType ARRAY_LIST_TYPE = new ForLoadedType(ArrayList.class);
  private static final ForLoadedType HASH_MAP_TYPE = new ForLoadedType(HashMap.class);
  private static final ForLoadedType MAP_LOADED_TYPE = new ForLoadedType(Map.class);
  private static final ForLoadedType MAPENTRY_LOADED_TYPE = new ForLoadedType(Map.Entry.class);
  private static final ForLoadedType ITERABLE_LOADED_TYPE = new ForLoadedType(Iterable.class);
  private static final ForLoadedType ITERATOR_LOADED_TYPE = new ForLoadedType(Iterator.class);
  private static final ForLoadedType SELF_LOADED_TYPE =
      new ForLoadedType(SelectByteBuddyHelpers.class);

  private static final MethodDescription ROW_GET_VALUE =
      ROW_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("getValue").and(ElementMatchers.takesArguments(int.class)))
          .getOnly();

  private static final MethodDescription ROW_ATTACH_VALUES =
      ROW_LOADED_TYPE
          .getDeclaredMethods()
          .filter(
              ElementMatchers.named("attachValues")
                  .and(ElementMatchers.takesArguments(Schema.class, Object[].class)))
          .getOnly();

  private static final MethodDescription ROW_WITH_STORAGE_RAW_ARRAY =
      ROW_WITH_STORAGE_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("getRawArray").and(ElementMatchers.takesArguments(0)))
          .getOnly();

  private static final MethodDescription ARRAY_LIST_ADD =
      ARRAY_LIST_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("add").and(ElementMatchers.takesArguments(Object.class)))
          .getOnly();

  private static final MethodDescription HASH_MAP_PUT =
      HASH_MAP_TYPE
          .getDeclaredMethods()
          .filter(
              ElementMatchers.named("put")
                  .and(ElementMatchers.takesArguments(Object.class, Object.class)))
          .getOnly();

  private static final MethodDescription MAP_ENTRYSET =
      MAP_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("entrySet")).getOnly();

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

  private static final MethodDescription NEW_LIST_FOR_ITERABLE =
      SELF_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("newListForIterable"))
          .getOnly();

  private static final MethodDescription NEW_MAP_FOR_MAP =
      SELF_LOADED_TYPE
          .getDeclaredMethods()
          .filter(ElementMatchers.named("newHashMapForMap"))
          .getOnly();

  private static final MethodDescription CLEAR_RANGE =
      SELF_LOADED_TYPE.getDeclaredMethods().filter(ElementMatchers.named("clearRange")).getOnly();

  /** Bytecode helper: allocates an {@link ArrayList} pre-sized to match the input iterable. */
  public static ArrayList<Object> newListForIterable(Iterable<?> iterable) {
    return iterable instanceof Collection
        ? new ArrayList<>(((Collection<?>) iterable).size())
        : new ArrayList<>();
  }

  /** Bytecode helper: allocates a {@link HashMap} pre-sized to match the input map. */
  public static HashMap<Object, Object> newHashMapForMap(Map<?, ?> map) {
    return Maps.newHashMapWithExpectedSize(map.size());
  }

  /** Bytecode helper: clears a slice of a reused scratch array when a sub-row/container is null. */
  public static void clearRange(Object[] array, int fromIndex, int toIndex) {
    Arrays.fill(array, fromIndex, toIndex, null);
  }

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
      new ConcurrentHashMap<>();

  // Non-evicting bounded cache so generated selector classes never thrash Metaspace; descriptors
  // past MAX_GENERATED_SELECTOR_CLASSES fall back cleanly to FlatRowSelector / SelectHelpers.
  private static final Map<SchemaAndDescriptor, Constructor<? extends RowSelector>>
      CACHED_SELECTOR_CONSTRUCTORS = new ConcurrentHashMap<>();
  private static final AtomicInteger GENERATED_SELECTOR_COUNT = new AtomicInteger();

  static RowSelector getRowSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    return CACHED_SELECTORS.computeIfAbsent(
        SchemaAndDescriptor.of(inputSchema, fieldAccessDescriptor),
        key ->
            createRowSelectorForOutputSchema(
                key.getSchema(),
                key.getFieldAccessDecriptor(),
                SelectHelpers.getOutputSchema(key.getSchema(), key.getFieldAccessDecriptor())));
  }

  static RowSelector getRowSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, Schema outputSchema) {
    return createRowSelectorForOutputSchema(inputSchema, fieldAccessDescriptor, outputSchema);
  }

  private static RowSelector createRowSelectorForOutputSchema(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, Schema outputSchema) {
    // 1. Empty selection fast path.
    int outputFieldCount = outputSchema.getFieldCount();
    if (outputFieldCount == 0) {
      final Row emptyRow = Row.attachValues(outputSchema, new Object[0]);
      return input -> emptyRow;
    }

    // 2. Top-level wildcard (*) fast path.
    if (fieldAccessDescriptor.getAllFields()) {
      if (outputSchema == inputSchema || inputSchema.equals(outputSchema)) {
        return input -> input == null ? Row.nullRow(outputSchema) : input;
      }
      return new RewrapRowSelector(outputSchema);
    }

    // 3. Single nested wildcard (e.g. "nested.*") fast path (without LIST/MAP qualifiers).
    if (fieldAccessDescriptor.getFieldsAccessed().isEmpty()
        && fieldAccessDescriptor.getNestedFieldsAccessed().size() == 1) {
      Map.Entry<FieldDescriptor, FieldAccessDescriptor> only =
          fieldAccessDescriptor.getNestedFieldsAccessed().entrySet().iterator().next();
      FieldDescriptor fd = only.getKey();
      if (fd.getQualifiers().isEmpty() && only.getValue().getAllFields()) {
        int fieldId = fd.getFieldId();
        Schema nestedSchema = inputSchema.getField(fieldId).getType().getRowSchema();
        boolean schemaMatches = outputSchema == nestedSchema || outputSchema.equals(nestedSchema);
        return new SingleNestedWildcardSelector(fieldId, outputSchema, schemaMatches);
      }
    }

    // 4. Wide flat/nested/container guard to avoid exceeding C2 HugeMethodLimit / 64KB limit.
    if (estimateBytecodeBytes(inputSchema, fieldAccessDescriptor) > MAX_ESTIMATED_BYTECODE_BYTES) {
      return fallbackSelector(inputSchema, fieldAccessDescriptor, outputSchema);
    }

    SchemaAndDescriptor key = SchemaAndDescriptor.of(inputSchema, fieldAccessDescriptor);
    Constructor<? extends RowSelector> ctor = CACHED_SELECTOR_CONSTRUCTORS.get(key);
    if (ctor == null) {
      if (GENERATED_SELECTOR_COUNT.get() >= MAX_GENERATED_SELECTOR_CLASSES) {
        return fallbackSelector(inputSchema, fieldAccessDescriptor, outputSchema);
      }
      ctor =
          CACHED_SELECTOR_CONSTRUCTORS.computeIfAbsent(
              key,
              k -> {
                if (GENERATED_SELECTOR_COUNT.incrementAndGet() > MAX_GENERATED_SELECTOR_CLASSES) {
                  return null;
                }
                return compileRowSelectorClass(k.getSchema(), k.getFieldAccessDecriptor());
              });
      if (ctor == null) {
        return fallbackSelector(inputSchema, fieldAccessDescriptor, outputSchema);
      }
    }
    try {
      return ctor.newInstance(outputSchema);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Unable to instantiate generated RowSelector", e);
    }
  }

  private static RowSelector fallbackSelector(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor, Schema outputSchema) {
    if (fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
      int[] ids =
          fieldAccessDescriptor.fieldIdsAccessed().stream().mapToInt(Integer::intValue).toArray();
      return new FlatRowSelector(ids, outputSchema);
    }
    return SelectHelpers.getRowSelector(inputSchema, fieldAccessDescriptor, outputSchema);
  }

  private static int estimateBytecodeBytes(Schema inputSchema, FieldAccessDescriptor descriptor) {
    if (descriptor.getAllFields()) {
      return inputSchema.getFieldCount() * 24;
    }
    int bytes = descriptor.getFieldsAccessed().size() * 24;
    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        descriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fd = nested.getKey();
      FieldType type = inputSchema.getField(fd.getFieldId()).getType();
      List<Qualifier> qualifiers = fd.getQualifiers();
      bytes += 40;
      for (Qualifier q : qualifiers) {
        switch (q.getKind()) {
          case LIST:
            bytes += 120;
            type = checkNotNull(type.getCollectionElementType());
            break;
          case MAP:
            bytes += 140;
            type = checkNotNull(type.getMapValueType());
            break;
          default:
            throw new RuntimeException("Unexpected type " + q.getKind());
        }
      }
      bytes += estimateBytecodeBytes(checkNotNull(type.getRowSchema()), nested.getValue());
      if (bytes > MAX_ESTIMATED_BYTECODE_BYTES) {
        return bytes;
      }
    }
    return bytes;
  }

  private static final class RewrapRowSelector implements RowSelector {
    private final Schema outputSchema;

    RewrapRowSelector(Schema outputSchema) {
      this.outputSchema = outputSchema;
    }

    @Override
    public Row select(Row input) {
      if (input == null) {
        return Row.nullRow(outputSchema);
      }
      return SelectHelpers.rewrapRow(input, outputSchema);
    }
  }

  private static final class SingleNestedWildcardSelector implements RowSelector {
    private final int fieldId;
    private final Schema outputSchema;
    private final boolean schemaMatches;

    SingleNestedWildcardSelector(int fieldId, Schema outputSchema, boolean schemaMatches) {
      this.fieldId = fieldId;
      this.outputSchema = outputSchema;
      this.schemaMatches = schemaMatches;
    }

    @Override
    public Row select(Row input) {
      if (input == null) {
        return Row.nullRow(outputSchema);
      }
      Row nested = input.getValue(fieldId);
      if (nested == null) {
        return Row.nullRow(outputSchema);
      }
      if (schemaMatches) {
        return nested;
      }
      return SelectHelpers.rewrapRow(nested, outputSchema);
    }
  }

  private static final class FlatRowSelector implements RowSelector {
    private final int[] fieldIds;
    private final Schema outputSchema;

    FlatRowSelector(int[] fieldIds, Schema outputSchema) {
      this.fieldIds = fieldIds;
      this.outputSchema = outputSchema;
    }

    @Override
    public Row select(Row input) {
      int n = fieldIds.length;
      Object[] out = new Object[n];
      if (input == null) {
        return Row.attachValues(outputSchema, out);
      }
      if (input instanceof RowWithStorage) {
        @Nullable Object @Nullable [] raw = ((RowWithStorage) input).getRawArray();
        if (raw != null) {
          for (int i = 0; i < n; i++) {
            out[i] = raw[fieldIds[i]];
          }
          return Row.attachValues(outputSchema, out);
        }
      }
      for (int i = 0; i < n; i++) {
        out[i] = input.getValue(fieldIds[i]);
      }
      return Row.attachValues(outputSchema, out);
    }
  }

  private static Constructor<? extends RowSelector> compileRowSelectorClass(
      Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    int outputFieldCount = SelectHelpers.countRowFields(inputSchema, fieldAccessDescriptor);
    try {
      DynamicType.Builder<RowSelector> builder =
          BYTE_BUDDY
              .subclass(RowSelector.class)
              .method(ElementMatchers.named("select"))
              .intercept(
                  new SelectInstruction(fieldAccessDescriptor, inputSchema, outputFieldCount))
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
          .load(
              ReflectHelpers.findClassLoader(Row.class.getClassLoader()),
              getClassLoadingStrategy(RowSelector.class))
          .getLoaded()
          .getDeclaredConstructor(Schema.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Unable to generate RowSelector", e);
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
        return new ByteCodeAppender.Size(size.getMaximalSize(), numLocals);
      };
    }
  }

  // Manage array creation and appending.
  private static class ArrayManager {
    private final StackManipulation.Size sizeDecreaseArrayStore;
    private final int arraySize;
    private final boolean reused;
    int currentArrayField = 0;

    ArrayManager(int arraySize, boolean reused) {
      // Size decreases by index and array reference (2) and array element (1) after each store.
      this.sizeDecreaseArrayStore =
          StackSize.DOUBLE
              .toDecreasingSize()
              .aggregate(Generic.OBJECT.getStackSize().toDecreasingSize());
      this.arraySize = arraySize;
      this.reused = reused;
    }

    boolean isReused() {
      return reused;
    }

    StackManipulation createArray() {
      return new StackManipulation() {
        @Override
        public boolean isValid() {
          return true;
        }

        @Override
        public Size apply(MethodVisitor methodVisitor, Context context) {
          StackManipulation.Size size =
              IntegerConstant.forValue(arraySize).apply(methodVisitor, context);
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
        public StackManipulation.Size apply(MethodVisitor methodVisitor, Context context) {
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

    StackManipulation.Size emitClearRange(
        int fromIndex, int toIndex, MethodVisitor mv, Context ctx) {
      if (fromIndex >= toIndex) {
        return new StackManipulation.Size(0, 0);
      }
      return new StackManipulation.Compound(
              Duplication.SINGLE,
              IntegerConstant.forValue(fromIndex),
              IntegerConstant.forValue(toIndex),
              MethodInvocation.invoke(CLEAR_RANGE))
          .apply(mv, ctx);
    }
  }

  private static class SelectInstruction implements Implementation {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final Schema inputSchema;
    private final int outputFieldCount;
    private final ByteBuddyLocalVariableManager localVariables;
    private static final int INPUT_ROW_ARG = 1;
    private final int currentSelectRowArg;
    private final int fieldValueArg;

    private static final int NUM_FUNCTION_ARGS = 2; // Args are this and the row to select.

    public SelectInstruction(
        FieldAccessDescriptor fieldAccessDescriptor, Schema inputSchema, int outputFieldCount) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.inputSchema = inputSchema;
      this.outputFieldCount = outputFieldCount;
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

        // Push OUTPUTSCHEMA directly for Row.attachValues(Schema, Object[]) (avoids Row.Builder).
        StackManipulation loadOutputSchema =
            new StackManipulation.Compound(
                MethodVariableAccess.loadThis(),
                FieldAccess.forField(
                        implementationTarget
                            .getInstrumentedType()
                            .getDeclaredFields()
                            .filter(ElementMatchers.named(SELECT_SCHEMA_FIELD_NAME))
                            .getOnly())
                    .read());
        size = size.aggregate(loadOutputSchema.apply(methodVisitor, implementationContext));

        // Fast path for flat top-level field subset selections: direct RowWithStorage raw array
        // reads avoid virtual Row.getValue(i) calls entirely.
        if (fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()
            && !fieldAccessDescriptor.getAllFields()) {
          ArrayManager arrayManager = new ArrayManager(outputFieldCount, false);
          size =
              size.aggregate(
                  arrayManager.createArray().apply(methodVisitor, implementationContext));

          List<Integer> ids = fieldAccessDescriptor.fieldIdsAccessed();
          Label doneLabel = new Label();
          Label slowPathLabel = new Label();

          // if (input == null) goto done (array already filled with nulls)
          size =
              size.aggregate(
                  MethodVariableAccess.REFERENCE
                      .loadFrom(INPUT_ROW_ARG)
                      .apply(methodVisitor, implementationContext));
          methodVisitor.visitJumpInsn(Opcodes.IFNULL, doneLabel);

          // Check input instanceof RowWithStorage and getRawArray() != null.
          size =
              size.aggregate(
                  MethodVariableAccess.REFERENCE
                      .loadFrom(INPUT_ROW_ARG)
                      .apply(methodVisitor, implementationContext));
          methodVisitor.visitTypeInsn(Opcodes.INSTANCEOF, ROW_WITH_STORAGE_TYPE.getInternalName());
          methodVisitor.visitJumpInsn(Opcodes.IFEQ, slowPathLabel);

          int rawArrVar = localVariables.createVariable();
          size =
              size.aggregate(
                  new StackManipulation.Compound(
                          MethodVariableAccess.REFERENCE.loadFrom(INPUT_ROW_ARG),
                          TypeCasting.to(ROW_WITH_STORAGE_TYPE),
                          MethodInvocation.invoke(ROW_WITH_STORAGE_RAW_ARRAY),
                          Duplication.SINGLE,
                          localVariables.writeVariable(rawArrVar))
                      .apply(methodVisitor, implementationContext));
          methodVisitor.visitJumpInsn(Opcodes.IFNULL, slowPathLabel);

          // Fast array-to-array copy.
          for (int i = 0; i < ids.size(); i++) {
            StackManipulation readRaw =
                new StackManipulation.Compound(
                    localVariables.readVariable(rawArrVar),
                    IntegerConstant.forValue(ids.get(i)),
                    ArrayAccess.REFERENCE.load());
            size =
                size.aggregate(
                    arrayManager.store(i, readRaw).apply(methodVisitor, implementationContext));
          }
          methodVisitor.visitJumpInsn(Opcodes.GOTO, doneLabel);

          // Slow path: invoke Row.getValue(id) for each selected field (no per-field null checks).
          methodVisitor.visitLabel(slowPathLabel);
          for (int i = 0; i < ids.size(); i++) {
            StackManipulation readVirtual =
                new StackManipulation.Compound(
                    MethodVariableAccess.REFERENCE.loadFrom(INPUT_ROW_ARG),
                    IntegerConstant.forValue(ids.get(i)),
                    MethodInvocation.invoke(ROW_GET_VALUE));
            size =
                size.aggregate(
                    arrayManager.store(i, readVirtual).apply(methodVisitor, implementationContext));
          }

          methodVisitor.visitLabel(doneLabel);
          StackManipulation attachAndReturn =
              new StackManipulation.Compound(
                  MethodInvocation.invoke(ROW_ATTACH_VALUES), MethodReturn.REFERENCE);
          size = size.aggregate(attachAndReturn.apply(methodVisitor, implementationContext));
          return new ByteCodeAppender.Size(
              Math.max(size.getMaximalSize(), 6), localVariables.getTotalNumVariables());
        }

        // General path (handles arbitrary nesting, lists, iterables, maps, null sub-rows).
        ArrayManager arrayManager = new ArrayManager(outputFieldCount, false);
        size =
            size.aggregate(arrayManager.createArray().apply(methodVisitor, implementationContext));

        // Store the current input row into currentSelectRowArg (already statically typed Row).
        size =
            size.aggregate(
                localVariables
                    .copy(INPUT_ROW_ARG, currentSelectRowArg)
                    .apply(methodVisitor, implementationContext));

        size =
            size.aggregate(
                selectIntoArray(
                    inputSchema,
                    fieldAccessDescriptor,
                    arrayManager,
                    methodVisitor,
                    implementationContext));

        StackManipulation attachToRow =
            new StackManipulation.Compound(
                MethodInvocation.invoke(ROW_ATTACH_VALUES), MethodReturn.REFERENCE);
        size = size.aggregate(attachToRow.apply(methodVisitor, implementationContext));
        return new ByteCodeAppender.Size(
            Math.max(size.getMaximalSize(), 8), localVariables.getTotalNumVariables());
      };
    }

    // Read a field directly from the non-null Row in currentSelectRowArg.
    private StackManipulation getNonNullCurrentRowFieldValue(int fieldId) {
      return new StackManipulation.Compound(
          localVariables.readVariable(currentSelectRowArg),
          IntegerConstant.forValue(fieldId),
          MethodInvocation.invoke(ROW_GET_VALUE));
    }

    // Generate bytecode to select all specified fields from the Row in currentSelectRowArg.
    // Performs ONE null check per sub-row instead of per-field null checks and checkcasts.
    StackManipulation.Size selectIntoArray(
        Schema inputSchema,
        FieldAccessDescriptor fieldAccessDescriptor,
        ArrayManager arrayManager,
        MethodVisitor methodVisitor,
        Context implementationContext) {
      StackManipulation.Size size = new StackManipulation.Size(0, 0);
      int startSlot = arrayManager.currentArrayField;

      Label nullRowLabel = new Label();
      Label doneLabel = new Label();

      // Single null check for the entire (sub-)row.
      size =
          size.aggregate(
              localVariables
                  .readVariable(currentSelectRowArg)
                  .apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFNULL, nullRowLabel);

      if (fieldAccessDescriptor.getAllFields()) {
        for (int i = 0; i < inputSchema.getFieldCount(); i++) {
          size =
              size.aggregate(
                  arrayManager
                      .append(getNonNullCurrentRowFieldValue(i))
                      .apply(methodVisitor, implementationContext));
        }
      } else {
        for (int i : fieldAccessDescriptor.fieldIdsAccessed()) {
          size =
              size.aggregate(
                  arrayManager
                      .append(getNonNullCurrentRowFieldValue(i))
                      .apply(methodVisitor, implementationContext));
        }

        for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
            fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
          FieldDescriptor field = nested.getKey();
          FieldAccessDescriptor nestedAccess = nested.getValue();
          FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();

          size =
              size.aggregate(
                  selectIntoArrayHelper(
                      field.getQualifiers(),
                      0,
                      field.getFieldId(),
                      nestedAccess,
                      nestedInputType,
                      arrayManager,
                      methodVisitor,
                      implementationContext));
        }
      }

      int endSlot = arrayManager.currentArrayField;
      if (arrayManager.isReused() && endSlot > startSlot) {
        methodVisitor.visitJumpInsn(Opcodes.GOTO, doneLabel);
        methodVisitor.visitLabel(nullRowLabel);
        size =
            size.aggregate(
                arrayManager.emitClearRange(
                    startSlot, endSlot, methodVisitor, implementationContext));
        methodVisitor.visitLabel(doneLabel);
      } else {
        // Fresh Object[] is already zero-initialized to null at every slot.
        methodVisitor.visitLabel(nullRowLabel);
      }
      return size;
    }

    // Load the current field value to process. If fieldId != -1, read from the non-null Row
    // stored in currentSelectRowArg; otherwise read from fieldValueArg.
    private StackManipulation loadFieldValue(int fieldId) {
      if (fieldId != -1) {
        return getNonNullCurrentRowFieldValue(fieldId);
      } else {
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
        // Cast to Row ONCE on entering this sub-row, rather than on every field read.
        ByteBuddyLocalVariableManager.BackupLocalVariable backup =
            localVariables.backupVariable(currentSelectRowArg);
        StackManipulation updateLocalVariable =
            new StackManipulation.Compound(
                loadFieldValue(fieldId),
                TypeCasting.to(ROW_LOADED_TYPE),
                backup.backup(),
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
      int nestedFieldCount =
          SelectHelpers.countFields(
              nestedInputType, qualifiers, qualifierPosition + 1, fieldAccessDescriptor);

      int startSlot = arrayManager.currentArrayField;
      int[] outputSlots = new int[nestedFieldCount];
      int[] localVariablesForArrays = new int[nestedFieldCount];
      for (int i = 0; i < nestedFieldCount; i++) {
        outputSlots[i] = arrayManager.reserveSlot();
        localVariablesForArrays[i] = localVariables.createVariable();
      }

      // Read the container ONCE and cast to Iterable.
      int iterableVar = localVariables.createVariable();
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      loadFieldValue(fieldId),
                      TypeCasting.to(ITERABLE_LOADED_TYPE),
                      Duplication.SINGLE,
                      localVariables.writeVariable(iterableVar))
                  .apply(methodVisitor, implementationContext));

      Label onNullLabel = new Label();
      Label doneLabel = new Label();
      methodVisitor.visitJumpInsn(Opcodes.IFNULL, onNullLabel);

      // Allocate K pre-sized ArrayLists once and store them into output slots + locals.
      for (int i = 0; i < nestedFieldCount; i++) {
        StackManipulation createList =
            new StackManipulation.Compound(
                localVariables.readVariable(iterableVar),
                MethodInvocation.invoke(NEW_LIST_FOR_ITERABLE),
                Duplication.SINGLE,
                localVariables.writeVariable(localVariablesForArrays[i]));
        size =
            size.aggregate(
                arrayManager
                    .store(outputSlots[i], createList)
                    .apply(methodVisitor, implementationContext));
      }

      // Allocate a single reusable scratch Object[nestedFieldCount] outside the loop.
      ArrayManager nestedArrayManager = new ArrayManager(nestedFieldCount, true);
      int scratchArrayVar = localVariables.createVariable();
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      nestedArrayManager.createArray(),
                      localVariables.writeVariable(scratchArrayVar))
                  .apply(methodVisitor, implementationContext));

      // Obtain iterator from iterableVar.
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      localVariables.readVariable(iterableVar),
                      MethodInvocation.invoke(ITERABLE_ITERATOR))
                  .apply(methodVisitor, implementationContext));

      Label startLoopLabel = new Label();
      Label exitLoopLabel = new Label();
      methodVisitor.visitLabel(startLoopLabel);

      size =
          size.aggregate(
              new StackManipulation.Compound(
                      Duplication.SINGLE, MethodInvocation.invoke(ITERATOR_HASNEXT))
                  .apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFEQ, exitLoopLabel);

      // Read next element and save in fieldValueArg for the recursive select.
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      Duplication.SINGLE,
                      MethodInvocation.invoke(ITERATOR_NEXT),
                      localVariables.writeVariable(fieldValueArg))
                  .apply(methodVisitor, implementationContext));

      // Push scratch array, populate it (with reused-array null clearing), and distribute fields.
      size =
          size.aggregate(
              localVariables
                  .readVariable(scratchArrayVar)
                  .apply(methodVisitor, implementationContext));
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

      for (int i = 0; i < nestedFieldCount; i++) {
        size =
            size.aggregate(
                new StackManipulation.Compound(
                        localVariables.readVariable(localVariablesForArrays[i]),
                        localVariables.readVariable(scratchArrayVar),
                        IntegerConstant.forValue(i),
                        ArrayAccess.REFERENCE.load(),
                        MethodInvocation.invoke(ARRAY_LIST_ADD),
                        Removal.SINGLE)
                    .apply(methodVisitor, implementationContext));
      }
      // Pop the scratch array pushed before selectIntoArrayHelper.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
      methodVisitor.visitLabel(exitLoopLabel);
      // Pop the iterator.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      int endSlot = arrayManager.currentArrayField;
      if (arrayManager.isReused() && endSlot > startSlot) {
        methodVisitor.visitJumpInsn(Opcodes.GOTO, doneLabel);
        methodVisitor.visitLabel(onNullLabel);
        size =
            size.aggregate(
                arrayManager.emitClearRange(
                    startSlot, endSlot, methodVisitor, implementationContext));
        methodVisitor.visitLabel(doneLabel);
      } else {
        methodVisitor.visitLabel(onNullLabel);
      }
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
      int nestedFieldCount =
          SelectHelpers.countFields(
              nestedInputType, qualifiers, qualifierPosition + 1, fieldAccessDescriptor);

      int startSlot = arrayManager.currentArrayField;
      int[] outputSlots = new int[nestedFieldCount];
      int[] localVariablesForMaps = new int[nestedFieldCount];
      for (int i = 0; i < nestedFieldCount; i++) {
        outputSlots[i] = arrayManager.reserveSlot();
        localVariablesForMaps[i] = localVariables.createVariable();
      }

      // Read the map ONCE and cast to Map.
      int mapVar = localVariables.createVariable();
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      loadFieldValue(fieldId),
                      TypeCasting.to(MAP_LOADED_TYPE),
                      Duplication.SINGLE,
                      localVariables.writeVariable(mapVar))
                  .apply(methodVisitor, implementationContext));

      Label onNullLabel = new Label();
      Label doneLabel = new Label();
      methodVisitor.visitJumpInsn(Opcodes.IFNULL, onNullLabel);

      // Allocate K pre-sized HashMaps once and store them into output slots + locals.
      for (int i = 0; i < nestedFieldCount; i++) {
        StackManipulation createMap =
            new StackManipulation.Compound(
                localVariables.readVariable(mapVar),
                MethodInvocation.invoke(NEW_MAP_FOR_MAP),
                Duplication.SINGLE,
                localVariables.writeVariable(localVariablesForMaps[i]));
        size =
            size.aggregate(
                arrayManager
                    .store(outputSlots[i], createMap)
                    .apply(methodVisitor, implementationContext));
      }

      // Allocate a single reusable scratch Object[nestedFieldCount] outside the loop.
      ArrayManager nestedArrayManager = new ArrayManager(nestedFieldCount, true);
      int scratchArrayVar = localVariables.createVariable();
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      nestedArrayManager.createArray(),
                      localVariables.writeVariable(scratchArrayVar))
                  .apply(methodVisitor, implementationContext));

      // Obtain entrySet iterator from mapVar.
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      localVariables.readVariable(mapVar),
                      MethodInvocation.invoke(MAP_ENTRYSET),
                      MethodInvocation.invoke(ITERABLE_ITERATOR))
                  .apply(methodVisitor, implementationContext));

      Label startLoopLabel = new Label();
      Label exitLoopLabel = new Label();
      methodVisitor.visitLabel(startLoopLabel);

      size =
          size.aggregate(
              new StackManipulation.Compound(
                      Duplication.SINGLE, MethodInvocation.invoke(ITERATOR_HASNEXT))
                  .apply(methodVisitor, implementationContext));
      methodVisitor.visitJumpInsn(Opcodes.IFEQ, exitLoopLabel);

      int keyVariable = localVariables.createVariable();
      size =
          size.aggregate(
              new StackManipulation.Compound(
                      Duplication.SINGLE,
                      MethodInvocation.invoke(ITERATOR_NEXT),
                      TypeCasting.to(MAPENTRY_LOADED_TYPE),
                      Duplication.SINGLE,
                      MethodInvocation.invoke(MAPENTRY_GETKEY),
                      localVariables.writeVariable(keyVariable),
                      MethodInvocation.invoke(MAPENTRY_GETVALUE),
                      localVariables.writeVariable(fieldValueArg))
                  .apply(methodVisitor, implementationContext));

      // Push scratch array, populate it (with reused-array null clearing), and distribute fields.
      size =
          size.aggregate(
              localVariables
                  .readVariable(scratchArrayVar)
                  .apply(methodVisitor, implementationContext));
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

      for (int i = 0; i < nestedFieldCount; i++) {
        size =
            size.aggregate(
                new StackManipulation.Compound(
                        localVariables.readVariable(localVariablesForMaps[i]),
                        localVariables.readVariable(keyVariable),
                        localVariables.readVariable(scratchArrayVar),
                        IntegerConstant.forValue(i),
                        ArrayAccess.REFERENCE.load(),
                        MethodInvocation.invoke(HASH_MAP_PUT),
                        Removal.SINGLE)
                    .apply(methodVisitor, implementationContext));
      }
      // Pop the scratch array pushed before selectIntoArrayHelper.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      methodVisitor.visitJumpInsn(Opcodes.GOTO, startLoopLabel);
      methodVisitor.visitLabel(exitLoopLabel);
      // Pop the iterator.
      size = size.aggregate(Removal.SINGLE.apply(methodVisitor, implementationContext));

      int endSlot = arrayManager.currentArrayField;
      if (arrayManager.isReused() && endSlot > startSlot) {
        methodVisitor.visitJumpInsn(Opcodes.GOTO, doneLabel);
        methodVisitor.visitLabel(onNullLabel);
        size =
            size.aggregate(
                arrayManager.emitClearRange(
                    startSlot, endSlot, methodVisitor, implementationContext));
        methodVisitor.visitLabel(doneLabel);
      } else {
        methodVisitor.visitLabel(onNullLabel);
      }
      return size;
    }
  }
}
