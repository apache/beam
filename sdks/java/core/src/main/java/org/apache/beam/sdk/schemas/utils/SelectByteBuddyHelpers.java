package org.apache.beam.sdk.schemas.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.asm.AsmVisitorWrapper;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.method.MethodDescription.ForLoadedMethod;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.modifier.FieldManifestation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.modifier.Visibility;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.Generic;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.description.type.TypeDescription.Generic.LazyProjection.ForLoadedFieldType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.Duplication;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.StackSize;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.collection.ArrayFactory;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.constant.IntegerConstant;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.FieldAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.jar.asm.ClassWriter;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.jar.asm.MethodVisitor;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.jar.asm.Opcodes;
import org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

public class SelectByteBuddyHelpers {
    private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();
    private static final String SELECT_SCHEMA_FIELD_NAME = "OUTPUTSCHEMA";

    private static final ForLoadedType ROW_LOADED_TYPE = new ForLoadedType(Row.class);

    interface RowSelector {
        Row select(Row input);
    }

    @AutoValue
    abstract class SchemaAndDescriptor {
        abstract Schema getSchema();

        abstract FieldAccessDescriptor getFieldAccessDecriptor();
    }

    private static Map<SchemaAndDescriptor, RowSelector> CACHED_SELECTORS =
            Maps.newConcurrentMap();

    public static RowSelector getRowSelector(Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
        return CACHED_SELECTORS.computeIfAbsent(
                new AutoValue_SelectByteBuddyHelpers_SchemaAndDescriptor(inputSchema, fieldAccessDescriptor),
                SelectByteBuddyHelpers::createRowSelector);
    }

    static RowSelector createRowSelector(SchemaAndDescriptor schemaAndDescriptor) {
        Schema outputSchema = SelectHelpers.getOutputSchema(
                schemaAndDescriptor.getSchema(), schemaAndDescriptor.getFieldAccessDecriptor());
        try {
            DynamicType.Builder<RowSelector> builder =
                    BYTE_BUDDY
                            .subclass(RowSelector.class)
                            .method(ElementMatchers.named("select"))
                            .intercept(new SelectInstruction(schemaAndDescriptor.getSchema(), schemaAndDescriptor.getFieldAccessDecriptor()))
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
            throw new RuntimeException(
                    "Unable to generate a creator for " + clazz + " with schema " + schema);
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
                                FieldAccess.forField(implementationTarget.getInstrumentedType()
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

    private static class SelectInstruction implements Implementation {
        private final FieldAccessDescriptor fieldAccessDescriptor;
        private final Schema inputSchema;
        private final Schema outputSchema;
        private int currentField;
        private final StackManipulation.Size sizeDecrease;

        public SelectInstruction(FieldAccessDescriptor fieldAccessDescriptor, Schema inputSchema, Schema outputSchema) {
            this.fieldAccessDescriptor = fieldAccessDescriptor;
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.currentField = 0;
            // Size decreases by index and array reference (2) and array element (1, 2) after each element storage.
            sizeDecrease = StackSize.DOUBLE.toDecreasingSize().aggregate(Generic.OBJECT.getStackSize().toDecreasingSize());
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(final Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                int numLocals = 1 + instrumentedMethod.getParameters().size() + 1;
                StackManipulation.Size size = new StackManipulation.Size(0, numLocals);

                // Create the Row builder.
                StackManipulation createRowBuilder = new StackManipulation.Compound(
                        FieldAccess.forField(implementationTarget
                                .getInstrumentedType()
                                .getDeclaredFields()
                                .filter(ElementMatchers.named(SELECT_SCHEMA_FIELD_NAME))
                                .getOnly())
                        .read(),
                        MethodInvocation.invoke(new ForLoadedType(Row.class)
                                .getDeclaredMethods()
                                .filter(ElementMatchers.named("withSchema"))
                                .getOnly()));
                size = size.aggregate(createRowBuilder.apply(methodVisitor, implementationContext));

                // Create a new object array with one entry for each field in the output schema.
                size = size.aggregate(IntegerConstant.forValue(outputSchema.getFieldCount())
                        .apply(methodVisitor, implementationContext));
                methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, Generic.OBJECT.asErasure().getInternalName());
                size = size.aggregate(StackSize.ZERO.toDecreasingSize());

                // Store the current input row into a local variable at offset 2.
                StackManipulation storeRowInLocalVariable = new StackManipulation.Compound(
                        MethodVariableAccess.REFERENCE.loadFrom(1),
                        MethodVariableAccess.REFERENCE.storeAt(2));
                size = size.aggregate(storeRowInLocalVariable.apply(methodVisitor, implementationContext));

                size = size.aggregate(
                        selectIntoRow(inputSchema, outputSchema, fieldAccessDescriptor, methodVisitor, implementationContext));

                // Return the actual row.
                StackManipulation attachToRow = new StackManipulation.Compound(
                        MethodInvocation.invoke(new ForLoadedType(Row.Builder.class)
                                .getDeclaredMethods()
                                .filter(ElementMatchers.named("attachValues"))
                                .getOnly()),
                        MethodInvocation.invoke(new ForLoadedType(Row.Builder.class)
                                .getDeclaredMethods()
                                .filter(ElementMatchers.named("build"))
                                .getOnly()));
                size = size.aggregate(attachToRow.apply(methodVisitor, implementationContext));
                return new Size(size.getMaximalSize(), size.getSizeImpact());
            };
        }

        private static StackManipulation getValue(int i) {
            return new StackManipulation.Compound(
                    MethodVariableAccess.REFERENCE.loadFrom(2),  // Load from the local variable.
                    // TODO: CAST
                    IntegerConstant.forValue(i),
                    MethodInvocation.invoke(ROW_LOADED_TYPE
                            .getDeclaredMethods()
                            .filter(ElementMatchers.named("getValue"))
                            .getOnly()));
        }

        // Generate bytecode to select all specified fields from the Row. The current row being selected is stored
        // in the local variable at position 2.
        StackManipulation.Size selectIntoRow(Schema inputSchema,
                                         FieldAccessDescriptor fieldAccessDescriptor,
                                         MethodVisitor methodVisitor,
                                         Context implementationContext) {
            StackManipulation.Size size = new StackManipulation.Size(0, 0);
            if (fieldAccessDescriptor.getAllFields()) {
                for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
                    StackManipulation stackManipulation = new StackManipulation.Compound(
                            Duplication.SINGLE,  // Duplicate the array reference.
                            IntegerConstant.forValue(currentField),
                            getValue(i));
                    size = size.aggregate(stackManipulation.apply(methodVisitor, implementationContext));
                    methodVisitor.visitInsn(Opcodes.AASTORE);
                    size = size.aggregate(sizeDecrease);
                    currentField++;
                }
                return size;
            }

            for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
                // TODO: Once we support specific qualifiers (like array slices), extract them here.
                // TODO: REFACTOR
                StackManipulation stackManipulation = new StackManipulation.Compound(
                        Duplication.SINGLE,  // Duplicate the array reference.
                        IntegerConstant.forValue(currentField),
                        getValue(fieldId));
                size = size.aggregate(stackManipulation.apply(methodVisitor, implementationContext));
                methodVisitor.visitInsn(Opcodes.AASTORE);
                size = size.aggregate(sizeDecrease);
                currentField++;
            }

            for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
                    fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
                FieldDescriptor field = nested.getKey();
                FieldAccessDescriptor nestedAccess = nested.getValue();
                FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
                FieldType nestedOutputType = outputSchema.getField(currentField).getType();
                size = size.aggregate(selectIntoRowHelper(
                        field.getQualifiers(),
                        field.getFieldId(),
                        nestedAccess,
                        nestedInputType,
                        nestedOutputType,
                        methodVisitor,
                        implementationContext));
            }
            return size;
        }

        private StackManipulation.Size selectIntoRowHelper(
                List<Qualifier> qualifiers,
                int fieldId,
                FieldAccessDescriptor fieldAccessDescriptor,
                FieldType inputType,
                FieldType outputType,
                MethodVisitor methodVisitor,
                Context implementationContext) {
            StackManipulation.Size size = new StackManipulation.Size(0, 0);
            if (qualifiers.isEmpty()) {
                StackManipulation updateLocalVariable = new StackManipulation.Compound(
                        // Push the existing row value onto the stack so we can restore it later.
                        MethodVariableAccess.REFERENCE.loadFrom(2),
                        // Read the ith value and store it into the local row variable.
                        getValue(fieldId),
                        MethodVariableAccess.REFERENCE.storeAt(2));
                size = size.aggregate(updateLocalVariable.apply(methodVisitor, implementationContext));
                size = size.aggregate(selectIntoRow(inputType.getRowSchema(), fieldAccessDescriptor, methodVisitor, implementationContext);

                // Restore the old row value from what's stored on the stack.
                size = size.aggregate(MethodVariableAccess.REFERENCE.storeAt(2)
                        .apply(methodVisitor, implementationContext));
                return size;
            }


            // There are qualifiers. That means that the result will be either a list or a map, so
            // construct the result and add that to our Row.
            return  selectIntoRowWithQualifiers(
                    qualifiers, 0, fieldId, fieldAccessDescriptor, inputType, outputType, methodVisitor, implementationContext);
        }

        private StackManipulation.Size selectIntoRowWithQualifiers(
                List<Qualifier> qualifiers,
                int qualifierPosition,
                int fieldId,
                FieldAccessDescriptor fieldAccessDescriptor,
                FieldType inputType,
                FieldType outputType,
                MethodVisitor methodVisitor,
                Context implementationContext) {
            StackManipulation.Size size = new StackManipulation.Size(0, 0);
            if (qualifierPosition >= qualifiers.size()) {
                // We have already constructed all arrays and maps. What remains must be a Row.
                StackManipulation updateLocalVariable = new StackManipulation.Compound(
                        // Push the existing row value onto the stack so we can restore it later.
                        MethodVariableAccess.REFERENCE.loadFrom(2),
                        // Read the ith value and store it into the local row variable.
                        getValue(fieldId),
                        MethodVariableAccess.REFERENCE.storeAt(2));
                size = size.aggregate(updateLocalVariable.apply(methodVisitor, implementationContext));
                size = size.aggregate(selectIntoRow(inputType.getRowSchema(), fieldAccessDescriptor, methodVisitor, implementationContext);

                // Restore the old row value from what's stored on the stack.
                size = size.aggregate(MethodVariableAccess.REFERENCE.storeAt(2)
                        .apply(methodVisitor, implementationContext));
                return size;
            }

            Qualifier qualifier = qualifiers.get(qualifierPosition);
            switch (qualifier.getKind()) {
                case LIST:
                {
                    FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
                    FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());
                    Iterable<Object> iterable = (Iterable) value;

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
                    // TODO: doing this on each element might be inefficient. Consider caching this, or
                    // using codegen based on the schema.
                    Schema nestedSchema = getOutputSchema(tempSchema, tempAccessDescriptor);

                    List<List<Object>> selectedLists =
                            org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayListWithCapacity(nestedSchema.getFieldCount());
                    for (int i = 0; i < nestedSchema.getFieldCount(); i++) {
                        selectedLists.add(org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayListWithCapacity(Iterables.size(iterable)));
                    }
                    for (Object o : iterable) {
                        Row.Builder selectElementBuilder = Row.withSchema(nestedSchema);
                        selectIntoRowWithQualifiers(
                                qualifiers,
                                qualifierPosition + 1,
                                o,
                                selectElementBuilder,
                                fieldAccessDescriptor,
                                nestedInputType,
                                nestedOutputType);

                        Row elementBeforeDistribution = selectElementBuilder.build();
                        for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
                            selectedLists.get(i).add(elementBeforeDistribution.getValue(i));
                        }
                    }
                    for (List aList : selectedLists) {
                        output.addValue(aList);
                    }
                    break;
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
                    Schema nestedSchema = getOutputSchema(tempSchema, tempAccessDescriptor);
                    List<Map> selectedMaps = org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayListWithExpectedSize(nestedSchema.getFieldCount());
                    for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
                        selectedMaps.add(org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps.newHashMap());
                    }

                    Map<Object, Object> map = (Map) value;
                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        Row.Builder selectValueBuilder = Row.withSchema(nestedSchema);
                        selectIntoRowWithQualifiers(
                                qualifiers,
                                qualifierPosition + 1,
                                entry.getValue(),
                                selectValueBuilder,
                                fieldAccessDescriptor,
                                nestedInputType,
                                nestedOutputType);

                        Row valueBeforeDistribution = selectValueBuilder.build();
                        for (int i = 0; i < nestedSchema.getFieldCount(); ++i) {
                            selectedMaps.get(i).put(entry.getKey(), valueBeforeDistribution.getValue(i));
                        }
                    }
                    for (Map aMap : selectedMaps) {
                        output.addValue(aMap);
                    }
                    break;
                }
                default:
                    throw new RuntimeException("Unexpected type " + qualifier.getKind());
            }
    }
}