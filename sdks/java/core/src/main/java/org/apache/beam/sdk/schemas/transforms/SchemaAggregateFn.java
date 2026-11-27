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
package org.apache.beam.sdk.schemas.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldTypeDescriptors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** This is the builder used by {@link Group} to build up a composed {@link CombineFn}. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes",
  "unchecked"
})
class SchemaAggregateFn {
  static Inner create() {
    return new AutoValue_SchemaAggregateFn_Inner.Builder()
        .setFieldAggregations(Lists.newArrayList())
        .build();
  }

  /**
   * Attempts to resolve {@code descriptor} to a direct field-index chain (with no intervening
   * array/map qualifiers or nested row flattening). Returns {@code null} when a general {@link
   * RowSelector} must be used.
   */
  private static int @Nullable [] tryResolveDirectPath(
      Schema inputSchema, FieldAccessDescriptor descriptor) {
    int[] buf = new int[4];
    int depth = 0;
    Schema curSchema = inputSchema;
    FieldAccessDescriptor cur = descriptor;
    while (true) {
      if (cur.getAllFields()) {
        return null;
      }
      List<FieldDescriptor> direct = cur.getFieldsAccessed();
      Map<FieldDescriptor, FieldAccessDescriptor> nested = cur.getNestedFieldsAccessed();
      if (direct.size() == 1 && nested.isEmpty()) {
        FieldDescriptor fd = direct.get(0);
        Integer id = fd.getFieldId();
        if (id == null || !fd.getQualifiers().isEmpty()) {
          return null;
        }
        FieldType leafType = curSchema.getField(id).getType();
        // For nested ROW leaves, SelectHelpers flattening could unpack its child field.
        if (leafType.getTypeName() == TypeName.ROW) {
          return null;
        }
        if (depth == buf.length) {
          buf = Arrays.copyOf(buf, depth * 2);
        }
        buf[depth++] = id;
        return Arrays.copyOf(buf, depth);
      }
      if (direct.isEmpty() && nested.size() == 1) {
        Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
            nested.entrySet().iterator().next();
        FieldDescriptor fd = entry.getKey();
        Integer id = fd.getFieldId();
        if (id == null || !fd.getQualifiers().isEmpty()) {
          return null;
        }
        FieldType nodeType = curSchema.getField(id).getType();
        if (nodeType.getTypeName() != TypeName.ROW) {
          return null;
        }
        if (depth == buf.length) {
          buf = Arrays.copyOf(buf, depth * 2);
        }
        buf[depth++] = id;
        curSchema = nodeType.getRowSchema();
        cur = entry.getValue();
      } else {
        return null;
      }
    }
  }

  private static @Nullable LogicalType<Object, Object>[] buildLogicalUnwrapChain(
      FieldType fieldType) {
    if (!fieldType.getTypeName().isLogicalType()) {
      return null;
    }
    List<LogicalType<Object, Object>> chain = new ArrayList<>(2);
    FieldType cur = fieldType;
    while (cur.getTypeName().isLogicalType()) {
      LogicalType<Object, Object> lt = (LogicalType<Object, Object>) cur.getLogicalType();
      chain.add(lt);
      cur = lt.getBaseType();
    }
    return chain.toArray(new LogicalType[0]);
  }

  private static @Nullable Object applyUnwrapChain(
      @Nullable Object value, LogicalType<Object, Object> @Nullable [] chain) {
    if (value == null || chain == null) {
      return value;
    }
    Object cur = value;
    for (int i = 0; i < chain.length; i++) {
      cur = chain[i].toBaseType(cur);
      if (cur == null) {
        return null;
      }
    }
    return cur;
  }

  /** Implementation of {@link #create}. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings("rawtypes")
  abstract static class Inner extends CombineFn<Row, Object[], Row> {
    private static final byte OUT_DIRECT = 0;
    private static final byte OUT_ARRAY = 1;
    private static final byte OUT_DATETIME = 2;
    private static final byte OUT_BYTES = 3;

    // Transient execution plan derived from fieldAggregations/inputSchema/outputSchema to eliminate
    // per-row/per-key allocation and double-hashing inside CombineFns.CoCombineResult.
    private transient volatile @Nullable ExecutionPlan plan;

    private static final class ExecutionPlan {
      final int count;
      final CombineFn<Object, Object, Object>[] combineFns;
      final FieldExtractor[] extractors;
      // Non-negative if extract[i] can reuse a previously extracted value computed at slot < i.
      final int[] reuseSlot;
      final boolean hasReuse;
      final byte[] outputCoercions;
      final boolean useFullRowBuilder;
      final boolean[] outputNullable;
      final boolean[] outputArrayElemNonNull;
      final Schema outputSchema;

      ExecutionPlan(Schema inputSchema, Schema outputSchema, List<FieldAggregation> aggregations) {
        int n = aggregations.size();
        this.count = n;
        this.combineFns = new CombineFn[n];
        this.extractors = new FieldExtractor[n];
        this.reuseSlot = new int[n];
        Arrays.fill(this.reuseSlot, -1);
        this.outputCoercions = new byte[n];
        this.outputNullable = new boolean[n];
        this.outputArrayElemNonNull = new boolean[n];
        this.outputSchema = outputSchema;

        boolean anyReuse = false;
        boolean fullBuilder = false;
        Map<Object, Integer> seenExtractors = new HashMap<>(n * 2);

        for (int i = 0; i < n; i++) {
          FieldAggregation agg = aggregations.get(i);
          combineFns[i] = agg.fn;
          extractors[i] = buildExtractor(inputSchema, agg);

          Object extKey =
              new java.util.AbstractMap.SimpleImmutableEntry<>(
                  agg.fieldsToAggregate, agg.aggregateBaseValues);
          Integer prev = seenExtractors.get(extKey);
          if (prev != null) {
            reuseSlot[i] = prev;
            anyReuse = true;
          } else {
            seenExtractors.put(extKey, i);
          }

          FieldType outType = outputSchema.getField(i).getType();
          outputNullable[i] = outType.getNullable();
          switch (outType.getTypeName()) {
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case DECIMAL:
            case ROW:
              outputCoercions[i] = OUT_DIRECT;
              break;
            case DATETIME:
              outputCoercions[i] = OUT_DATETIME;
              break;
            case BYTES:
              outputCoercions[i] = OUT_BYTES;
              break;
            case ARRAY:
              FieldType elemType = outType.getCollectionElementType();
              if (elemType == null
                  || elemType.getTypeName() == TypeName.DATETIME
                  || elemType.getTypeName() == TypeName.BYTES
                  || elemType.getTypeName() == TypeName.ROW
                  || elemType.getTypeName() == TypeName.LOGICAL_TYPE
                  || elemType.getTypeName().isCollectionType()
                  || elemType.getTypeName() == TypeName.MAP) {
                fullBuilder = true;
              } else {
                outputArrayElemNonNull[i] = !elemType.getNullable();
              }
              outputCoercions[i] = OUT_ARRAY;
              break;
            case ITERABLE:
            case LOGICAL_TYPE:
            case MAP:
            default:
              // Delegate validation/coercion of logical types, iterables, and maps to Row.Builder.
              fullBuilder = true;
              outputCoercions[i] = OUT_DIRECT;
              break;
          }
        }
        this.hasReuse = anyReuse;
        this.useFullRowBuilder = fullBuilder;
      }

      private static FieldExtractor buildExtractor(Schema inputSchema, FieldAggregation agg) {
        if (agg.fieldsToAggregate.referencesSingleField()) {
          if (!agg.needsFlattening) {
            int @Nullable [] directPath = tryResolveDirectPath(inputSchema, agg.fieldsToAggregate);
            if (directPath != null) {
              LogicalType<Object, Object> @Nullable [] unwrapChain = null;
              if (agg.aggregateBaseValues) {
                FieldType leafType = agg.flattenedInputSubSchema.getField(0).getType();
                unwrapChain = buildLogicalUnwrapChain(leafType);
              }
              if (directPath.length == 1 && unwrapChain == null) {
                return new DirectTopLevelExtractor(directPath[0]);
              }
              return new DirectPathExtractor(directPath, unwrapChain);
            }
          }
          return new SelectorSingleExtractor(inputSchema, agg);
        }
        return new SelectorMultiExtractor(inputSchema, agg);
      }
    }

    private interface FieldExtractor {
      @Nullable Object extract(Row row);
    }

    private static final class DirectTopLevelExtractor implements FieldExtractor {
      private final int fieldIdx;

      DirectTopLevelExtractor(int fieldIdx) {
        this.fieldIdx = fieldIdx;
      }

      @Override
      public @Nullable Object extract(Row row) {
        return row.getValue(fieldIdx);
      }
    }

    private static final class DirectPathExtractor implements FieldExtractor {
      private final int[] path;
      private final LogicalType<Object, Object> @Nullable [] unwrapChain;

      DirectPathExtractor(int[] path, LogicalType<Object, Object> @Nullable [] unwrapChain) {
        this.path = path;
        this.unwrapChain = unwrapChain;
      }

      @Override
      public @Nullable Object extract(Row row) {
        Row cur = row;
        int last = path.length - 1;
        for (int i = 0; i < last; i++) {
          cur = cur.getRow(path[i]);
          if (cur == null) {
            return null;
          }
        }
        Object val = cur.getValue(path[last]);
        return applyUnwrapChain(val, unwrapChain);
      }
    }

    private static final class SelectorSingleExtractor implements FieldExtractor {
      private final RowSelector rowSelector;
      private final @Nullable RowSelector flatteningSelector;
      private final LogicalType<Object, Object> @Nullable [] unwrapChain;

      SelectorSingleExtractor(Schema inputSchema, FieldAggregation agg) {
        this.rowSelector =
            SelectHelpers.getRowSelectorOptimized(inputSchema, agg.fieldsToAggregate);
        this.flatteningSelector =
            agg.needsFlattening
                ? SelectHelpers.getRowSelectorOptimized(
                    agg.inputSubSchema, agg.flattenedFieldAccessDescriptor)
                : null;
        this.unwrapChain =
            agg.aggregateBaseValues
                ? buildLogicalUnwrapChain(agg.flattenedInputSubSchema.getField(0).getType())
                : null;
      }

      @Override
      public @Nullable Object extract(Row row) {
        Row selected = rowSelector.select(row);
        if (flatteningSelector != null) {
          selected = flatteningSelector.select(selected);
        }
        return applyUnwrapChain(selected.getValue(0), unwrapChain);
      }
    }

    private static final class SelectorMultiExtractor implements FieldExtractor {
      private final RowSelector rowSelector;

      SelectorMultiExtractor(Schema inputSchema, FieldAggregation agg) {
        this.rowSelector =
            SelectHelpers.getRowSelectorOptimized(inputSchema, agg.fieldsToAggregate);
      }

      @Override
      public Row extract(Row row) {
        return rowSelector.select(row);
      }
    }

    private ExecutionPlan getPlan() {
      ExecutionPlan p = this.plan;
      if (p == null) {
        p = new ExecutionPlan(getInputSchema(), getOutputSchema(), getFieldAggregations());
        this.plan = p;
      }
      return p;
    }

    // Represents an aggregation of one or more fields.
    static class FieldAggregation<FieldT, AccumT, OutputT> implements Serializable {
      FieldAccessDescriptor fieldsToAggregate;
      private final boolean aggregateBaseValues;
      // The specification of the output field.
      private final Field outputField;
      // The combine function.
      private final CombineFn<FieldT, AccumT, OutputT> fn;
      // The TupleTag identifying this aggregation element in the composed combine fn.
      private final TupleTag<Object> combineTag;
      // The schema corresponding to the subset of input fields being aggregated.
      private final @Nullable Schema inputSubSchema;
      private final @Nullable FieldAccessDescriptor flattenedFieldAccessDescriptor;
      // The flattened version of inputSubSchema.
      private final @Nullable Schema flattenedInputSubSchema;
      // The output schema resulting from the aggregation.
      private final Schema aggregationSchema;
      private final boolean needsFlattening;

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          boolean aggregateBaseValues,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag) {
        this(
            fieldsToAggregate,
            aggregateBaseValues,
            outputField,
            fn,
            combineTag,
            Schema.builder().addField(outputField).build(),
            null);
      }

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          boolean aggregateBaseValues,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag,
          Schema aggregationSchema,
          @Nullable Schema inputSchema) {
        this.aggregateBaseValues = aggregateBaseValues;
        if (inputSchema != null) {
          this.fieldsToAggregate = fieldsToAggregate.resolve(inputSchema);
          if (aggregateBaseValues) {
            Preconditions.checkArgument(fieldsToAggregate.referencesSingleField());
          }
          this.inputSubSchema = SelectHelpers.getOutputSchema(inputSchema, this.fieldsToAggregate);
          this.flattenedFieldAccessDescriptor =
              SelectHelpers.allLeavesDescriptor(inputSubSchema, SelectHelpers.CONCAT_FIELD_NAMES);
          this.flattenedInputSubSchema =
              SelectHelpers.getOutputSchema(inputSubSchema, flattenedFieldAccessDescriptor);
          this.needsFlattening = !this.inputSubSchema.equals(flattenedInputSubSchema);
        } else {
          this.fieldsToAggregate = fieldsToAggregate;
          this.inputSubSchema = null;
          this.flattenedFieldAccessDescriptor = null;
          this.flattenedInputSubSchema = null;
          this.needsFlattening = false;
        }
        this.outputField = outputField;
        this.fn = fn;
        this.combineTag = combineTag;
        this.aggregationSchema = aggregationSchema;
      }

      // The Schema is not necessarily known when the SchemaAggregateFn is created. Once the schema
      // is known, resolve will be called with the proper schema.
      FieldAggregation<FieldT, AccumT, OutputT> resolve(Schema schema) {
        return new FieldAggregation<>(
            fieldsToAggregate,
            aggregateBaseValues,
            outputField,
            fn,
            combineTag,
            aggregationSchema,
            schema);
      }
    }

    abstract Builder toBuilder();

    @AutoValue.Builder
    @AutoValue.CopyAnnotations
    @SuppressWarnings("rawtypes")
    abstract static class Builder {
      abstract Builder setInputSchema(@Nullable Schema inputSchema);

      abstract Builder setOutputSchema(@Nullable Schema outputSchema);

      abstract Builder setComposedCombineFn(@Nullable ComposedCombineFn composedCombineFn);

      abstract Builder setFieldAggregations(List<FieldAggregation> fieldAggregations);

      abstract Inner build();
    }

    abstract @Nullable Schema getInputSchema();

    abstract @Nullable Schema getOutputSchema();

    abstract @Nullable ComposedCombineFn getComposedCombineFn();

    abstract List<FieldAggregation> getFieldAggregations();

    /** Once the schema is known, this function is called by the {@link Group} transform. */
    Inner withSchema(Schema inputSchema) {
      List<FieldAggregation> fieldAggregations =
          getFieldAggregations().stream()
              .map(f -> f.resolve(inputSchema))
              .collect(Collectors.toList());

      ComposedCombineFn composedCombineFn = null;
      // ComposedCombineFn is retained solely to provide wire-compatible KeyedAccumulatorCoder;
      // runtime extraction executes through ExecutionPlan.
      SimpleFunction<Row, Object> coderPlaceholderFn = CoderPlaceholderExtractFn.INSTANCE;
      for (int i = 0; i < fieldAggregations.size(); ++i) {
        FieldAggregation fieldAggregation = fieldAggregations.get(i);
        Coder extractOutputCoder;
        if (fieldAggregation.fieldsToAggregate.referencesSingleField()) {
          FieldType fieldType = fieldAggregation.flattenedInputSubSchema.getField(0).getType();
          if (fieldAggregation.aggregateBaseValues) {
            while (fieldType.getTypeName().isLogicalType()) {
              fieldType = fieldType.getLogicalType().getBaseType();
            }
          }
          extractOutputCoder = SchemaCoder.coderForFieldType(fieldType);
        } else {
          extractOutputCoder = SchemaCoder.of(fieldAggregation.inputSubSchema);
        }
        if (i == 0) {
          composedCombineFn =
              CombineFns.compose()
                  .with(
                      coderPlaceholderFn,
                      extractOutputCoder,
                      fieldAggregation.fn,
                      fieldAggregation.combineTag);
        } else {
          composedCombineFn =
              ((ComposedCombineFn) composedCombineFn)
                  .with(
                      coderPlaceholderFn,
                      extractOutputCoder,
                      fieldAggregation.fn,
                      fieldAggregation.combineTag);
        }
      }

      return toBuilder()
          .setInputSchema(inputSchema)
          .setComposedCombineFn(composedCombineFn)
          .setFieldAggregations(fieldAggregations)
          .build();
    }

    /** Aggregate all values of a set of fields into an output field. */
    <CombineInputT, AccumT, CombineOutputT> Inner aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        boolean aggregateBaseValues,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          fieldsToAggregate,
          aggregateBaseValues,
          fn,
          Field.of(outputFieldName, FieldTypeDescriptors.fieldTypeForJavaType(fn.getOutputType())));
    }

    /** Aggregate all values of a set of fields into an output field. */
    <CombineInputT, AccumT, CombineOutputT> Inner aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        boolean aggregateBaseValues,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      // Copy before mutating to keep AutoValue instances immutable when reused by callers.
      List<FieldAggregation> fieldAggregations = new ArrayList<>(getFieldAggregations());
      TupleTag<Object> combineTag = new TupleTag<>(Integer.toString(fieldAggregations.size()));
      FieldAggregation fieldAggregation =
          new FieldAggregation<>(
              fieldsToAggregate, aggregateBaseValues, outputField, fn, combineTag);
      fieldAggregations.add(fieldAggregation);

      return toBuilder()
          .setOutputSchema(getOutputSchema(fieldAggregations))
          .setFieldAggregations(fieldAggregations)
          .build();
    }

    private Schema getOutputSchema(List<FieldAggregation> fieldAggregations) {
      Schema.Builder outputSchema = Schema.builder();
      for (FieldAggregation aggregation : fieldAggregations) {
        outputSchema.addField(aggregation.outputField);
      }
      return outputSchema.build();
    }

    /** Placeholder extract function used solely when constructing ComposedCombineFn's coder. */
    private static final class CoderPlaceholderExtractFn extends SimpleFunction<Row, Object> {
      static final CoderPlaceholderExtractFn INSTANCE = new CoderPlaceholderExtractFn();

      @Override
      public Object apply(Row input) {
        throw new UnsupportedOperationException("Executed via SchemaAggregateFn.ExecutionPlan");
      }
    }

    @Override
    public Object[] createAccumulator() {
      ExecutionPlan p = getPlan();
      Object[] acc = new Object[p.count];
      CombineFn<Object, Object, Object>[] fns = p.combineFns;
      for (int i = 0; i < fns.length; i++) {
        acc[i] = fns[i].createAccumulator();
      }
      return acc;
    }

    @Override
    public Object[] addInput(Object[] accumulator, Row input) {
      ExecutionPlan p = getPlan();
      CombineFn<Object, Object, Object>[] fns = p.combineFns;
      FieldExtractor[] exts = p.extractors;
      int n = p.count;
      if (!p.hasReuse) {
        for (int i = 0; i < n; i++) {
          accumulator[i] = fns[i].addInput(accumulator[i], exts[i].extract(input));
        }
      } else {
        Object[] extracted = new Object[n];
        int[] reuse = p.reuseSlot;
        for (int i = 0; i < n; i++) {
          int slot = reuse[i];
          Object val;
          if (slot >= 0) {
            val = extracted[slot];
          } else {
            val = exts[i].extract(input);
            extracted[i] = val;
          }
          accumulator[i] = fns[i].addInput(accumulator[i], val);
        }
      }
      return accumulator;
    }

    @Override
    public Object[] compact(Object[] accumulator) {
      ExecutionPlan p = getPlan();
      CombineFn<Object, Object, Object>[] fns = p.combineFns;
      for (int i = 0; i < fns.length; i++) {
        accumulator[i] = fns[i].compact(accumulator[i]);
      }
      return accumulator;
    }

    @Override
    public Object[] mergeAccumulators(Iterable<Object[]> accumulators) {
      ExecutionPlan p = getPlan();
      CombineFn<Object, Object, Object>[] fns = p.combineFns;
      Iterator<Object[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }
      Object[] merged = new Object[fns.length];
      for (int i = 0; i < fns.length; i++) {
        merged[i] = fns[i].mergeAccumulators(new SlotIterable(accumulators, i));
      }
      return merged;
    }

    private static final class SlotIterable implements Iterable<Object> {
      private final Iterable<Object[]> source;
      private final int index;

      SlotIterable(Iterable<Object[]> source, int index) {
        this.source = source;
        this.index = index;
      }

      @Override
      public Iterator<Object> iterator() {
        final Iterator<Object[]> it = source.iterator();
        final int idx = index;
        return new Iterator<Object>() {
          @Override
          public boolean hasNext() {
            return it.hasNext();
          }

          @Override
          public Object next() {
            return it.next()[idx];
          }
        };
      }
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder)
        throws CannotProvideCoderException {
      return getComposedCombineFn().getAccumulatorCoder(registry, inputCoder);
    }

    @Override
    public Coder<Row> getDefaultOutputCoder(CoderRegistry registry, Coder<Row> inputCoder) {
      return SchemaCoder.of(getOutputSchema());
    }

    @Override
    public Row extractOutput(Object[] accumulator) {
      ExecutionPlan p = getPlan();
      CombineFn<Object, Object, Object>[] fns = p.combineFns;
      int n = p.count;
      if (p.useFullRowBuilder) {
        Row.Builder output = Row.withSchema(p.outputSchema);
        for (int i = 0; i < n; i++) {
          output.addValue(fns[i].extractOutput(accumulator[i]));
        }
        return output.build();
      }
      Object[] outValues = new Object[n];
      byte[] coercions = p.outputCoercions;
      boolean[] nullable = p.outputNullable;
      boolean[] arrayElemNonNull = p.outputArrayElemNonNull;
      for (int i = 0; i < n; i++) {
        Object val = fns[i].extractOutput(accumulator[i]);
        if (val == null) {
          if (!nullable[i]) {
            throw new IllegalArgumentException(
                String.format("Field %s is not nullable", p.outputSchema.getField(i).getName()));
          }
          continue;
        }
        byte mode = coercions[i];
        if (mode == OUT_ARRAY) {
          List<?> list =
              val instanceof List ? (List<?>) val : Lists.newArrayList((Iterable<?>) val);
          if (arrayElemNonNull[i]) {
            for (int j = 0, m = list.size(); j < m; j++) {
              if (list.get(j) == null) {
                throw new IllegalArgumentException(
                    String.format(
                        "Array field %s does not allow null elements",
                        p.outputSchema.getField(i).getName()));
              }
            }
          }
          val = list;
        } else if (mode == OUT_DATETIME && !(val instanceof Instant)) {
          val = ((ReadableInstant) val).toInstant();
        } else if (mode == OUT_BYTES && val instanceof ByteBuffer) {
          ByteBuffer bb = ((ByteBuffer) val).slice();
          byte[] bytes = new byte[bb.remaining()];
          bb.get(bytes);
          val = bytes;
        }
        outValues[i] = val;
      }
      return Row.attachValues(p.outputSchema, outValues);
    }
  }
}
