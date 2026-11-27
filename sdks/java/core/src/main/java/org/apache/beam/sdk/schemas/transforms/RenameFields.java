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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A transform for renaming fields inside an existing schema. Top level or nested fields can be
 * renamed. When renaming a nested field, the nested prefix does not need to be specified again when
 * specifying the new name.
 *
 * <p>Example use:
 *
 * <pre>{@code PCollection<Event> events = readEvents();
 * PCollection<Row> renamedEvents =
 *   events.apply(RenameFields.<Event>create()
 *       .rename("userName", "userId")
 *       .rename("location.country", "countryCode"));
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RenameFields {
  /** Create an instance of this transform. */
  public static <T> Inner<T> create() {
    return new Inner<>();
  }

  // Describes a single renameSchema rule
  @AutoValue
  abstract static class RenamePair implements Serializable {
    // The FieldAccessDescriptor describing the field to renameSchema. Must reference a singleton
    // field.
    abstract FieldAccessDescriptor getFieldAccessDescriptor();

    // The new name for the field.
    abstract String getNewName();

    static RenamePair of(FieldAccessDescriptor fieldAccessDescriptor, String newName) {
      return new AutoValue_RenameFields_RenamePair(fieldAccessDescriptor, newName);
    }

    RenamePair resolve(Schema schema) {
      FieldAccessDescriptor resolved = getFieldAccessDescriptor().resolve(schema);
      if (!resolved.referencesSingleField()) {
        throw new IllegalArgumentException(resolved + " references multiple fields.");
      }
      return RenamePair.of(resolved, getNewName());
    }
  }

  /** Precompiled per-field rename plan enabling fast, UUID-free multi-level renaming. */
  private interface FieldRenamePlan extends Serializable {
    @Nullable Object apply(@Nullable Object value);
  }

  @VisibleForTesting
  static final class RowRenamePlan implements FieldRenamePlan {
    final Schema outputSchema;
    final @Nullable FieldRenamePlan @Nullable [] fieldPlans;

    RowRenamePlan(Schema outputSchema, @Nullable FieldRenamePlan @Nullable [] fieldPlans) {
      this.outputSchema = outputSchema;
      this.fieldPlans = fieldPlans;
    }

    @Override
    public @Nullable Object apply(@Nullable Object value) {
      return value == null ? null : rename((Row) value);
    }

    Row rename(Row row) {
      if (row == null) {
        return null;
      }
      if (fieldPlans == null) {
        if (row instanceof RowWithStorage) {
          @Nullable Object[] raw = ((RowWithStorage) row).getRawArray();
          if (raw != null) {
            return Row.attachValues(outputSchema, raw);
          }
          return Row.attachValues(outputSchema, row.getValues());
        }
        int n = outputSchema.getFieldCount();
        Object[] values = new Object[n];
        for (int i = 0; i < n; i++) {
          values[i] = row.getValue(i);
        }
        return Row.attachValues(outputSchema, values);
      }
      int n = outputSchema.getFieldCount();
      Object[] values = new Object[n];
      @Nullable Object[] raw =
          row instanceof RowWithStorage ? ((RowWithStorage) row).getRawArray() : null;
      for (int i = 0; i < n; i++) {
        Object val = raw != null ? raw[i] : row.getValue(i);
        FieldRenamePlan childPlan = fieldPlans[i];
        values[i] = (childPlan != null && val != null) ? childPlan.apply(val) : val;
      }
      return Row.attachValues(outputSchema, values);
    }
  }

  private static final class CollectionRenamePlan implements FieldRenamePlan {
    final FieldRenamePlan elementPlan;

    CollectionRenamePlan(FieldRenamePlan elementPlan) {
      this.elementPlan = elementPlan;
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable Object apply(@Nullable Object value) {
      if (value == null) {
        return null;
      }
      Iterable<Object> iterable = (Iterable<Object>) value;
      List<Object> renamed =
          iterable instanceof Collection
              ? new ArrayList<>(((Collection<?>) iterable).size())
              : new ArrayList<>();
      for (Object elem : iterable) {
        renamed.add(elem == null ? null : elementPlan.apply(elem));
      }
      return renamed;
    }
  }

  private static final class MapRenamePlan implements FieldRenamePlan {
    final @Nullable FieldRenamePlan keyPlan;
    final @Nullable FieldRenamePlan valuePlan;

    MapRenamePlan(@Nullable FieldRenamePlan keyPlan, @Nullable FieldRenamePlan valuePlan) {
      this.keyPlan = keyPlan;
      this.valuePlan = valuePlan;
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable Object apply(@Nullable Object value) {
      if (value == null) {
        return null;
      }
      Map<Object, Object> inputMap = (Map<Object, Object>) value;
      Map<Object, Object> out = Maps.newHashMapWithExpectedSize(inputMap.size());
      for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
        Object k = entry.getKey();
        Object v = entry.getValue();
        Object outK = (keyPlan != null && k != null) ? keyPlan.apply(k) : k;
        Object outV = (valuePlan != null && v != null) ? valuePlan.apply(v) : v;
        out.put(outK, outV);
      }
      return out;
    }
  }

  private static final class FieldPlanResult {
    final FieldType fieldType;
    final @Nullable FieldRenamePlan plan;

    FieldPlanResult(FieldType fieldType, @Nullable FieldRenamePlan plan) {
      this.fieldType = fieldType;
      this.plan = plan;
    }
  }

  @VisibleForTesting
  static RowRenamePlan buildRenamePlan(Schema inputSchema, Collection<RenamePair> renames) {
    Map<Integer, String> topLevelRenames = Maps.newHashMap();
    Multimap<Integer, RenamePair> nestedRenames = ArrayListMultimap.create();

    for (RenamePair rename : renames) {
      FieldAccessDescriptor access = rename.getFieldAccessDescriptor();
      if (!access.fieldIdsAccessed().isEmpty()) {
        Integer fieldId = Iterables.getOnlyElement(access.fieldIdsAccessed());
        topLevelRenames.put(fieldId, rename.getNewName());
      } else {
        Map.Entry<Integer, FieldAccessDescriptor> nestedAccess =
            Iterables.getOnlyElement(access.nestedFieldsById().entrySet());
        nestedRenames.put(
            nestedAccess.getKey(), RenamePair.of(nestedAccess.getValue(), rename.getNewName()));
      }
    }

    int fieldCount = inputSchema.getFieldCount();
    @Nullable FieldRenamePlan[] childPlans = null;
    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < fieldCount; i++) {
      Field field = inputSchema.getField(i);
      String newName = topLevelRenames.getOrDefault(i, field.getName());
      Collection<RenamePair> nested =
          nestedRenames.asMap().getOrDefault(i, Collections.emptyList());
      if (nested.isEmpty()) {
        builder.addField(field.withName(newName));
      } else {
        FieldPlanResult res = buildFieldPlan(field.getType(), nested);
        builder.addField(field.withName(newName).withType(res.fieldType));
        if (res.plan != null) {
          if (childPlans == null) {
            childPlans = new FieldRenamePlan[fieldCount];
          }
          childPlans[i] = res.plan;
        }
      }
    }
    return new RowRenamePlan(builder.build(), childPlans);
  }

  private static FieldPlanResult buildFieldPlan(
      FieldType inputType, Collection<RenamePair> renames) {
    if (renames.isEmpty()) {
      return new FieldPlanResult(inputType, null);
    }
    switch (inputType.getTypeName()) {
      case ROW:
        RowRenamePlan child = buildRenamePlan(inputType.getRowSchema(), renames);
        return new FieldPlanResult(
            FieldType.row(child.outputSchema)
                .withNullable(inputType.getNullable())
                .withMetadata(inputType.getAllMetadata()),
            child);
      case ARRAY:
        FieldPlanResult arrElem = buildFieldPlan(inputType.getCollectionElementType(), renames);
        return new FieldPlanResult(
            FieldType.array(arrElem.fieldType)
                .withNullable(inputType.getNullable())
                .withMetadata(inputType.getAllMetadata()),
            arrElem.plan == null ? null : new CollectionRenamePlan(arrElem.plan));
      case ITERABLE:
        FieldPlanResult iterElem = buildFieldPlan(inputType.getCollectionElementType(), renames);
        return new FieldPlanResult(
            FieldType.iterable(iterElem.fieldType)
                .withNullable(inputType.getNullable())
                .withMetadata(inputType.getAllMetadata()),
            iterElem.plan == null ? null : new CollectionRenamePlan(iterElem.plan));
      case MAP:
        // FieldAccessDescriptor.resolve resolves nested map renames against the map value schema
        // only; do not apply value-resolved field IDs to composite map keys.
        FieldPlanResult valRes = buildFieldPlan(inputType.getMapValueType(), renames);
        return new FieldPlanResult(
            FieldType.map(inputType.getMapKeyType(), valRes.fieldType)
                .withNullable(inputType.getNullable())
                .withMetadata(inputType.getAllMetadata()),
            valRes.plan == null ? null : new MapRenamePlan(null, valRes.plan));
      case LOGICAL_TYPE:
        throw new RuntimeException("RenameFields does not support renaming logical types.");
      default:
        return new FieldPlanResult(inputType, null);
    }
  }

  /** The class implementing the actual PTransform. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private List<RenamePair> renames;

    private Inner() {
      renames = Lists.newArrayList();
    }

    private Inner(List<RenamePair> renames) {
      this.renames = renames;
    }

    /** Rename a specific field. */
    public Inner<T> rename(String field, String newName) {
      return rename(FieldAccessDescriptor.withFieldNames(field), newName);
    }

    /** Rename a specific field. */
    public Inner<T> rename(FieldAccessDescriptor field, String newName) {
      List<RenamePair> newList =
          ImmutableList.<RenamePair>builder()
              .addAll(renames)
              .add(RenamePair.of(field, newName))
              .build();

      return new Inner<>(newList);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      @Nullable SerializableFunction<T, Row> toRowFn =
          SchemaUtils.isIdentityFunction(input.getToRowFunction())
              ? null
              : input.getToRowFunction();
      if (renames.isEmpty() && toRowFn == null) {
        @SuppressWarnings("unchecked")
        PCollection<Row> passthrough = (PCollection<Row>) input;
        return passthrough;
      }
      List<RenamePair> resolvedRenames =
          renames.stream().map(r -> r.resolve(inputSchema)).collect(Collectors.toList());
      final RowRenamePlan plan = buildRenamePlan(inputSchema, resolvedRenames);
      final Schema outputSchema = plan.outputSchema;
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element T element, OutputReceiver<Row> o) {
                      Row row = toRowFn != null ? toRowFn.apply(element) : (Row) element;
                      o.output(plan.rename(row));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}
