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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;

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

  private static FieldType renameFieldType(
      FieldType inputType,
      Collection<RenamePair> renames,
      Map<UUID, Schema> renamedSchemasMap,
      Map<UUID, BitSet> nestedFieldRenamedMap) {
    if (renames.isEmpty()) {
      return inputType;
    }

    switch (inputType.getTypeName()) {
      case ROW:
        renameSchema(inputType.getRowSchema(), renames, renamedSchemasMap, nestedFieldRenamedMap);
        return FieldType.row(renamedSchemasMap.get(inputType.getRowSchema().getUUID()));
      case ARRAY:
        return FieldType.array(
            renameFieldType(
                inputType.getCollectionElementType(),
                renames,
                renamedSchemasMap,
                nestedFieldRenamedMap));
      case ITERABLE:
        return FieldType.iterable(
            renameFieldType(
                inputType.getCollectionElementType(),
                renames,
                renamedSchemasMap,
                nestedFieldRenamedMap));
      case MAP:
        return FieldType.map(
            renameFieldType(
                inputType.getMapKeyType(), renames, renamedSchemasMap, nestedFieldRenamedMap),
            renameFieldType(
                inputType.getMapValueType(), renames, renamedSchemasMap, nestedFieldRenamedMap));
      case LOGICAL_TYPE:
        throw new RuntimeException("RenameFields does not support renaming logical types.");
      default:
        return inputType;
    }
  }

  // Apply the user-specified renames to the input schema.
  @VisibleForTesting
  static void renameSchema(
      Schema inputSchema,
      Collection<RenamePair> renames,
      Map<UUID, Schema> renamedSchemasMap,
      Map<UUID, BitSet> nestedFieldRenamedMap) {
    // The mapping of renames to apply at this level of the schema.
    Map<Integer, String> topLevelRenames = Maps.newHashMap();
    // For nested schemas, collect all applicable renames here.
    Multimap<Integer, RenamePair> nestedRenames = ArrayListMultimap.create();

    for (RenamePair rename : renames) {
      FieldAccessDescriptor access = rename.getFieldAccessDescriptor();
      if (!access.fieldIdsAccessed().isEmpty()) {
        // This references a field at this level of the schema.
        Integer fieldId = Iterables.getOnlyElement(access.fieldIdsAccessed());
        topLevelRenames.put(fieldId, rename.getNewName());
      } else {
        // This references a nested field.
        Map.Entry<Integer, FieldAccessDescriptor> nestedAccess =
            Iterables.getOnlyElement(access.nestedFieldsById().entrySet());
        nestedFieldRenamedMap
            .computeIfAbsent(inputSchema.getUUID(), s -> new BitSet(inputSchema.getFieldCount()))
            .set(nestedAccess.getKey());
        nestedRenames.put(
            nestedAccess.getKey(), RenamePair.of(nestedAccess.getValue(), rename.getNewName()));
      }
    }

    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
      Field field = inputSchema.getField(i);
      FieldType fieldType = field.getType();
      String newName = topLevelRenames.getOrDefault(i, field.getName());
      Collection<RenamePair> nestedFieldRenames =
          nestedRenames.asMap().getOrDefault(i, Collections.emptyList());
      builder.addField(
          newName,
          renameFieldType(fieldType, nestedFieldRenames, renamedSchemasMap, nestedFieldRenamedMap));
    }
    renamedSchemasMap.put(inputSchema.getUUID(), builder.build());
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
      final Map<UUID, Schema> renamedSchemasMap = Maps.newHashMap();
      final Map<UUID, BitSet> nestedFieldRenamedMap = Maps.newHashMap();

      List<RenamePair> resolvedRenames =
          renames.stream().map(r -> r.resolve(input.getSchema())).collect(Collectors.toList());
      renameSchema(input.getSchema(), resolvedRenames, renamedSchemasMap, nestedFieldRenamedMap);
      final Schema outputSchema = renamedSchemasMap.get(input.getSchema().getUUID());
      final BitSet nestedRenames = nestedFieldRenamedMap.get(input.getSchema().getUUID());
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> o) {
                      o.output(
                          renameRow(
                              row,
                              outputSchema,
                              nestedRenames,
                              renamedSchemasMap,
                              nestedFieldRenamedMap));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }

  // TODO(reuvenlax): For better performance, we should reuse functionality in
  // SelectByteBuddyHelpers to generate
  // byte code to do the rename. This would allow us to skip walking over the schema on each row.
  // For now we added
  // the optimization to skip schema walking if there are no nested renames (as determined by the
  // nestedFieldRenamedMap).
  @VisibleForTesting
  static Row renameRow(
      Row row,
      Schema schema,
      @Nullable BitSet nestedRenames,
      Map<UUID, Schema> renamedSubSchemasMap,
      Map<UUID, BitSet> nestedFieldRenamedMap) {
    if (nestedRenames == null || nestedRenames.isEmpty()) {
      // Fast path, short circuit subschems.
      return Row.withSchema(schema).attachValues(row.getValues());
    } else {
      List<Object> values = Lists.newArrayListWithCapacity(row.getValues().size());
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        if (nestedRenames.get(i)) {
          values.add(
              renameFieldValue(
                  row.getValue(i),
                  schema.getField(i).getType(),
                  renamedSubSchemasMap,
                  nestedFieldRenamedMap));
        } else {
          values.add(row.getValue(i));
        }
      }
      return Row.withSchema(schema).attachValues(values);
    }
  }

  private static Object renameFieldValue(
      Object value,
      FieldType fieldType,
      Map<UUID, Schema> renamedSubSchemas,
      Map<UUID, BitSet> nestedFieldRenamed) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        List<Object> renamedValues = Lists.newArrayList();
        for (Object o : (List) value) {
          renamedValues.add(
              renameFieldValue(
                  o, fieldType.getCollectionElementType(), renamedSubSchemas, nestedFieldRenamed));
        }
        return renamedValues;
      case MAP:
        Map<Object, Object> renamedMap = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
          renamedMap.put(
              renameFieldValue(
                  entry.getKey(), fieldType.getMapKeyType(), renamedSubSchemas, nestedFieldRenamed),
              renameFieldValue(
                  entry.getValue(),
                  fieldType.getMapValueType(),
                  renamedSubSchemas,
                  nestedFieldRenamed));
        }
        return renamedMap;
      case ROW:
        return renameRow(
            (Row) value,
            fieldType.getRowSchema(),
            nestedFieldRenamed.get(fieldType.getRowSchema().getUUID()),
            renamedSubSchemas,
            nestedFieldRenamed);
      default:
        return value;
    }
  }
}
