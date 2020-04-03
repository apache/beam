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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.commons.compress.utils.Lists;

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
@Experimental(Kind.SCHEMAS)
public class RenameFields {
  /** Create an instance of this transform. */
  public static <T> Inner<T> create() {
    return new Inner<>();
  }

  // Describes a single renameSchema rule.
  private static class RenamePair implements Serializable {
    // The FieldAccessDescriptor describing the field to renameSchema. Must reference a singleton
    // field.
    private final FieldAccessDescriptor fieldAccessDescriptor;
    // The new name for the field.
    private final String newName;

    RenamePair(FieldAccessDescriptor fieldAccessDescriptor, String newName) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.newName = newName;
    }

    RenamePair resolve(Schema schema) {
      FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);
      if (!resolved.referencesSingleField()) {
        throw new IllegalArgumentException(resolved + " references multiple fields.");
      }
      return new RenamePair(resolved, newName);
    }
  }

  private static FieldType renameFieldType(FieldType inputType, Collection<RenamePair> renames) {
    switch (inputType.getTypeName()) {
      case ROW:
        return FieldType.row(renameSchema(inputType.getRowSchema(), renames));
      case ARRAY:
        return FieldType.array(renameFieldType(inputType.getCollectionElementType(), renames));
      case ITERABLE:
        return FieldType.iterable(renameFieldType(inputType.getCollectionElementType(), renames));
      case MAP:
        return FieldType.map(
            renameFieldType(inputType.getMapKeyType(), renames),
            renameFieldType(inputType.getMapValueType(), renames));
      default:
        return inputType;
    }
  }

  // Apply the user-specified renames to the input schema.
  private static Schema renameSchema(Schema inputSchema, Collection<RenamePair> renames) {
    // The mapping of renames to apply at this level of the schema.
    Map<Integer, String> topLevelRenames = Maps.newHashMap();
    // For nested schemas, collect all applicable renames here.
    Multimap<Integer, RenamePair> nestedRenames = ArrayListMultimap.create();

    for (RenamePair rename : renames) {
      FieldAccessDescriptor access = rename.fieldAccessDescriptor;
      if (!access.fieldIdsAccessed().isEmpty()) {
        // This references a field at this level of the schema.
        Integer fieldId = Iterables.getOnlyElement(access.fieldIdsAccessed());
        topLevelRenames.put(fieldId, rename.newName);
      } else {
        // This references a nested field.
        Map.Entry<Integer, FieldAccessDescriptor> nestedAccess =
            Iterables.getOnlyElement(access.nestedFieldsById().entrySet());
        nestedRenames.put(
            nestedAccess.getKey(), new RenamePair(nestedAccess.getValue(), rename.newName));
      }
    }

    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
      Field field = inputSchema.getField(i);
      FieldType fieldType = field.getType();
      String newName = topLevelRenames.getOrDefault(i, field.getName());
      Collection<RenamePair> nestedFieldRenames = nestedRenames.asMap().get(i);
      if (nestedFieldRenames != null) {
        // There are nested field renames. Recursively renameSchema the rest of the schema.
        builder.addField(newName, renameFieldType(fieldType, nestedFieldRenames));
      } else {
        // No renameSchema for this field. Just add it back as is, potentially with a new name.
        builder.addField(newName, fieldType);
      }
    }
    return builder.build();
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
              .add(new RenamePair(field, newName))
              .build();

      return new Inner<>(newList);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();

      List<RenamePair> pairs =
          renames.stream().map(r -> r.resolve(inputSchema)).collect(Collectors.toList());
      final Schema outputSchema = renameSchema(inputSchema, pairs);
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> o) {
                      o.output(Row.withSchema(outputSchema).attachValues(row.getValues()));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}
