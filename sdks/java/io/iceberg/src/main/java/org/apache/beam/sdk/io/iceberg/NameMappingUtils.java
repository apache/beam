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
package org.apache.beam.sdk.io.iceberg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ascii;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for the {@code schema.name-mapping.default} table property, which zero-copy registered
 * files (no embedded field ids) depend on for column resolution.
 */
class NameMappingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NameMappingUtils.class);
  private static final int MAX_LOGGED_MAPPING_CHARS = 4000;

  private NameMappingUtils() {}

  /** A damaged property must self-heal via regeneration, never poison the stage reading it. */
  static @Nullable NameMapping parseOrNull(@Nullable String mappingJson) {
    if (mappingJson == null) {
      return null;
    }
    try {
      return NameMappingParser.fromJson(mappingJson);
    } catch (RuntimeException e) {
      // Regeneration overwrites the property, so this is the only record of the bad value.
      LOG.warn(
          "Malformed {} property; it will be regenerated from the schema. Error: {}. Value: {}",
          TableProperties.DEFAULT_NAME_MAPPING,
          AddFiles.errorMessage(e),
          Ascii.truncate(mappingJson, MAX_LOGGED_MAPPING_CHARS, "..."));
      return null;
    }
  }

  /**
   * Whether the mapping resolves every schema column to its own field id. A null-id entry (reads
   * resolve the column to nothing) or a wrong-id entry (reads bind it to the wrong field) counts as
   * not covered. List and map contents are addressed via the synthetic {@code element}/{@code
   * key}/{@code value} path components, matching how {@code MappingUtil.create} names them.
   */
  static boolean covers(NameMapping mapping, Types.StructType struct) {
    return coversStruct(mapping, struct, new ArrayList<>());
  }

  private static boolean coversStruct(
      NameMapping mapping, Types.StructType struct, List<String> path) {
    for (Types.NestedField field : struct.fields()) {
      if (!coversField(mapping, field.name(), field.fieldId(), field.type(), path)) {
        return false;
      }
    }
    return true;
  }

  private static boolean coversField(
      NameMapping mapping, String name, int expectedId, Type type, List<String> path) {
    path.add(name);
    try {
      @Nullable MappedField found = mapping.find(path);
      if (found == null) {
        return false;
      }
      @Nullable Integer foundId = found.id();
      if (foundId == null || foundId != expectedId) {
        return false;
      }
      if (type.isStructType()) {
        return coversStruct(mapping, type.asStructType(), path);
      } else if (type.isListType()) {
        Types.ListType list = type.asListType();
        return coversField(mapping, "element", list.elementId(), list.elementType(), path);
      } else if (type.isMapType()) {
        Types.MapType map = type.asMapType();
        if (!coversField(mapping, "key", map.keyId(), map.keyType(), path)) {
          return false;
        }
        return coversField(mapping, "value", map.valueId(), map.valueType(), path);
      }
      return true;
    } finally {
      path.remove(path.size() - 1);
    }
  }

  /**
   * Schema-derived mapping, with custom names (user aliases, pre-rename file names) carried over
   * from {@code existing} by field id. Schema names win: a carried name that collides with a name
   * already present at its level is dropped, since Iceberg rejects ambiguous mappings. Entries for
   * ids no longer in the schema (including null-id tombstones) are not carried; their file columns
   * stay unmapped, which readers treat the same way. Merge failures fall back to the plain
   * schema-derived mapping.
   */
  static String regenerate(Schema schema, @Nullable NameMapping existing) {
    NameMapping generated = MappingUtil.create(schema);
    if (existing == null) {
      return NameMappingParser.toJson(generated);
    }
    try {
      Map<Integer, Set<String>> existingNamesById = new HashMap<>();
      indexNamesById(existing.asMappedFields(), existingNamesById);
      MappedFields mergedFields = mergeNames(generated.asMappedFields(), existingNamesById, "");
      return NameMappingParser.toJson(NameMapping.of(mergedFields));
    } catch (RuntimeException e) {
      LOG.warn(
          "Could not carry custom name-mapping entries over; regenerating from the schema: {}",
          AddFiles.errorMessage(e));
      return NameMappingParser.toJson(generated);
    }
  }

  // Duplicate ids cannot survive parsing (NameMapping rejects them); the name-set union is
  // defensive.
  private static void indexNamesById(MappedFields fields, Map<Integer, Set<String>> index) {
    for (MappedField field : fields.fields()) {
      @Nullable Integer id = field.id();
      if (id != null) {
        @Nullable Set<String> names = index.get(id);
        if (names == null) {
          names = new LinkedHashSet<>();
          index.put(id, names);
        }
        names.addAll(field.names());
      }
      @Nullable MappedFields nested = field.nestedMapping();
      if (nested != null) {
        indexNamesById(nested, index);
      }
    }
  }

  private static MappedFields mergeNames(
      MappedFields generated, Map<Integer, Set<String>> existingNamesById, String parentPath) {
    Set<String> schemaNames = new HashSet<>();
    for (MappedField field : generated.fields()) {
      schemaNames.addAll(field.names());
    }
    Set<String> claimed = new HashSet<>(schemaNames);
    List<MappedField> merged = new ArrayList<>();
    for (MappedField field : generated.fields()) {
      Set<String> names = new LinkedHashSet<>(field.names());
      String fieldPath = Iterables.get(field.names(), 0);
      if (!parentPath.isEmpty()) {
        fieldPath = parentPath + "." + fieldPath;
      }

      Set<String> extras = Collections.emptySet();
      @Nullable Integer id = field.id();
      if (id != null) {
        extras = existingNamesById.getOrDefault(id, Collections.emptySet());
      }
      for (String extra : extras) {
        if (names.contains(extra)) {
          continue;
        }
        if (!claimed.contains(extra)) {
          names.add(extra);
          claimed.add(extra);
          continue;
        }
        String reason;
        if (schemaNames.contains(extra)) {
          reason = "a schema column at the same level has this name";
        } else {
          reason = "it was already carried over for another field at this level";
        }
        LOG.warn("Dropping custom name '{}' of '{}': {}.", extra, fieldPath, reason);
      }
      List<String> mergedNames = new ArrayList<>(names);
      @Nullable MappedFields generatedNested = field.nestedMapping();
      if (generatedNested == null) {
        merged.add(MappedField.of(field.id(), mergedNames));
      } else {
        MappedFields mergedNested = mergeNames(generatedNested, existingNamesById, fieldPath);
        merged.add(MappedField.of(field.id(), mergedNames, mergedNested));
      }
    }
    return MappedFields.of(merged);
  }
}
