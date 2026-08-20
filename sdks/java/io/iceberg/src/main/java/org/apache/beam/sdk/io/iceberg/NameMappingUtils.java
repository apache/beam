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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private NameMappingUtils() {}

  /** A damaged property must self-heal via regeneration, never poison the stage reading it. */
  static @Nullable NameMapping parseOrNull(@Nullable String mappingJson) {
    if (mappingJson == null) {
      return null;
    }
    try {
      return NameMappingParser.fromJson(mappingJson);
    } catch (RuntimeException e) {
      LOG.warn(
          "Malformed {} property; it will be regenerated from the schema: {}",
          TableProperties.DEFAULT_NAME_MAPPING,
          AddFiles.errorMessage(e));
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
      @Nullable Integer foundId = found == null ? null : found.id();
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
        return coversField(mapping, "key", map.keyId(), map.keyType(), path)
            && coversField(mapping, "value", map.valueId(), map.valueType(), path);
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
      return NameMappingParser.toJson(
          NameMapping.of(mergeNames(generated.asMappedFields(), existingNamesById, "")));
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
      if (field.id() != null) {
        index.computeIfAbsent(field.id(), unused -> new LinkedHashSet<>()).addAll(field.names());
      }
      if (field.nestedMapping() != null) {
        indexNamesById(field.nestedMapping(), index);
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
      String fieldPath =
          parentPath.isEmpty()
              ? names.iterator().next()
              : parentPath + "." + names.iterator().next();
      @Nullable Set<String> extras = field.id() == null ? null : existingNamesById.get(field.id());
      if (extras != null) {
        for (String extra : extras) {
          if (names.contains(extra)) {
            continue;
          }
          if (claimed.contains(extra)) {
            LOG.warn(
                "Dropping custom name '{}' of '{}': {}.",
                extra,
                fieldPath,
                schemaNames.contains(extra)
                    ? "a schema column at the same level has this name"
                    : "it was already carried over for another field at this level");
          } else {
            names.add(extra);
            claimed.add(extra);
          }
        }
      }
      @Nullable MappedFields generatedNested = field.nestedMapping();
      if (generatedNested == null) {
        merged.add(MappedField.of(field.id(), new ArrayList<>(names)));
      } else {
        merged.add(
            MappedField.of(
                field.id(),
                new ArrayList<>(names),
                mergeNames(generatedNested, existingNamesById, fieldPath)));
      }
    }
    return MappedFields.of(merged);
  }
}
