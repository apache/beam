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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * What a file contributes to schema inference: the canonical form of the schema it declares, and
 * the columns its footer proves free of nulls.
 *
 * <p>The schema half depends only on the declared schema, never on the data, so files written by
 * the same job dedup to one entry no matter where their nulls fall. The null evidence is combined
 * per schema by {@link CollectDistinctSchemas} and reapplied by the commit side via {@link
 * #markRequired}.
 *
 * <p>The canonical form sorts struct fields by name at every level and renumbers ids. Ids are
 * positional and meaningless (the commit side reconciles columns by name), so never diff two file
 * schemas by id. Other field attributes (doc, defaults) are preserved, matching what SchemaDelta
 * compares. Column paths are dotted, like pins.
 */
final class FileSchemas {
  private FileSchemas() {}

  /** Canonical JSON of the schema the file declares. */
  static String canonicalJson(ParquetMetadata footer) {
    Schema converted = ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema());
    return SchemaParser.toJson(canonical(converted));
  }

  /** This file as a schema group of one: its declared schema and its null-free columns. */
  static CollectDistinctSchemas.SchemaGroup schemaGroup(ParquetMetadata footer) {
    Schema converted = ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema());
    Schema tightened = tighten(converted, footer);
    return CollectDistinctSchemas.SchemaGroup.of(
        SchemaParser.toJson(canonical(converted)), 1, changedToRequired(converted, tightened));
  }

  /**
   * This one file's schema in table terms: convert, then tighten using its own footer. Uses the
   * file's own null evidence, not its group's: a file that proves a column null-free registers even
   * when other files sharing its schema could not prove it.
   */
  static Schema effective(ParquetMetadata footer) {
    Schema converted = ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema());
    return tighten(converted, footer);
  }

  /**
   * The file's own null count for one dotted column of its schema, or null when the file cannot
   * prove it. Pin evidence must come from the file itself, not from the Metrics built for the
   * DataFile: the table's write.metadata.metrics configuration shapes those and must not be able to
   * turn pin enforcement off. A column the schema declares required, with every ancestor required,
   * counts 0 without any statistic: Parquet's repetition is structural, so such a column cannot
   * encode a null. Otherwise the footer's counts are read by the {@link #tighten} rules: a leaf's
   * count is summed over row groups and unknown when any non-empty row group lacks it; a struct
   * counts 0 when a leaf beneath it (struct nesting only) has none, since a null struct nulls every
   * leaf, and is unknown otherwise, since a leaf's nulls include its ancestors' and cannot be
   * attributed; a column under a list or map is unknown; a file with no rows proves every column.
   */
  static @Nullable Long nullCount(ParquetMetadata footer, Schema fileSchema, String column) {
    if (rowCount(footer) == 0) {
      return 0L;
    }
    Types.NestedField field = fileSchema.findField(column);
    if (field == null) {
      return null;
    }
    if (requiredAlongPath(fileSchema, column)) {
      return 0L;
    }
    List<String> path = new ArrayList<>(Arrays.asList(column.split("\\.", -1)));
    Map<List<String>, @Nullable Long> counts = nullCountsByLeaf(footer);
    if (field.type().isPrimitiveType()) {
      return counts.get(path);
    }
    if (field.type().isStructType()) {
      Tightened struct = tightenStruct(field.type().asStructType(), path, zeroNullLeaves(counts));
      return struct.hasNullFreeLeaf ? Long.valueOf(0) : null;
    }
    return null;
  }

  /**
   * Marks a declared-optional column required when every row group has a null count of zero for it,
   * so the file does not request a relaxation it does not need; an absent count is not proof. A
   * struct is null-free when any leaf under it is (a null struct nulls all its leaves). Nothing
   * under lists or maps is tightened: a zero count there would be valid evidence too, but mapping
   * physical chunk paths (writer-dependent names like {@code list.element}, {@code array}) onto the
   * converted schema is not worth it. A file with no rows (no row groups, or only empty ones, as
   * pyarrow writes an empty table) proves every column, matching how the pin check treats empty
   * files.
   */
  static Schema tighten(Schema schema, ParquetMetadata footer) {
    if (rowCount(footer) == 0) {
      return new Schema(tightenAll(schema.asStruct()).fields());
    }
    Set<List<String>> zeroNullLeaves = leafPathsWithZeroNullCounts(footer);
    if (zeroNullLeaves.isEmpty()) {
      return schema;
    }
    return new Schema(
        tightenStruct(schema.asStruct(), new ArrayList<>(), zeroNullLeaves).struct.fields());
  }

  /** With no rows, nothing can hold a null: every leaf and struct outside lists and maps. */
  private static Types.StructType tightenAll(Types.StructType struct) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField field : struct.fields()) {
      Type type = field.type();
      if (type.isStructType()) {
        fields.add(withOptionality(field, tightenAll(type.asStructType()), false));
      } else if (type.isPrimitiveType()) {
        fields.add(withOptionality(field, type, false));
      } else {
        fields.add(field);
      }
    }
    return Types.StructType.of(fields);
  }

  /**
   * Returns the schema with the given dotted column paths made required. The commit side parses a
   * group's schema JSON (optionality as the writer declared it) and applies the group's null-free
   * columns with this before classifying, so only relaxations some file actually needs remain.
   */
  static Schema markRequired(Schema declared, Collection<String> columns) {
    if (columns.isEmpty()) {
      return declared;
    }
    Types.StructType required = markRequiredStruct(declared.asStruct(), "", new HashSet<>(columns));
    return new Schema(required.fields());
  }

  private static Types.StructType markRequiredStruct(
      Types.StructType struct, String prefix, Set<String> columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField field : struct.fields()) {
      String path = prefix + field.name();
      Type type = field.type();
      if (type.isStructType()) {
        type = markRequiredStruct(type.asStructType(), path + ".", columns);
      }
      boolean required = !field.isRequired() && columns.contains(path);
      fields.add(withOptionality(field, type, field.isOptional() && !required));
    }
    return Types.StructType.of(fields);
  }

  /** Dotted paths of fields the tightened schema made required, sorted. */
  private static List<String> changedToRequired(Schema declared, Schema tightened) {
    List<String> paths = new ArrayList<>();
    collectChangedToRequired(declared.asStruct(), tightened.asStruct(), "", paths);
    Collections.sort(paths);
    return paths;
  }

  private static void collectChangedToRequired(
      Types.StructType declared, Types.StructType tightened, String prefix, List<String> out) {
    for (int i = 0; i < declared.fields().size(); i++) {
      Types.NestedField before = declared.fields().get(i);
      Types.NestedField after = tightened.fields().get(i);
      String path = prefix + before.name();
      if (before.isOptional() && after.isRequired()) {
        out.add(path);
      }
      if (before.type().isStructType()) {
        collectChangedToRequired(
            before.type().asStructType(), after.type().asStructType(), path + ".", out);
      }
    }
  }

  private static long rowCount(ParquetMetadata footer) {
    long rows = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      rows += block.getRowCount();
    }
    return rows;
  }

  /** Whether the column and each of its ancestors are required: no null can be encoded there. */
  private static boolean requiredAlongPath(Schema schema, String column) {
    Types.NestedField field = schema.findField(column);
    if (field == null) {
      return false;
    }
    Map<Integer, Integer> parents = TypeUtil.indexParents(schema.asStruct());
    for (@Nullable Integer id = field.fieldId(); id != null; id = parents.get(id)) {
      if (schema.findField(id).isOptional()) {
        return false;
      }
    }
    return true;
  }

  /** Leaf paths proven null-free: the one source of null evidence for tighten and for pins. */
  private static Set<List<String>> leafPathsWithZeroNullCounts(ParquetMetadata footer) {
    return zeroNullLeaves(nullCountsByLeaf(footer));
  }

  private static Set<List<String>> zeroNullLeaves(Map<List<String>, @Nullable Long> counts) {
    Set<List<String>> proven = new HashSet<>();
    for (Map.Entry<List<String>, @Nullable Long> entry : counts.entrySet()) {
      Long count = entry.getValue();
      if (count != null && count == 0) {
        proven.add(entry.getKey());
      }
    }
    return proven;
  }

  /**
   * Null count per leaf chunk path, summed over the row groups that have rows; null where any such
   * row group lacks the statistic (an absent count proves nothing). An empty row group holds no
   * nulls whatever its statistics say, so it constrains nothing.
   */
  private static Map<List<String>, @Nullable Long> nullCountsByLeaf(ParquetMetadata footer) {
    Map<List<String>, @Nullable Long> counts = new HashMap<>();
    for (BlockMetaData block : footer.getBlocks()) {
      if (block.getRowCount() == 0) {
        continue;
      }
      for (ColumnChunkMetaData chunk : block.getColumns()) {
        List<String> path = Arrays.asList(chunk.getPath().toArray());
        Statistics<?> stats = chunk.getStatistics();
        Long inChunk = null;
        if (stats != null && stats.isNumNullsSet()) {
          inChunk = stats.getNumNulls();
        }
        if (!counts.containsKey(path)) {
          counts.put(path, inChunk);
          continue;
        }
        Long soFar = counts.get(path);
        if (soFar == null || inChunk == null) {
          counts.put(path, null);
        } else {
          counts.put(path, soFar + inChunk);
        }
      }
    }
    return counts;
  }

  private static final class Tightened {
    final Types.StructType struct;

    /** Some leaf below, not under a list or map, is proven: the struct itself was never null. */
    final boolean hasNullFreeLeaf;

    Tightened(Types.StructType struct, boolean hasNullFreeLeaf) {
      this.struct = struct;
      this.hasNullFreeLeaf = hasNullFreeLeaf;
    }
  }

  private static Tightened tightenStruct(
      Types.StructType struct, List<String> path, Set<List<String>> zeroNulls) {
    List<Types.NestedField> fields = new ArrayList<>();
    boolean hasNullFreeLeaf = false;
    for (Types.NestedField field : struct.fields()) {
      path.add(field.name());
      if (field.type().isPrimitiveType()) {
        boolean nullFreeLeaf = zeroNulls.contains(path);
        fields.add(nullFreeLeaf ? withOptionality(field, field.type(), false) : field);
        hasNullFreeLeaf |= nullFreeLeaf;
      } else if (field.type().isStructType()) {
        Tightened child = tightenStruct(field.type().asStructType(), path, zeroNulls);
        boolean optional = field.isOptional() && !child.hasNullFreeLeaf;
        fields.add(withOptionality(field, child.struct, optional));
        hasNullFreeLeaf |= child.hasNullFreeLeaf;
      } else {
        fields.add(field);
      }
      path.remove(path.size() - 1);
    }
    return new Tightened(Types.StructType.of(fields), hasNullFreeLeaf);
  }

  /** Copies every attribute (id, name, doc, defaults), replacing only type and optionality. */
  private static Types.NestedField withOptionality(
      Types.NestedField field, Type type, boolean optional) {
    return Types.NestedField.from(field).ofType(type).isOptional(optional).build();
  }

  static Schema canonical(Schema schema) {
    Type sorted = TypeUtil.visit(schema.asStruct(), new SortFields());
    int[] nextId = {0};
    return TypeUtil.assignFreshIds(new Schema(sorted.asStructType().fields()), () -> ++nextId[0]);
  }

  /**
   * Rebuilds every struct with its fields sorted by name; every other attribute (optionality, doc,
   * defaults) is preserved. Iceberg owns the traversal, so nested types this code has never heard
   * of (variant, and whatever comes next) are visited rather than silently passed through.
   */
  private static class SortFields extends TypeUtil.SchemaVisitor<Type> {
    @Override
    public Type struct(Types.StructType struct, List<Type> fieldTypes) {
      List<Types.NestedField> rebuilt = new ArrayList<>();
      for (int i = 0; i < struct.fields().size(); i++) {
        Types.NestedField field = struct.fields().get(i);
        rebuilt.add(Types.NestedField.from(field).ofType(fieldTypes.get(i)).build());
      }
      rebuilt.sort((a, b) -> a.name().compareTo(b.name()));
      return Types.StructType.of(rebuilt);
    }

    @Override
    public Type field(Types.NestedField field, Type fieldType) {
      return fieldType;
    }

    @Override
    public Type list(Types.ListType list, Type elementType) {
      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementType);
      }
      return Types.ListType.ofRequired(list.elementId(), elementType);
    }

    @Override
    public Type map(Types.MapType map, Type keyType, Type valueType) {
      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), keyType, valueType);
      }
      return Types.MapType.ofRequired(map.keyId(), map.valueId(), keyType, valueType);
    }

    @Override
    public Type variant(Types.VariantType variant) {
      return variant;
    }

    @Override
    public Type primitive(Type.PrimitiveType primitive) {
      return primitive;
    }
  }
}
