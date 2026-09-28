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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Decides, once per file, which table column each Parquet column is. Iceberg readers resolve a file
 * by the field ids it carries and consult the table's name mapping only for a file that carries
 * none, so a file's own ids must mean what the table means by them. Everything that reads a
 * footer's columns by id takes the {@link Resolved} footer this produces.
 */
final class ParquetFieldIds {
  private ParquetFieldIds() {}

  /** A footer whose schema carries the table's field ids. */
  static final class Resolved {
    private final ParquetMetadata footer;

    private Resolved(ParquetMetadata footer) {
      this.footer = footer;
    }

    ParquetMetadata footer() {
      return footer;
    }

    /**
     * The file's dotted names for the given table field ids, skipping ids the file lacks. Iceberg
     * looks metrics modes up by these names, which differ from the table's after a rename.
     */
    List<String> columnNames(List<Integer> fieldIds) {
      Schema fileSchema = ParquetSchemaUtil.convertAndPrune(footer.getFileMetaData().getSchema());
      List<String> names = new ArrayList<>();
      for (int fieldId : fieldIds) {
        @Nullable String name = fileSchema.findColumnName(fieldId);
        if (name != null) {
          names.add(name);
        }
      }
      return names;
    }
  }

  /** The file's own field ids disagree with the table; readers would misread its columns. */
  static final class ConflictException extends IllegalArgumentException {
    ConflictException(String message) {
      super(message);
    }
  }

  /**
   * A file without ids gets the table's through {@code mapping}, which must be the one readers will
   * use ({@link NameMappingUtils#forReaders}). A file with ids keeps them when they agree with the
   * table and is refused otherwise: no metrics or mapping written at registration can change how
   * readers resolve it.
   */
  static Resolved resolve(ParquetMetadata footer, Table table, NameMapping mapping) {
    return resolve(footer, table.schema(), table.schemas().values(), mapping);
  }

  /** Resolves against one schema alone: no earlier versions, and a mapping made from it. */
  @VisibleForTesting
  static Resolved resolve(ParquetMetadata footer, Schema schema) {
    return resolve(footer, schema, Collections.singletonList(schema), MappingUtil.create(schema));
  }

  private static Resolved resolve(
      ParquetMetadata footer, Schema current, Collection<Schema> versions, NameMapping mapping) {
    MessageType fileType = footer.getFileMetaData().getSchema();
    if (!ParquetSchemaUtil.hasIds(fileType)) {
      MessageType mapped = ParquetSchemaUtil.applyNameMapping(fileType, mapping);
      FileMetaData meta = footer.getFileMetaData();
      return new Resolved(
          new ParquetMetadata(
              new FileMetaData(mapped, meta.getKeyValueMetaData(), meta.getCreatedBy()),
              footer.getBlocks()));
    }
    @Nullable String conflict = conflict(fileType, current, versions, mapping);
    if (conflict != null) {
      throw new ConflictException(conflict);
    }
    return new Resolved(footer);
  }

  /**
   * An id agrees when some version of the table's schema, or the name mapping, gives it the file
   * column's full path, so files written before a rename stay valid. Comparing only the last name
   * would accept a shipping.city that carries billing.city's id, which readers find under neither.
   * An id the table has never used agrees too, unless the column's path belongs to a table column
   * with another id, which readers would then read as null.
   */
  private static @Nullable String conflict(
      MessageType fileType, Schema current, Collection<Schema> versions, NameMapping mapping) {
    Schema fileSchema;
    try {
      fileSchema = ParquetSchemaUtil.convert(fileType);
    } catch (RuntimeException e) {
      return "its field ids cannot be resolved: " + AddFiles.errorMessage(e);
    }
    Map<Integer, Types.NestedField> byId = new TreeMap<>(TypeUtil.indexById(fileSchema.asStruct()));
    for (Types.NestedField field : byId.values()) {
      int id = field.fieldId();
      String path = fileSchema.findColumnName(id);
      if (knownAs(versions, mapping, id, path)) {
        continue;
      }
      Types.@Nullable NestedField sameId = current.findField(id);
      if (sameId != null) {
        return "column "
            + path
            + " carries field id "
            + id
            + ", which the table uses for column "
            + current.findColumnName(id);
      }
      Types.@Nullable NestedField sameName = current.findField(path);
      if (sameName != null) {
        return "column "
            + path
            + " carries field id "
            + id
            + ", but the table's column "
            + path
            + " has field id "
            + sameName.fieldId();
      }
    }
    return null;
  }

  /** Paths are dotted full names, as both Iceberg schemas and name mappings index them. */
  private static boolean knownAs(
      Collection<Schema> versions, NameMapping mapping, int id, String path) {
    for (Schema schema : versions) {
      if (path.equals(schema.findColumnName(id))) {
        return true;
      }
    }
    @Nullable MappedField mapped = mapping.find(path);
    if (mapped == null) {
      return false;
    }
    @Nullable Integer mappedId = mapped.id();
    return mappedId != null && mappedId == id;
  }
}
