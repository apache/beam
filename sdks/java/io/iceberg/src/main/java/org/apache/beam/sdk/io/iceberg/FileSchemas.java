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
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Derives the schema a file contributes to schema inference. The canonical form sorts struct fields
 * by name at every level and renumbers ids in deterministic order, so files that differ only in
 * column order produce identical JSON. Ids are positional and meaningless: the commit side
 * reconciles columns by name.
 */
final class FileSchemas {
  private FileSchemas() {}

  static String canonicalJson(ParquetMetadata footer) {
    Schema converted = ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema());
    return SchemaParser.toJson(canonical(converted));
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
