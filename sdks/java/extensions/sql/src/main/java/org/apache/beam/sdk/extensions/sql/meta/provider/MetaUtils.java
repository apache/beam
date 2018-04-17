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

package org.apache.beam.sdk.extensions.sql.meta.provider;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Utility methods for metadata.
 */
public class MetaUtils {
  public static Schema getRowTypeFromTable(Table table) {
    return
        table
            .getColumns()
            .stream()
            .map(MetaUtils::toRecordField)
            .collect(toSchema());
  }

  private static Schema.Field toRecordField(Column column) {
    String description = column.getComment() != null ? column.getComment() : "";
    return Schema.Field.of(column.getName(), column.getFieldType())
        .withDescription(description)
        .withNullable(column.getNullable());
  }
}
