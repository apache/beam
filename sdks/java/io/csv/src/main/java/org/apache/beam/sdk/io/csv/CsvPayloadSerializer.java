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
package org.apache.beam.sdk.io.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CsvPayloadSerializer implements PayloadSerializer {

  private final Schema schema;
  private final CSVFormat csvFormat;

  CsvPayloadSerializer(Schema schema, @Nullable CSVFormat csvFormat) {
    this.schema = schema;
    if (csvFormat == null) {
      csvFormat = CSVFormat.DEFAULT;
    }
    this.csvFormat = csvFormat;
  }

  CSVFormat getCsvFormat() {
    return csvFormat;
  }

  @Override
  public byte[] serialize(Row row) {
    StringBuilder builder = new StringBuilder();
    try {
      boolean newRecord = true;
      for (int i = 0; i < schema.getFieldCount(); i++) {
        String name = schema.getField(i).getName();
        Object value = row.getValue(name);
        if (value == null) {
          value = "";
        }
        csvFormat.print(value, builder, newRecord);
        newRecord = false;
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return builder.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Row deserialize(byte[] bytes) {
    // TODO(https://github.com/apache/beam/issues/24552)
    throw new UnsupportedOperationException("not yet implemented");
  }
}
