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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Predefined;

/** {@link Schema.LogicalType} for {@link CSVFormat}. */
@Experimental(Kind.SCHEMAS)
public class CsvFormatLogicalType implements Schema.LogicalType<CSVFormat, String> {
  public static final String IDENTIFIER = "beam:logical_type:csv_format:v1";
  private static final Map<CSVFormat, String> FORMAT_STRING_MAP = new HashMap<>();

  static {
    for (Predefined predefined : Predefined.values()) {
      CSVFormat csvFormat = CSVFormat.valueOf(predefined.name());
      FORMAT_STRING_MAP.put(csvFormat, predefined.name());
    }
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  // unused
  @Override
  public Schema.FieldType getArgumentType() {
    return FieldType.STRING;
  }

  @Override
  public FieldType getBaseType() {
    return FieldType.STRING;
  }

  /** Converts a {@link CSVFormat} to its {@link Predefined#name()}. */
  @Override
  public String toBaseType(CSVFormat input) {
    if (!FORMAT_STRING_MAP.containsKey(input)) {
      throw new IllegalArgumentException(String.format("%s not supported", input));
    }
    return FORMAT_STRING_MAP.get(input);
  }

  /** Converts to a {@link CSVFormat} from its {@link Predefined#name()}. */
  @Override
  public CSVFormat toInputType(String base) {
    return CSVFormat.valueOf(base);
  }
}
