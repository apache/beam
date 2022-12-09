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

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

/** {@link PayloadSerializerProvider} implementation supporting CSV. */
@AutoService(PayloadSerializerProvider.class)
public class CsvPayloadSerializerProvider implements PayloadSerializerProvider {
  static final String IDENTIFIER = "csv";
  public static final Field CSV_FORMAT_PARAMETER_FIELD =
      Field.nullable("csv_format", FieldType.logicalType(new CsvFormatLogicalType()));
  public static final Field SCHEMA_FIELDS_PARAMETER_FIELD =
      Field.nullable("schema_fields", FieldType.array(FieldType.STRING));
  public static final Schema CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA =
      Schema.of(CSV_FORMAT_PARAMETER_FIELD, SCHEMA_FIELDS_PARAMETER_FIELD);

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /**
   * Validates {@link Map} of {@param params} against {@link
   * #CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA} and converts the {@param params} as a {@link Row}.
   * The design goals of this method are to schematize {@link #getSerializer(Schema, Map)}'s params
   * to improve error handling and developer expectations of the keys and value types.
   */
  static Row rowFrom(Map<String, Object> params) {
    return Row.withSchema(CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA).withFieldValues(params).build();
  }

  /**
   * Implementation of {@link PayloadSerializerProvider#getSerializer(Schema, Map)} method.
   * Instantiates a {@link CsvPayloadSerializer}. See {@link
   * #CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA} for {@param params}'s expected keys and value types.
   */
  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> params) {
    Row paramsRow = rowFrom(params);
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    List<String> schemaFields = null;
    if (paramsRow.getValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName()) != null) {
      schemaFields =
          Objects.requireNonNull(paramsRow.getValue(SCHEMA_FIELDS_PARAMETER_FIELD.getName()));
    }
    if (paramsRow.getValue(CSV_FORMAT_PARAMETER_FIELD.getName()) != null) {
      csvFormat = Objects.requireNonNull(paramsRow.getValue(CSV_FORMAT_PARAMETER_FIELD.getName()));
    }
    return new CsvPayloadSerializer(schema, csvFormat, schemaFields);
  }
}
