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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.util.JsonToRowUtils.jsonToRow;
import static org.apache.beam.sdk.util.JsonToRowUtils.newObjectMapperWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJsonDeserializer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * <i>Experimental</i>
 *
 * <p>Creates a {@link PTransform} to convert input JSON objects to {@link Row Rows}
 * with given {@link Schema}.
 *
 * <p>Currently supported {@link Schema} field types are: <ul> <li>{@link Schema.TypeName#BYTE}</li>
 * <li>{@link Schema.TypeName#INT16}</li> <li>{@link Schema.TypeName#INT32}</li> <li>{@link
 * Schema.TypeName#INT64}</li> <li>{@link Schema.TypeName#FLOAT}</li> <li>{@link
 * Schema.TypeName#DOUBLE}</li> <li>{@link Schema.TypeName#BOOLEAN}</li> <li>{@link
 * Schema.TypeName#STRING}</li> </ul>
 *
 * <p>For specifics of JSON deserialization see {@link RowJsonDeserializer}.
 *
 * <p>Conversion is strict, with minimal type coercion:
 *
 * <p>Booleans are only parsed from {@code true} or {@code false} literals, not from {@code "true"}
 * or {@code "false"} strings or any other values (exception is thrown in these cases).
 *
 * <p>If a JSON number doesn't fit into the corresponding schema field type, an exception is be
 * thrown. Strings are not auto-converted to numbers. Floating point numbers are not auto-converted
 * to integral numbers. Precision loss also causes exceptions.
 *
 * <p>Only JSON string values can be parsed into {@link TypeName#STRING}. Numbers, booleans are not
 * automatically converted, exceptions are thrown in these cases.
 *
 * <p>If a schema field is missing from the JSON value, an exception will be thrown.
 *
 * <p>Explicit {@code null} literals are allowed in JSON objects. No other values are parsed into
 * {@code null}.
 */
@Experimental
public class JsonToRow {

  public static PTransform<PCollection<? extends String>, PCollection<Row>> withSchema(
      Schema rowSchema) {
    return JsonToRowFn.forSchema(rowSchema);
  }

  static class JsonToRowFn extends PTransform<PCollection<? extends String>, PCollection<Row>> {
    private transient volatile @Nullable ObjectMapper objectMapper;
    private Schema schema;

    static JsonToRowFn forSchema(Schema rowSchema) {
      return new JsonToRowFn(rowSchema);
    }

    private JsonToRowFn(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<? extends String> jsonStrings) {
      return jsonStrings
          .apply(
              ParDo.of(
                  new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      context.output(jsonToRow(objectMapper(), context.element()));
                    }
                  }))
          .setCoder(schema.getRowCoder());
    }

    private ObjectMapper objectMapper() {
      if (this.objectMapper == null) {
        synchronized (this) {
          if (this.objectMapper == null) {
            this.objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(this.schema));
          }
        }
      }

      return this.objectMapper;
    }
  }
}
