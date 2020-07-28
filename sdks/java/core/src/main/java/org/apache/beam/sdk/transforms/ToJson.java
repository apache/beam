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

import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;
import static org.apache.beam.sdk.util.RowJsonUtils.rowToJson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <i>Experimental</i>
 *
 * <p>Creates a {@link PTransform} that serializes UTF-8 JSON objects from a {@link Schema}-aware
 * PCollection (i.e. {@link PCollection#hasSchema()} returns true). JSON format is compatible with
 * {@link JsonToRow}.
 *
 * <p>For specifics of JSON serialization see {@link RowJsonSerializer}.
 */
@Experimental
public class ToJson<T> extends PTransform<PCollection<T>, PCollection<String>> {
  private transient volatile @Nullable ObjectMapper objectMapper;

  private ToJson() {}

  public static <T> ToJson<T> of() {
    return new ToJson<>();
  }

  @Override
  public PCollection<String> expand(PCollection<T> rows) {
    Schema inputSchema = rows.getSchema();
    // Throw exception if this schema is not supported by RowJson
    RowJson.verifySchemaSupported(inputSchema);
    SerializableFunction<T, Row> toRow = rows.getToRowFunction();
    return rows.apply(
        ParDo.of(
            new DoFn<T, String>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                context.output(
                    rowToJson(objectMapper(inputSchema), toRow.apply(context.element())));
              }
            }));
  }

  private ObjectMapper objectMapper(Schema schema) {
    if (this.objectMapper == null) {
      synchronized (this) {
        if (this.objectMapper == null) {
          this.objectMapper = newObjectMapperWith(RowJsonSerializer.forSchema(schema));
        }
      }
    }

    return this.objectMapper;
  }
}
