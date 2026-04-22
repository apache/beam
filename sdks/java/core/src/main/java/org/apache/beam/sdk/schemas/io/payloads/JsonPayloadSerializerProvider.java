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
package org.apache.beam.sdk.schemas.io.payloads;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
@AutoService(PayloadSerializerProvider.class)
public class JsonPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "json";
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> tableParams) {
    return new JsonPayloadSerializer(schema);
  }

  private static class JsonPayloadSerializer implements PayloadSerializer {
    private final Schema schema;
    private transient @Nullable ObjectMapper deserializeMapper;
    private transient @Nullable ObjectMapper serializeMapper;

    public JsonPayloadSerializer(Schema schema) {
      this.schema = schema;
      this.deserializeMapper = null;
      this.serializeMapper = null;
    }

    private synchronized ObjectMapper getDeserializeMapper() {
      if (deserializeMapper == null) {
        deserializeMapper = RowJsonUtils.newObjectMapperWith(RowJsonDeserializer.forSchema(schema));
      }
      return deserializeMapper;
    }

    private synchronized ObjectMapper getSerializeMapper() {
      if (serializeMapper == null) {
        serializeMapper = RowJsonUtils.newObjectMapperWith(RowJsonSerializer.forSchema(schema));
      }
      return serializeMapper;
    }

    @Override
    public byte[] serialize(Row row) {
      return RowJsonUtils.rowToJson(getSerializeMapper(), row).getBytes(UTF_8);
    }

    @Override
    public Row deserialize(byte[] bytes) {
      return RowJsonUtils.jsonToRow(getDeserializeMapper(), new String(bytes, UTF_8));
    }
  }
}
