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
package org.apache.beam.sdk.schemas.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.json.JSONObject;

/**
 * Utils to convert JSON records to Beam {@link Row}.
 *
 * <h2>JSON-Schema (https://json-schema.org) support</h2>
 *
 * <p>This class provides utility methods to parse, validate and translate between <b>JSON
 * Schema</b>-formatted schemas and Beam Schemas. The support is based on the <code>
 * everit-json-schema</code> package, which is <b>not provided by default</b>.
 *
 * <p>Therefore, functionality in {@link JsonUtils::beamSchemaFromJsonSchema} requires that you
 * include {@code everit-json-schema} in your project like so:
 *
 * <pre>{@code
 *  <dependency>
 * 	  <groupId>com.github.erosb < /groupId>
 * 	  <artifactId>everit-json-schema < /artifactId>
 * 	  <version>${everit-json.version} < /version>
 *  < /dependency>
 * }</pre>
 *
 * <p><b>Note:</b> This functionality has been tested with {@code everit-json-schema} version
 * 1.14.1.
 *
 * <h3>JSON-Schema supported features</h3>
 *
 * <p>The current Beam implementation does not support all possible features of JSON-schema. The
 * current implementation supports:
 *
 * <p>
 *
 * <ul>
 *   <li>String, boolean and numeric values (integer and floating-point).
 *   <li>Arrays with any <b>one</b> of the supported types as elements. This includes nested arrays.
 *   <li>Objects with any of the supported types as fields. This includes nested objects.
 *   <li>Fields marked as required are non-null. Other fields are nullable.
 * </ul>
 *
 * <p><b>The following JSON-schema features are not supported:</b>
 *
 * <p>
 *
 * <ul>
 *   <li>Tuple-like arrays (or arrays with multiple item types).
 *   <li>Validation of row regular expressions, enum values, array lengths, etc.
 *   <li>Special annotations for types (e.g. <code>contentMediaType</code>) are ignored.
 *   <li>Composite schemas (schemas made out of a <a
 *       href="https://json-schema.org/understanding-json-schema/reference/combining.html#combining">combination
 *       of other schemas</a>)
 * </ul>
 */
@Experimental(Kind.SCHEMAS)
public class JsonUtils {

  /** Returns a {@link SimpleFunction} mapping JSON byte[] arrays to Beam {@link Row}s. */
  public static SimpleFunction<byte[], Row> getJsonBytesToRowFunction(Schema beamSchema) {
    return new JsonToRowFn<byte[]>(beamSchema) {
      @Override
      public Row apply(byte[] input) {
        String jsonString = byteArrayToJsonString(input);
        return RowJsonUtils.jsonToRow(objectMapper, jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping JSON {@link String}s to Beam {@link Row}s. */
  public static SimpleFunction<String, Row> getJsonStringToRowFunction(Schema beamSchema) {
    return new JsonToRowFn<String>(beamSchema) {
      @Override
      public Row apply(String jsonString) {
        return RowJsonUtils.jsonToRow(objectMapper, jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping Beam {@link Row}s to JSON byte[] arrays. */
  public static SimpleFunction<Row, byte[]> getRowToJsonBytesFunction(Schema beamSchema) {
    return new RowToJsonFn<byte[]>(beamSchema) {
      @Override
      public byte[] apply(Row input) {
        String jsonString = RowJsonUtils.rowToJson(objectMapper, input);
        return jsonStringToByteArray(jsonString);
      }
    };
  }

  /** Returns a {@link SimpleFunction} mapping Beam {@link Row}s to JSON {@link String}s. */
  public static SimpleFunction<Row, String> getRowToJsonStringsFunction(Schema beamSchema) {
    return new RowToJsonFn<String>(beamSchema) {
      @Override
      public String apply(Row input) {
        return RowJsonUtils.rowToJson(objectMapper, input);
      }
    };
  }

  public static Schema beamSchemaFromJsonSchema(String jsonSchemaStr) {
    org.everit.json.schema.ObjectSchema jsonSchema = jsonSchemaFromString(jsonSchemaStr);
    return beamSchemaFromJsonSchema(jsonSchema);
  }

  private static Schema beamSchemaFromJsonSchema(org.everit.json.schema.ObjectSchema jsonSchema) {
    Schema.Builder beamSchemaBuilder = Schema.builder();
    for (Map.Entry<String, org.everit.json.schema.Schema> entry :
        jsonSchema.getPropertySchemas().entrySet()) {
      String propertyName = entry.getKey();
      org.everit.json.schema.Schema propertySchema = entry.getValue();
      if (propertySchema == null) {
        throw new IllegalArgumentException("Unable to parse schema " + jsonSchema);
      }
      java.util.function.BiFunction<String, Schema.FieldType, Schema.Field> fieldConstructor =
          jsonSchema.getRequiredProperties().contains(propertyName)
              ? Schema.Field::of
              : Schema.Field::nullable;
      if (propertySchema instanceof org.everit.json.schema.ArraySchema) {
        if (((ArraySchema) propertySchema).getAllItemSchema() == null) {
          throw new IllegalArgumentException(
              "Tuple-like arrays are unsupported. Expected a single item type for field "
                  + propertyName);
        }
        beamSchemaBuilder =
            beamSchemaBuilder.addField(
                fieldConstructor.apply(
                    propertyName,
                    Schema.FieldType.array(
                        beamTypeFromJsonSchemaType(
                            ((ArraySchema) propertySchema).getAllItemSchema()))));
      } else {
        try {
          beamSchemaBuilder =
              beamSchemaBuilder.addField(
                  fieldConstructor.apply(propertyName, beamTypeFromJsonSchemaType(propertySchema)));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Unsupported field type " + propertySchema.getClass() + " in field " + propertyName,
              e);
        }
      }
    }
    return beamSchemaBuilder.build();
  }

  private static Schema.FieldType beamTypeFromJsonSchemaType(
      org.everit.json.schema.Schema propertySchema) {
    if (propertySchema instanceof org.everit.json.schema.ObjectSchema) {
      return Schema.FieldType.row(beamSchemaFromJsonSchema((ObjectSchema) propertySchema));
    } else if (propertySchema instanceof org.everit.json.schema.BooleanSchema) {
      return Schema.FieldType.BOOLEAN;
    } else if (propertySchema instanceof org.everit.json.schema.NumberSchema) {
      return ((NumberSchema) propertySchema).requiresInteger()
          ? Schema.FieldType.INT64
          : Schema.FieldType.DOUBLE;
    } else if (propertySchema instanceof org.everit.json.schema.StringSchema) {
      return Schema.FieldType.STRING;
    } else if (propertySchema instanceof org.everit.json.schema.ReferenceSchema) {
      org.everit.json.schema.Schema sch = ((ReferenceSchema) propertySchema).getReferredSchema();
      return beamTypeFromJsonSchemaType(sch);
    } else if (propertySchema instanceof org.everit.json.schema.ArraySchema) {
      if (((ArraySchema) propertySchema).getAllItemSchema() == null) {
        throw new IllegalArgumentException(
            "Array schema is not properly formatted or unsupported ("
                + propertySchema
                + "). Note that JSON-schema's tuple-like arrays are not supported by Beam.");
      }
      return Schema.FieldType.array(
          beamTypeFromJsonSchemaType(((ArraySchema) propertySchema).getAllItemSchema()));
    } else {
      throw new IllegalArgumentException("Unsupported schema type: " + propertySchema.getClass());
    }
  }

  private static org.everit.json.schema.ObjectSchema jsonSchemaFromString(String jsonSchema) {
    JSONObject parsedSchema = new JSONObject(jsonSchema);
    org.everit.json.schema.Schema schemaValidator =
        org.everit.json.schema.loader.SchemaLoader.load(parsedSchema);
    if (!(schemaValidator instanceof ObjectSchema)) {
      throw new IllegalArgumentException(
          String.format("The schema is not a valid object schema:%n %s", jsonSchema));
    }
    return (org.everit.json.schema.ObjectSchema) schemaValidator;
  }

  private abstract static class JsonToRowFn<T> extends SimpleFunction<T, Row> {
    final RowJson.RowJsonDeserializer deserializer;
    final ObjectMapper objectMapper;

    private JsonToRowFn(Schema beamSchema) {
      deserializer = RowJson.RowJsonDeserializer.forSchema(beamSchema);
      objectMapper = RowJsonUtils.newObjectMapperWith(deserializer);
    }
  }

  private abstract static class RowToJsonFn<T> extends SimpleFunction<Row, T> {
    final RowJson.RowJsonSerializer serializer;
    final ObjectMapper objectMapper;

    private RowToJsonFn(Schema beamSchema) {
      serializer = RowJson.RowJsonSerializer.forSchema(beamSchema);
      objectMapper = RowJsonUtils.newObjectMapperWith(serializer);
    }
  }

  static byte[] jsonStringToByteArray(String jsonString) {
    return jsonString.getBytes(StandardCharsets.UTF_8);
  }

  static String byteArrayToJsonString(byte[] jsonBytes) {
    return new String(jsonBytes, StandardCharsets.UTF_8);
  }
}
