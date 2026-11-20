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
package org.apache.beam.sdk.schemas.variant;

import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_ARRAY;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_OBJECT;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.BASIC_TYPE_SHORT_STRING;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.primitiveHeader;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.sizeInBytes;
import static org.apache.beam.sdk.schemas.variant.VariantEncoding.writeLittleEndian;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Encodes JSON into {@link Variant Variants}.
 *
 * <p>JSON is first read into a small tree ({@code Map}, {@code List}, {@code Long}, {@code
 * BigDecimal}, {@code Double}, {@code String}, {@code Boolean}, {@code null}) and then encoded. The
 * encoder always produces the same canonical bytes for the same logical value: the metadata
 * dictionary contains exactly the keys used, sorted and unique; object fields are sorted by key
 * (unsigned UTF-8 byte order); integers, offsets and ids use the smallest width that fits.
 */
final class VariantBuilder {
  private VariantBuilder() {}

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  // ---------------------------------------------------------------------------------------------
  // JSON -> tree
  // ---------------------------------------------------------------------------------------------

  static Variant parseJson(String json) {
    if (json == null) {
      throw new IllegalArgumentException("JSON text must not be null");
    }
    try (JsonParser parser = JSON_FACTORY.createParser(json)) {
      JsonToken token = parser.nextToken();
      if (token == null) {
        throw new IllegalArgumentException("Cannot parse empty JSON text as a Variant");
      }
      Object tree = readJson(parser, token);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("Unexpected trailing content after JSON value");
      }
      return encode(tree);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
    }
  }

  private static @Nullable Object readJson(JsonParser parser, JsonToken token) throws IOException {
    switch (token) {
      case START_OBJECT:
        {
          Map<String, @Nullable Object> map = new LinkedHashMap<>();
          while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String key = parser.currentName();
            JsonToken valueToken = parser.nextToken();
            if (map.containsKey(key)) {
              throw new IllegalArgumentException("Duplicate key in JSON object: " + key);
            }
            map.put(key, readJson(parser, valueToken));
          }
          return map;
        }
      case START_ARRAY:
        {
          List<@Nullable Object> list = new ArrayList<>();
          JsonToken next;
          while ((next = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (next == null) {
              throw new IllegalArgumentException("Unterminated JSON array");
            }
            list.add(readJson(parser, next));
          }
          return list;
        }
      case VALUE_STRING:
        return parser.getText();
      case VALUE_NUMBER_INT:
        {
          JsonParser.NumberType numberType = parser.getNumberType();
          if (numberType == JsonParser.NumberType.INT || numberType == JsonParser.NumberType.LONG) {
            return parser.getLongValue();
          }
          return numberFromText(parser.getText(), parser.getDoubleValue());
        }
      case VALUE_NUMBER_FLOAT:
        return numberFromText(parser.getText(), parser.getDoubleValue());
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      default:
        throw new IllegalArgumentException("Unexpected JSON token " + token);
    }
  }

  /** Builds a Variant from a Jackson tree, following the same rules as {@link #parseJson}. */
  static Variant fromJsonNode(JsonNode node) {
    return encode(jsonNodeToTree(node));
  }

  private static @Nullable Object jsonNodeToTree(JsonNode node) {
    switch (node.getNodeType()) {
      case OBJECT:
        {
          Map<String, @Nullable Object> map = new LinkedHashMap<>();
          Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            map.put(field.getKey(), jsonNodeToTree(field.getValue()));
          }
          return map;
        }
      case ARRAY:
        {
          List<@Nullable Object> list = new ArrayList<>(node.size());
          for (JsonNode element : node) {
            list.add(jsonNodeToTree(element));
          }
          return list;
        }
      case STRING:
        return node.textValue();
      case BOOLEAN:
        return node.booleanValue();
      case NUMBER:
        if (node.isIntegralNumber() && node.canConvertToLong()) {
          return node.longValue();
        }
        return numberFromText(
            node.isBigDecimal() ? node.decimalValue().toString() : node.asText(),
            node.doubleValue());
      case NULL:
      case MISSING:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported JSON node type " + node.getNodeType());
    }
  }

  /**
   * Spark's {@code parse_json} rule: a number written without an exponent and with at most 38
   * digits is an exact decimal; anything else is a double.
   */
  private static Object numberFromText(String text, double fallback) {
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (!(Character.isDigit(c) || c == '-' || c == '.')) {
        return fallback;
      }
    }
    try {
      BigDecimal decimal = new BigDecimal(text);
      if (decimal.precision() <= VariantEncoding.MAX_DECIMAL16_PRECISION
          && decimal.scale() <= VariantEncoding.MAX_DECIMAL16_PRECISION) {
        return decimal;
      }
    } catch (NumberFormatException e) {
      // fall through
    }
    return fallback;
  }

  // ---------------------------------------------------------------------------------------------
  // Tree -> bytes
  // ---------------------------------------------------------------------------------------------

  /**
   * Encodes a JSON tree. An existing {@link Variant} is also accepted, in which case it is
   * re-encoded in canonical form.
   */
  static Variant encode(@Nullable Object tree) {
    TreeSet<String> keys = new TreeSet<>(VariantEncoding.KEY_ORDER);
    collectKeys(tree, keys);
    Map<String, Integer> ids = new HashMap<>();
    for (String key : keys) {
      ids.put(key, ids.size());
    }
    return Variant.of(encodeMetadata(new ArrayList<>(keys)), encodeValue(tree, ids));
  }

  private static void collectKeys(@Nullable Object value, TreeSet<String> keys) {
    if (value instanceof Variant) {
      Variant variant = (Variant) value;
      if (variant.getType() == VariantType.OBJECT) {
        for (String name : variant.fieldNames()) {
          keys.add(name);
          collectKeys(variant.getField(name), keys);
        }
      } else if (variant.getType() == VariantType.ARRAY) {
        for (int i = 0; i < variant.arraySize(); i++) {
          collectKeys(variant.getElement(i), keys);
        }
      }
    } else if (value instanceof Map) {
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        keys.add(key(entry.getKey()));
        collectKeys(entry.getValue(), keys);
      }
    } else if (value instanceof List) {
      for (Object element : (List<?>) value) {
        collectKeys(element, keys);
      }
    }
  }

  private static String key(@Nullable Object key) {
    if (!(key instanceof String)) {
      throw new IllegalArgumentException("Variant object keys must be strings, got " + key);
    }
    return (String) key;
  }

  private static byte[] encodeMetadata(List<String> sortedKeys) {
    int dictionarySize = sortedKeys.size();
    List<byte[]> keyBytes = new ArrayList<>(dictionarySize);
    int totalBytes = 0;
    for (String key : sortedKeys) {
      byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
      keyBytes.add(bytes);
      totalBytes = Math.addExact(totalBytes, bytes.length);
    }
    int offsetSize = sizeInBytes(Math.max(totalBytes, dictionarySize));
    int header =
        VariantEncoding.VERSION
            | (dictionarySize > 0 ? VariantEncoding.SORTED_STRINGS_BIT : 0)
            | ((offsetSize - 1) << 6);
    byte[] out = new byte[1 + offsetSize * (dictionarySize + 2) + totalBytes];
    out[0] = (byte) header;
    writeLittleEndian(out, 1, dictionarySize, offsetSize);
    int offsetPos = 1 + offsetSize;
    int stringPos = offsetPos + (dictionarySize + 1) * offsetSize;
    int offset = 0;
    for (int i = 0; i < dictionarySize; i++) {
      writeLittleEndian(out, offsetPos + i * offsetSize, offset, offsetSize);
      byte[] bytes = keyBytes.get(i);
      System.arraycopy(bytes, 0, out, stringPos + offset, bytes.length);
      offset += bytes.length;
    }
    writeLittleEndian(out, offsetPos + dictionarySize * offsetSize, offset, offsetSize);
    return out;
  }

  private static byte[] encodeValue(@Nullable Object value, Map<String, Integer> ids) {
    if (value == null) {
      return new byte[] {primitiveHeader(VariantEncoding.PRIMITIVE_NULL)};
    } else if (value instanceof Variant) {
      return encodeVariant((Variant) value, ids);
    } else if (value instanceof Boolean) {
      return new byte[] {
        primitiveHeader(
            (Boolean) value ? VariantEncoding.PRIMITIVE_TRUE : VariantEncoding.PRIMITIVE_FALSE)
      };
    } else if (value instanceof Long) {
      return encodeLong((Long) value);
    } else if (value instanceof Double) {
      byte[] out = new byte[9];
      out[0] = primitiveHeader(VariantEncoding.PRIMITIVE_DOUBLE);
      writeLittleEndian(out, 1, Double.doubleToLongBits((Double) value), 8);
      return out;
    } else if (value instanceof BigDecimal) {
      return encodeDecimal((BigDecimal) value);
    } else if (value instanceof String) {
      return encodeString((String) value);
    } else if (value instanceof Map) {
      TreeMap<String, @Nullable Object> sorted = new TreeMap<>(VariantEncoding.KEY_ORDER);
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        sorted.put(key(entry.getKey()), entry.getValue());
      }
      List<String> names = new ArrayList<>(sorted.keySet());
      List<byte[]> children = new ArrayList<>(sorted.size());
      for (Object child : sorted.values()) {
        children.add(encodeValue(child, ids));
      }
      return encodeObject(names, children, ids);
    } else if (value instanceof List) {
      List<byte[]> children = new ArrayList<>();
      for (Object element : (List<?>) value) {
        children.add(encodeValue(element, ids));
      }
      return encodeArray(children);
    }
    throw new IllegalArgumentException("Cannot encode " + value.getClass().getName());
  }

  /** Re-encodes an existing Variant against a new dictionary. Primitives are copied as-is. */
  private static byte[] encodeVariant(Variant variant, Map<String, Integer> ids) {
    VariantType type = variant.getType();
    if (type == VariantType.OBJECT) {
      TreeSet<String> names = new TreeSet<>(VariantEncoding.KEY_ORDER);
      names.addAll(variant.fieldNames());
      List<byte[]> children = new ArrayList<>(names.size());
      for (String name : names) {
        Variant child = variant.getField(name);
        if (child == null) {
          throw new MalformedVariantException("Field '" + name + "' listed but not found");
        }
        children.add(encodeVariant(child, ids));
      }
      return encodeObject(new ArrayList<>(names), children, ids);
    } else if (type == VariantType.ARRAY) {
      List<byte[]> children = new ArrayList<>(variant.arraySize());
      for (int i = 0; i < variant.arraySize(); i++) {
        children.add(encodeVariant(variant.getElement(i), ids));
      }
      return encodeArray(children);
    } else if (type == VariantType.INT16
        || type == VariantType.INT32
        || type == VariantType.INT64) {
      return encodeLong(variant.getLong()); // canonical integer width
    }
    return variant.value();
  }

  private static byte[] encodeObject(
      List<String> sortedNames, List<byte[]> children, Map<String, Integer> ids) {
    int n = sortedNames.size();
    int maxId = 0;
    int[] fieldIds = new int[n];
    for (int i = 0; i < n; i++) {
      Integer id = ids.get(sortedNames.get(i));
      if (id == null) {
        throw new IllegalStateException("Key missing from dictionary: " + sortedNames.get(i));
      }
      fieldIds[i] = id;
      maxId = Math.max(maxId, id);
    }
    int dataSize = totalSize(children);
    boolean isLarge = n > 0xFF;
    int numSize = isLarge ? 4 : 1;
    int idSize = sizeInBytes(maxId);
    int offsetSize = sizeInBytes(dataSize);
    int dataPos = 1 + numSize + n * idSize + (n + 1) * offsetSize;
    byte[] out = new byte[Math.addExact(dataPos, dataSize)];
    int valueHeader = ((isLarge ? 1 : 0) << 4) | ((idSize - 1) << 2) | (offsetSize - 1);
    out[0] = (byte) ((valueHeader << 2) | BASIC_TYPE_OBJECT);
    writeLittleEndian(out, 1, n, numSize);
    int idPos = 1 + numSize;
    int offsetPos = idPos + n * idSize;
    for (int i = 0; i < n; i++) {
      writeLittleEndian(out, idPos + i * idSize, fieldIds[i], idSize);
    }
    writeChildren(out, offsetPos, offsetSize, dataPos, children);
    return out;
  }

  private static byte[] encodeArray(List<byte[]> children) {
    int n = children.size();
    int dataSize = totalSize(children);
    boolean isLarge = n > 0xFF;
    int numSize = isLarge ? 4 : 1;
    int offsetSize = sizeInBytes(dataSize);
    int dataPos = 1 + numSize + (n + 1) * offsetSize;
    byte[] out = new byte[Math.addExact(dataPos, dataSize)];
    int valueHeader = ((isLarge ? 1 : 0) << 2) | (offsetSize - 1);
    out[0] = (byte) ((valueHeader << 2) | BASIC_TYPE_ARRAY);
    writeLittleEndian(out, 1, n, numSize);
    writeChildren(out, 1 + numSize, offsetSize, dataPos, children);
    return out;
  }

  private static int totalSize(List<byte[]> children) {
    int size = 0;
    for (byte[] child : children) {
      size = Math.addExact(size, child.length);
    }
    return size;
  }

  /** Writes the offset list (n + 1 entries) and the children's bytes. */
  private static void writeChildren(
      byte[] out, int offsetPos, int offsetSize, int dataPos, List<byte[]> children) {
    int offset = 0;
    for (int i = 0; i < children.size(); i++) {
      writeLittleEndian(out, offsetPos + i * offsetSize, offset, offsetSize);
      byte[] child = children.get(i);
      System.arraycopy(child, 0, out, dataPos + offset, child.length);
      offset += child.length;
    }
    writeLittleEndian(out, offsetPos + children.size() * offsetSize, offset, offsetSize);
  }

  private static byte[] encodeLong(long value) {
    int size;
    int type;
    if (value == (byte) value) {
      size = 1;
      type = VariantEncoding.PRIMITIVE_INT8;
    } else if (value == (short) value) {
      size = 2;
      type = VariantEncoding.PRIMITIVE_INT16;
    } else if (value == (int) value) {
      size = 4;
      type = VariantEncoding.PRIMITIVE_INT32;
    } else {
      size = 8;
      type = VariantEncoding.PRIMITIVE_INT64;
    }
    byte[] out = new byte[1 + size];
    out[0] = primitiveHeader(type);
    writeLittleEndian(out, 1, value, size);
    return out;
  }

  private static byte[] encodeDecimal(BigDecimal decimal) {
    if (decimal.scale() < 0) {
      decimal = decimal.setScale(0);
    }
    int precision = decimal.precision();
    int size;
    int type;
    if (precision <= VariantEncoding.MAX_DECIMAL4_PRECISION) {
      size = 4;
      type = VariantEncoding.PRIMITIVE_DECIMAL4;
    } else if (precision <= VariantEncoding.MAX_DECIMAL8_PRECISION) {
      size = 8;
      type = VariantEncoding.PRIMITIVE_DECIMAL8;
    } else {
      size = 16;
      type = VariantEncoding.PRIMITIVE_DECIMAL16;
    }
    byte[] out = new byte[2 + size];
    out[0] = primitiveHeader(type);
    out[1] = (byte) decimal.scale();
    byte[] bigEndian = decimal.unscaledValue().toByteArray(); // two's complement
    byte sign = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
    for (int i = 0; i < size; i++) {
      int src = bigEndian.length - 1 - i;
      out[2 + i] = src >= 0 ? bigEndian[src] : sign;
    }
    return out;
  }

  private static byte[] encodeString(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= VariantEncoding.MAX_SHORT_STRING_LENGTH) {
      byte[] out = new byte[1 + bytes.length];
      out[0] = (byte) ((bytes.length << 2) | BASIC_TYPE_SHORT_STRING);
      System.arraycopy(bytes, 0, out, 1, bytes.length);
      return out;
    }
    byte[] out = new byte[5 + bytes.length];
    out[0] = primitiveHeader(VariantEncoding.PRIMITIVE_STRING);
    writeLittleEndian(out, 1, bytes.length, 4);
    System.arraycopy(bytes, 0, out, 5, bytes.length);
    return out;
  }
}
