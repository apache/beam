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
package org.apache.beam.sdk.io.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonNull;
import org.bson.Document;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility methods for MongoDB IO. */
public class MongoDbUtils {

  /** Converts a Beam {@link Row} to a BSON {@link Document}. */
  public static Document toDocument(Row row) {
    Object converted = convertToBsonValue(row);
    if (converted instanceof Document) {
      return (Document) converted;
    }
    throw new IllegalArgumentException(
        "Expected Document but got "
            + (converted != null ? converted.getClass().getName() : "null"));
  }

  private static @Nullable Object convertToBsonValue(@Nullable Object value) {
    if (value == null) {
      return new BsonNull();
    }
    if (value instanceof Row) {
      Row row = (Row) value;
      Document doc = new Document();
      for (Field field : row.getSchema().getFields()) {
        Object fieldValue = row.getValue(field.getName());
        Object converted = convertToBsonValue(fieldValue);
        doc.append(field.getName(), converted != null ? converted : new BsonNull());
      }
      return doc;
    } else if (value instanceof Iterable) {
      List<Object> bsonList = new ArrayList<>();
      for (Object item : (Iterable<?>) value) {
        Object converted = convertToBsonValue(item);
        bsonList.add(converted != null ? converted : new BsonNull());
      }
      return bsonList;
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Document doc = new Document();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object converted = convertToBsonValue(entry.getValue());
        doc.append(String.valueOf(entry.getKey()), converted != null ? converted : new BsonNull());
      }
      return doc;
    }
    return value;
  }
}
