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
package org.apache.beam.sdk.values;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Concrete subclass of {@link Row} that explicitly stores all fields of the row. */
public class RowWithStorage extends Row {
  private @Nullable List<@Nullable Object> values;
  private final @Nullable Object @Nullable [] arrayValues;

  RowWithStorage(Schema schema, List<@Nullable Object> values) {
    super(schema);
    this.values = values;
    this.arrayValues = null;
  }

  RowWithStorage(Schema schema, @Nullable Object[] arrayValues) {
    super(schema);
    this.values = null;
    this.arrayValues = arrayValues;
  }

  @Override
  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  public <T extends @Nullable Object> T getValue(int fieldIdx) {
    @Nullable Object @Nullable [] arr = arrayValues;
    if (arr != null) {
      if (fieldIdx >= 0 && fieldIdx < arr.length) {
        return (T) arr[fieldIdx];
      }
      throw new IllegalArgumentException("No field at index " + fieldIdx);
    }
    List<@Nullable Object> list = values;
    if (list != null && fieldIdx >= 0 && fieldIdx < list.size()) {
      return (T) list.get(fieldIdx);
    }
    throw new IllegalArgumentException("No field at index " + fieldIdx);
  }

  /**
   * Returns the backing array directly when this row was created from an array, or {@code null} if
   * backed by a {@link List}. Callers MUST NOT mutate the returned array.
   */
  @Internal
  public @Nullable Object @Nullable [] getRawArray() {
    return arrayValues;
  }

  @Override
  public List<@Nullable Object> getValues() {
    List<@Nullable Object> list = values;
    if (list == null) {
      @Nullable Object @Nullable [] arr = arrayValues;
      if (arr == null) {
        throw new IllegalStateException("RowWithStorage has neither values list nor array");
      }
      list = Arrays.asList(arr);
      values = list;
    }
    return list;
  }

  @Override
  public int getFieldCount() {
    @Nullable Object @Nullable [] arr = arrayValues;
    if (arr != null) {
      return arr.length;
    }
    List<@Nullable Object> list = values;
    return list != null ? list.size() : 0;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    ObjectOutputStream.PutField fields = out.putFields();
    fields.put("values", arrayValues != null ? null : values);
    fields.put("arrayValues", arrayValues);
    out.writeFields();
  }
}
