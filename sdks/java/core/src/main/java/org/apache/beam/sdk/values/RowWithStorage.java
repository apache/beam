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

import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Concrete subclass of {@link Row} that explicitly stores all fields of the row. */
@Experimental(Kind.SCHEMAS)
public class RowWithStorage extends Row {
  private final List<Object> values;

  RowWithStorage(Schema schema, List<Object> values) {
    super(schema);
    this.values = values;
  }

  @Override
  @Nullable
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> T getValue(int fieldIdx) {
    if (values.size() > fieldIdx) {
      return (T) values.get(fieldIdx);
    } else {
      throw new IllegalArgumentException("No field at index " + fieldIdx);
    }
  }

  @Override
  public List<Object> getValues() {
    return values;
  }

  @Override
  public int getFieldCount() {
    return values.size();
  }
}
