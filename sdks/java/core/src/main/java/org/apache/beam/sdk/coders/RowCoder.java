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
package org.apache.beam.sdk.coders;

import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A sub-class of SchemaCoder that can only encode {@link Row} instances. */
@Experimental(Kind.SCHEMAS)
public class RowCoder extends SchemaCoder<Row> {
  public static RowCoder of(Schema schema) {
    return new RowCoder(schema);
  }

  private RowCoder(Schema schema) {
    super(
        schema,
        TypeDescriptors.rows(),
        SerializableFunctions.identity(),
        SerializableFunctions.identity());
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowCoder rowCoder = (RowCoder) o;
    return schema.equals(rowCoder.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }
}
