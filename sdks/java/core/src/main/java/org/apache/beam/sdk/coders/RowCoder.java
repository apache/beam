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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;

/**
 *  A {@link Coder} for {@link Row}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class RowCoder extends CustomCoder<Row> {
  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private Schema schema;
  private List<Coder> coders;

  private RowCoder(Schema schema, List<Coder> coders) {
    this.schema = schema;
    this.coders = coders;
  }

  public static RowCoder of(Schema schema, List<Coder> coderArray) {
    if (schema.getFieldCount() != coderArray.size()) {
      throw new IllegalArgumentException("Coder size doesn't match with field size");
    }
    return new RowCoder(schema, coderArray);
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public void encode(Row value, OutputStream outStream)
      throws CoderException, IOException {
    nullListCoder.encode(scanNullFields(value), outStream);
    for (int idx = 0; idx < value.getFieldCount(); ++idx) {
      if (value.getValue(idx) == null) {
        continue;
      }

      coders.get(idx).encode(value.getValue(idx), outStream);
    }
  }

  @Override
  public Row decode(InputStream inStream) throws CoderException, IOException {
    BitSet nullFields = nullListCoder.decode(inStream);

    List<Object> fieldValues = new ArrayList<>(schema.getFieldCount());
    for (int idx = 0; idx < schema.getFieldCount(); ++idx) {
      if (nullFields.get(idx)) {
        fieldValues.add(null);
      } else {
        fieldValues.add(coders.get(idx).decode(inStream));
      }
    }
    return Row.withRowType(schema).addValues(fieldValues).build();
  }

  /**
   * Scan {@link Row} to find fields with a NULL value.
   */
  private BitSet scanNullFields(Row row) {
    BitSet nullFields = new BitSet(row.getFieldCount());
    for (int idx = 0; idx < row.getFieldCount(); ++idx) {
      if (row.getValue(idx) == null) {
        nullFields.set(idx);
      }
    }
    return nullFields;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
    for (Coder c : coders) {
      c.verifyDeterministic();
    }
  }

  public List<Coder> getCoders() {
    return Collections.unmodifiableList(coders);
  }
}
