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
import java.util.BitSet;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;

/**
 *  A {@link Coder} for {@link BeamRecord}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class BeamRecordCoder extends CustomCoder<BeamRecord> {
  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private BeamRecordType recordType;
  private List<Coder> coderArray;

  private BeamRecordCoder(BeamRecordType recordType, List<Coder> coderArray) {
    this.recordType = recordType;
    this.coderArray = coderArray;
  }

  public static BeamRecordCoder of(BeamRecordType recordType, List<Coder> coderArray){
    if (recordType.size() != coderArray.size()) {
      throw new IllegalArgumentException("Coder size doesn't match with field size");
    }
    return new BeamRecordCoder(recordType, coderArray);
  }

  public BeamRecordType getRecordType() {
    return recordType;
  }

  @Override
  public void encode(BeamRecord value, OutputStream outStream)
      throws CoderException, IOException {
    nullListCoder.encode(scanNullFields(value), outStream);
    for (int idx = 0; idx < value.size(); ++idx) {
      if (value.getFieldValue(idx) == null) {
        continue;
      }

      coderArray.get(idx).encode(value.getFieldValue(idx), outStream);
    }
  }

  @Override
  public BeamRecord decode(InputStream inStream) throws CoderException, IOException {
    BitSet nullFields = nullListCoder.decode(inStream);

    BeamRecord record = new BeamRecord(recordType);
    for (int idx = 0; idx < recordType.size(); ++idx) {
      if (nullFields.get(idx)) {
        continue;
      }

      record.addField(idx, coderArray.get(idx).decode(inStream));
    }

    return record;
  }

  /**
   * Scan {@link BeamRecord} to find fields with a NULL value.
   */
  private BitSet scanNullFields(BeamRecord record){
    BitSet nullFields = new BitSet(record.size());
    for (int idx = 0; idx < record.size(); ++idx) {
      if (record.getFieldValue(idx) == null) {
        nullFields.set(idx);
      }
    }
    return nullFields;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
    for (Coder c : coderArray) {
      c.verifyDeterministic();
    }
  }
}
