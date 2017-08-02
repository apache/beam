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
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordTypeProvider;

/**
 *  A {@link Coder} for {@link BeamRecord}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class BeamRecordCoder extends CustomCoder<BeamRecord> {
  private static final ListCoder<Integer> nullListCoder = ListCoder.of(BigEndianIntegerCoder.of());
  private static final InstantCoder instantCoder = InstantCoder.of();

  private BeamRecordTypeProvider recordType;
  private List<Coder> coderArray;

  public BeamRecordCoder(BeamRecordTypeProvider recordType, List<Coder> coderArray) {
    this.recordType = recordType;
    this.coderArray = coderArray;
  }

  @Override
  public void encode(BeamRecord value, OutputStream outStream)
      throws CoderException, IOException {
    nullListCoder.encode(value.getNullFields(), outStream);
    for (int idx = 0; idx < value.size(); ++idx) {
      if (value.getNullFields().contains(idx)) {
        continue;
      }

      coderArray.get(idx).encode(value.getInteger(idx), outStream);
    }

    instantCoder.encode(value.getWindowStart(), outStream);
    instantCoder.encode(value.getWindowEnd(), outStream);
  }

  @Override
  public BeamRecord decode(InputStream inStream) throws CoderException, IOException {
    List<Integer> nullFields = nullListCoder.decode(inStream);

    BeamRecord record = new BeamRecord(recordType);
    record.setNullFields(nullFields);
    for (int idx = 0; idx < recordType.size(); ++idx) {
      if (nullFields.contains(idx)) {
        continue;
      }

      record.addField(idx, coderArray.get(idx).decode(inStream));
    }

    record.setWindowStart(instantCoder.decode(inStream));
    record.setWindowEnd(instantCoder.decode(inStream));

    return record;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
  }
}
