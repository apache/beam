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
package org.apache.beam.dsls.sql.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A {@link Coder} for {@link BeamSQLRecordType}.
 *
 */
public class BeamSQLRecordTypeCoder extends CustomCoder<BeamSQLRecordType> {
  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();

  private static final BeamSQLRecordTypeCoder INSTANCE = new BeamSQLRecordTypeCoder();
  private BeamSQLRecordTypeCoder(){}

  public static BeamSQLRecordTypeCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BeamSQLRecordType value, OutputStream outStream)
      throws CoderException, IOException {
    intCoder.encode(value.size(), outStream);
    for (String fieldName : value.getFieldsName()) {
      stringCoder.encode(fieldName, outStream);
    }
    for (SqlTypeName fieldType : value.getFieldsType()) {
      stringCoder.encode(fieldType.name(), outStream);
    }
    //add a dummy field to indicate the end of record
    intCoder.encode(value.size(), outStream);
  }

  @Override
  public BeamSQLRecordType decode(InputStream inStream) throws CoderException, IOException {
    BeamSQLRecordType typeRecord = new BeamSQLRecordType();
    int size = intCoder.decode(inStream);
    for (int idx = 0; idx < size; ++idx) {
      typeRecord.getFieldsName().add(stringCoder.decode(inStream));
    }
    for (int idx = 0; idx < size; ++idx) {
      typeRecord.getFieldsType().add(
          SqlTypeName.valueOf(stringCoder.decode(inStream)));
    }
    intCoder.decode(inStream);
    return typeRecord;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
  }

}
