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
package org.beam.dsls.sql.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * A {@link Coder} encodes {@link BeamSQLRow}.
 *
 */
public class BeamSqlRowCoder extends StandardCoder<BeamSQLRow>{
  private static final BeamSQLRecordTypeCoder recordTypeCoder = BeamSQLRecordTypeCoder.of();

  private static final ListCoder<Integer> listCoder = ListCoder.of(BigEndianIntegerCoder.of());

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final BigEndianIntegerCoder intCoder = BigEndianIntegerCoder.of();
  private static final BigEndianLongCoder longCoder = BigEndianLongCoder.of();
  private static final DoubleCoder doubleCoder = DoubleCoder.of();

  private static final BeamSqlRowCoder INSTANCE = new BeamSqlRowCoder();
  private BeamSqlRowCoder(){}

  public static BeamSqlRowCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BeamSQLRow value, OutputStream outStream,
      org.apache.beam.sdk.coders.Coder.Context context) throws CoderException, IOException {
    recordTypeCoder.encode(value.getDataType(), outStream, context);
    listCoder.encode(value.getNullFields(), outStream, context);

    Context nested = context.nested();

    for (int idx = 0; idx < value.size(); ++idx) {
      if(value.getNullFields().contains(idx)){
        continue;
      }

      switch (value.getDataType().getFieldsType().get(idx)) {
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        intCoder.encode(value.getInteger(idx), outStream, nested);
        break;
      case DOUBLE:
      case FLOAT:
        doubleCoder.encode(value.getDouble(idx), outStream, nested);
        break;
      case BIGINT:
        longCoder.encode(value.getLong(idx), outStream, nested);
        break;
      case VARCHAR:
        stringCoder.encode(value.getString(idx), outStream, nested);
        break;
      case TIME:
      case TIMESTAMP:
        longCoder.encode(value.getDate(idx).getTime(), outStream, nested);
        break;

      default:
        throw new UnsupportedDataTypeException(value.getDataType().getFieldsType().get(idx));
      }
    }
  }

  @Override
  public BeamSQLRow decode(InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
      throws CoderException, IOException {
    BeamSQLRecordType type = recordTypeCoder.decode(inStream, context);
    List<Integer> nullFields = listCoder.decode(inStream, context);

    BeamSQLRow record = new BeamSQLRow(type);
    record.setNullFields(nullFields);

    for (int idx = 0; idx < type.size(); ++idx) {
      if(nullFields.contains(idx)){
        continue;
      }

      switch (type.getFieldsType().get(idx)) {
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        record.addField(idx, intCoder.decode(inStream, context));
        break;
      case DOUBLE:
      case FLOAT:
        record.addField(idx, doubleCoder.decode(inStream, context));
        break;
      case BIGINT:
        record.addField(idx, longCoder.decode(inStream, context));
        break;
      case VARCHAR:
        record.addField(idx, stringCoder.decode(inStream, context));
        break;
      case TIME:
      case TIMESTAMP:
        record.addField(idx, new Date(longCoder.decode(inStream, context)));
        break;

      default:
        throw new UnsupportedDataTypeException(type.getFieldsType().get(idx));
      }
    }

    return record;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {

  }

}
