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
package org.apache.beam.sdk.extensions.sql.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 *  A {@link Coder} encodes {@link BeamSqlRow}.
 */
public class BeamSqlRowCoder extends CustomCoder<BeamSqlRow> {
  private BeamSqlRowType sqlRecordType;

  private static final ListCoder<Integer> listCoder = ListCoder.of(BigEndianIntegerCoder.of());

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final BigEndianIntegerCoder intCoder = BigEndianIntegerCoder.of();
  private static final BigEndianLongCoder longCoder = BigEndianLongCoder.of();
  private static final DoubleCoder doubleCoder = DoubleCoder.of();
  private static final InstantCoder instantCoder = InstantCoder.of();
  private static final BigDecimalCoder bigDecimalCoder = BigDecimalCoder.of();
  private static final ByteCoder byteCoder = ByteCoder.of();

  public BeamSqlRowCoder(BeamSqlRowType sqlRecordType) {
    this.sqlRecordType = sqlRecordType;
  }

  @Override
  public void encode(BeamSqlRow value, OutputStream outStream)
      throws CoderException, IOException {
    listCoder.encode(value.getNullFields(), outStream);
    for (int idx = 0; idx < value.size(); ++idx) {
      if (value.getNullFields().contains(idx)) {
        continue;
      }

      switch (sqlRecordType.getFieldsType().get(idx)) {
        case Types.INTEGER:
          intCoder.encode(value.getInteger(idx), outStream);
          break;
        case Types.SMALLINT:
          intCoder.encode((int) value.getShort(idx), outStream);
          break;
        case Types.TINYINT:
          byteCoder.encode(value.getByte(idx), outStream);
          break;
        case Types.DOUBLE:
          doubleCoder.encode(value.getDouble(idx), outStream);
          break;
        case Types.FLOAT:
          doubleCoder.encode((double) value.getFloat(idx), outStream);
          break;
        case Types.DECIMAL:
          bigDecimalCoder.encode(value.getBigDecimal(idx), outStream);
          break;
        case Types.BIGINT:
          longCoder.encode(value.getLong(idx), outStream);
          break;
        case Types.VARCHAR:
        case Types.CHAR:
          stringCoder.encode(value.getString(idx), outStream);
          break;
        case Types.TIME:
          longCoder.encode(value.getGregorianCalendar(idx).getTime().getTime(), outStream);
          break;
        case Types.DATE:
        case Types.TIMESTAMP:
          longCoder.encode(value.getDate(idx).getTime(), outStream);
          break;
        case Types.BOOLEAN:
          byteCoder.encode((byte) (value.getBoolean(idx) ? 1 : 0), outStream);
          break;

        default:
          throw new UnsupportedOperationException(
              "Data type: " + sqlRecordType.getFieldsType().get(idx) + " not supported yet!");
      }
    }

    instantCoder.encode(value.getWindowStart(), outStream);
    instantCoder.encode(value.getWindowEnd(), outStream);
  }

  @Override
  public BeamSqlRow decode(InputStream inStream) throws CoderException, IOException {
    List<Integer> nullFields = listCoder.decode(inStream);

    BeamSqlRow record = new BeamSqlRow(sqlRecordType);
    record.setNullFields(nullFields);
    for (int idx = 0; idx < sqlRecordType.size(); ++idx) {
      if (nullFields.contains(idx)) {
        continue;
      }

      switch (sqlRecordType.getFieldsType().get(idx)) {
        case Types.INTEGER:
          record.addField(idx, intCoder.decode(inStream));
          break;
        case Types.SMALLINT:
          record.addField(idx, intCoder.decode(inStream).shortValue());
          break;
        case Types.TINYINT:
          record.addField(idx, byteCoder.decode(inStream));
          break;
        case Types.DOUBLE:
          record.addField(idx, doubleCoder.decode(inStream));
          break;
        case Types.FLOAT:
          record.addField(idx, doubleCoder.decode(inStream).floatValue());
          break;
        case Types.BIGINT:
          record.addField(idx, longCoder.decode(inStream));
          break;
        case Types.DECIMAL:
          record.addField(idx, bigDecimalCoder.decode(inStream));
          break;
        case Types.VARCHAR:
        case Types.CHAR:
          record.addField(idx, stringCoder.decode(inStream));
          break;
        case Types.TIME:
          GregorianCalendar calendar = new GregorianCalendar();
          calendar.setTime(new Date(longCoder.decode(inStream)));
          record.addField(idx, calendar);
          break;
        case Types.DATE:
        case Types.TIMESTAMP:
          record.addField(idx, new Date(longCoder.decode(inStream)));
          break;
        case Types.BOOLEAN:
          record.addField(idx, byteCoder.decode(inStream) == 1);
          break;

        default:
          throw new UnsupportedOperationException("Data type: "
              + sqlRecordType.getFieldsType().get(idx)
              + " not supported yet!");
      }
    }

    record.setWindowStart(instantCoder.decode(inStream));
    record.setWindowEnd(instantCoder.decode(inStream));

    return record;
  }

  public BeamSqlRowType getSqlRecordType() {
    return sqlRecordType;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
  }
}
