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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * A {@link Coder} encodes {@link BeamRecord}.
 */
@Experimental
public class BeamSqlRecordHelper {

  public static BeamSqlRecordType getSqlRecordType(BeamRecord record) {
    return (BeamSqlRecordType) record.getDataType();
  }

  public static BeamRecordCoder getSqlRecordCoder(BeamSqlRecordType recordType) {
    List<Coder> fieldCoders = new ArrayList<>();
    for (int idx = 0; idx < recordType.size(); ++idx) {
      switch (recordType.getFieldsType().get(idx)) {
      case Types.INTEGER:
        fieldCoders.add(BigEndianIntegerCoder.of());
        break;
      case Types.SMALLINT:
        fieldCoders.add(ShortCoder.of());
        break;
      case Types.TINYINT:
        fieldCoders.add(ByteCoder.of());
        break;
      case Types.DOUBLE:
        fieldCoders.add(DoubleCoder.of());
        break;
      case Types.FLOAT:
        fieldCoders.add(FloatCoder.of());
        break;
      case Types.DECIMAL:
        fieldCoders.add(BigDecimalCoder.of());
        break;
      case Types.BIGINT:
        fieldCoders.add(BigEndianLongCoder.of());
        break;
      case Types.VARCHAR:
      case Types.CHAR:
        fieldCoders.add(StringUtf8Coder.of());
        break;
      case Types.TIME:
        fieldCoders.add(TimeCoder.of());
        break;
      case Types.DATE:
      case Types.TIMESTAMP:
        fieldCoders.add(DateCoder.of());
        break;
      case Types.BOOLEAN:
        fieldCoders.add(BooleanCoder.of());
        break;

      default:
        throw new UnsupportedOperationException(
            "Data type: " + recordType.getFieldsType().get(idx) + " not supported yet!");
      }
    }
    return BeamRecordCoder.of(recordType, fieldCoders);
  }

  /**
   * {@link Coder} for Java type {@link Short}.
   */
  public static class ShortCoder extends CustomCoder<Short> {
    private static final ShortCoder INSTANCE = new ShortCoder();

    public static ShortCoder of() {
      return INSTANCE;
    }

    private ShortCoder() {
    }

    @Override
    public void encode(Short value, OutputStream outStream) throws CoderException, IOException {
      new DataOutputStream(outStream).writeShort(value);
    }

    @Override
    public Short decode(InputStream inStream) throws CoderException, IOException {
      return new DataInputStream(inStream).readShort();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }
  /**
   * {@link Coder} for Java type {@link Float}.
   */
  public static class FloatCoder extends CustomCoder<Float> {
    private static final FloatCoder INSTANCE = new FloatCoder();

    public static FloatCoder of() {
      return INSTANCE;
    }

    private FloatCoder() {
    }

    @Override
    public void encode(Float value, OutputStream outStream) throws CoderException, IOException {
      new DataOutputStream(outStream).writeFloat(value);
    }

    @Override
    public Float decode(InputStream inStream) throws CoderException, IOException {
      return new DataInputStream(inStream).readFloat();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }

  /**
   * {@link Coder} for Java type {@link GregorianCalendar}, it's stored as {@link Long}.
   */
  public static class TimeCoder extends CustomCoder<GregorianCalendar> {
    private static final BigEndianLongCoder longCoder = BigEndianLongCoder.of();
    private static final TimeCoder INSTANCE = new TimeCoder();

    public static TimeCoder of() {
      return INSTANCE;
    }

    private TimeCoder() {
    }

    @Override
    public void encode(GregorianCalendar value, OutputStream outStream)
        throws CoderException, IOException {
      longCoder.encode(value.getTime().getTime(), outStream);
    }

    @Override
    public GregorianCalendar decode(InputStream inStream) throws CoderException, IOException {
      GregorianCalendar calendar = new GregorianCalendar();
      calendar.setTime(new Date(longCoder.decode(inStream)));
      return calendar;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }
  /**
   * {@link Coder} for Java type {@link Date}, it's stored as {@link Long}.
   */
  public static class DateCoder extends CustomCoder<Date> {
    private static final BigEndianLongCoder longCoder = BigEndianLongCoder.of();
    private static final DateCoder INSTANCE = new DateCoder();

    public static DateCoder of() {
      return INSTANCE;
    }

    private DateCoder() {
    }

    @Override
    public void encode(Date value, OutputStream outStream) throws CoderException, IOException {
      longCoder.encode(value.getTime(), outStream);
    }

    @Override
    public Date decode(InputStream inStream) throws CoderException, IOException {
      return new Date(longCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }

  /**
   * {@link Coder} for Java type {@link Boolean}.
   */
  public static class BooleanCoder extends CustomCoder<Boolean> {
    private static final BooleanCoder INSTANCE = new BooleanCoder();

    public static BooleanCoder of() {
      return INSTANCE;
    }

    private BooleanCoder() {
    }

    @Override
    public void encode(Boolean value, OutputStream outStream) throws CoderException, IOException {
      new DataOutputStream(outStream).writeBoolean(value);
    }

    @Override
    public Boolean decode(InputStream inStream) throws CoderException, IOException {
      return new DataInputStream(inStream).readBoolean();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }
}
