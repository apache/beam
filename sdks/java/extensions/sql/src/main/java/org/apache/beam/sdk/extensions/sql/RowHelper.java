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
package org.apache.beam.sdk.extensions.sql;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.values.Row;

/**
 * Atomic {@link Coder}s for {@link Row} fields for SQL types.
 */
@Experimental
public class RowHelper {

  // TODO: WHy not use standard types?

  /**
   * {@link Coder} for Java type {@link Short}.
   */
  public static class ShortCoder extends AtomicCoder<Short> {
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
  }

  /**
   * {@link Coder} for Java type {@link Float}, it's stored as {@link BigDecimal}.
   */
  public static class FloatCoder extends AtomicCoder<Float> {
    private static final FloatCoder INSTANCE = new FloatCoder();
    private static final BigDecimalCoder CODER = BigDecimalCoder.of();

    public static FloatCoder of() {
      return INSTANCE;
    }

    private FloatCoder() {
    }

    @Override
    public void encode(Float value, OutputStream outStream) throws CoderException, IOException {
      CODER.encode(new BigDecimal(value), outStream);
    }

    @Override
    public Float decode(InputStream inStream) throws CoderException, IOException {
      return CODER.decode(inStream).floatValue();
    }
  }

  /**
   * {@link Coder} for Java type {@link Double}, it's stored as {@link BigDecimal}.
   */
  public static class DoubleCoder extends AtomicCoder<Double> {
    private static final DoubleCoder INSTANCE = new DoubleCoder();
    private static final BigDecimalCoder CODER = BigDecimalCoder.of();

    public static DoubleCoder of() {
      return INSTANCE;
    }

    private DoubleCoder() {
    }

    @Override
    public void encode(Double value, OutputStream outStream) throws CoderException, IOException {
      CODER.encode(new BigDecimal(value), outStream);
    }

    @Override
    public Double decode(InputStream inStream) throws CoderException, IOException {
      return CODER.decode(inStream).doubleValue();
    }
  }

  /**
   * {@link Coder} for Java type {@link GregorianCalendar}, it's stored as {@link Long}.
   */
  public static class TimeCoder extends AtomicCoder<GregorianCalendar> {
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
  }

  /**
   * {@link Coder} for Java type {@link Date}, it's stored as {@link Long}.
   */
  public static class DateCoder extends AtomicCoder<Date> {
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
  }

  /**
   * {@link Coder} for Java type {@link Boolean}.
   */
  public static class BooleanCoder extends AtomicCoder<Boolean> {
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
  }
}
