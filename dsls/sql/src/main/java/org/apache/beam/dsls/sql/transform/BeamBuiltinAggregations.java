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
package org.apache.beam.dsls.sql.transform;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlUdaf;
import org.apache.beam.dsls.sql.utils.CalciteUtils;

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG.
 */
class BeamBuiltinAggregations {
  /**
   * Built-in aggregation for COUNT.
   */
  public static class Count<T> extends BeamSqlUdaf<T, Long> {
    private BeamSqlRecordType accType;

    public Count() {
      accType = BeamSqlRecordType.create(Arrays.asList("__count"), Arrays.asList(Types.BIGINT));
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(0L));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType, Arrays.<Object>asList(accumulator.getLong(0) + 1));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      long v = 0L;
      while (accumulators.iterator().hasNext()) {
        v += accumulators.iterator().next().getLong(0);
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v));
    }

    @Override
    public Long result(BeamSqlRow accumulator) {
      return accumulator.getLong(0);
    }
  }

  /**
   * Built-in aggregation for MAX.
   */
  public static class Max<T extends Comparable<T>> extends BeamSqlUdaf<T, T> {
    private BeamSqlRecordType accType;

    public Max(int outputFieldType) {
      this.accType = BeamSqlRecordType.create(Arrays.asList("__max"),
          Arrays.asList(outputFieldType));
    }

    @Override
    public BeamSqlRow init() {
      return null;
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays
              .<Object>asList((accumulator == null || ((Comparable<T>) accumulator.getFieldValue(0))
                  .compareTo(input) < 0)
                      ? input : accumulator.getFieldValue(0)));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      T mergedV = (T) accumulators.iterator().next().getFieldValue(0);
      while (accumulators.iterator().hasNext()) {
        T v = (T) accumulators.iterator().next().getFieldValue(0);
        mergedV = mergedV.compareTo(v) > 0 ? mergedV : v;
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(mergedV));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      return (T) accumulator.getFieldValue(0);
    }
  }

  /**
   * Built-in aggregation for MIN.
   */
  public static class Min<T extends Comparable<T>> extends BeamSqlUdaf<T, T> {
    private BeamSqlRecordType accType;

    public Min(int outputFieldType) {
      this.accType = BeamSqlRecordType.create(Arrays.asList("__min"),
          Arrays.asList(outputFieldType));
    }

    @Override
    public BeamSqlRow init() {
      return null;
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays
              .<Object>asList((accumulator == null || ((Comparable<T>) accumulator.getFieldValue(0))
                  .compareTo(input) > 0)
                      ? input : accumulator.getFieldValue(0)));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      T mergedV = (T) accumulators.iterator().next().getFieldValue(0);
      while (accumulators.iterator().hasNext()) {
        T v = (T) accumulators.iterator().next().getFieldValue(0);
        mergedV = mergedV.compareTo(v) < 0 ? mergedV : v;
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(mergedV));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      return (T) accumulator.getFieldValue(0);
    }
  }

  /**
   * Built-in aggregation for SUM.
   */
  public static class Sum<T> extends BeamSqlUdaf<T, T> {
    private static List<Integer> supportedType = Arrays.asList(Types.INTEGER,
        Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DOUBLE,
        Types.FLOAT, Types.DECIMAL);

    private int outputFieldType;
    private BeamSqlRecordType accType;
    public Sum(int outputFieldType) {
      //check input data type is supported
      if (!supportedType.contains(outputFieldType)) {
        throw new UnsupportedOperationException(String.format(
            "data type [%s] is not supported in SUM", CalciteUtils.toCalciteType(outputFieldType)));
      }

      this.outputFieldType = outputFieldType;
      this.accType = BeamSqlRecordType.create(Arrays.asList("__sum"),
          Arrays.asList(Types.DECIMAL)); //by default use DOUBLE to store the value.
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(new BigDecimal(0)));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType, Arrays.<Object>asList(accumulator.getBigDecimal(0)
          .add(new BigDecimal(input.toString()))));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      BigDecimal v = new BigDecimal(0);
      while (accumulators.iterator().hasNext()) {
        v.add(accumulators.iterator().next().getBigDecimal(0));
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      Object result = null;
      switch (outputFieldType) {
      case Types.INTEGER:
        result = accumulator.getBigDecimal(0).intValue();
        break;
      case Types.BIGINT:
        result = accumulator.getBigDecimal(0).longValue();
        break;
      case Types.SMALLINT:
        result = accumulator.getBigDecimal(0).shortValue();
        break;
      case Types.TINYINT:
        result = accumulator.getBigDecimal(0).byteValue();
        break;
      case Types.DOUBLE:
        result = accumulator.getBigDecimal(0).doubleValue();
        break;
      case Types.FLOAT:
        result = accumulator.getBigDecimal(0).floatValue();
        break;
      case Types.DECIMAL:
        result = accumulator.getBigDecimal(0);
        break;
      default:
        break;
      }
      return (T) result;
    }

  }

  /**
   * Built-in aggregation for AVG.
   */
  public static class Avg<T> extends BeamSqlUdaf<T, T> {
    private static List<Integer> supportedType = Arrays.asList(Types.INTEGER,
        Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DOUBLE,
        Types.FLOAT, Types.DECIMAL);

    private int outputFieldType;
    private BeamSqlRecordType accType;
    public Avg(int outputFieldType) {
      //check input data type is supported
      if (!supportedType.contains(outputFieldType)) {
        throw new UnsupportedOperationException(String.format(
            "data type [%s] is not supported in AVG", CalciteUtils.toCalciteType(outputFieldType)));
      }

      this.outputFieldType = outputFieldType;
      this.accType = BeamSqlRecordType.create(Arrays.asList("__sum", "size"),
          Arrays.asList(Types.DECIMAL, Types.BIGINT)); //by default use DOUBLE to store the value.
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(new BigDecimal(0), 0L));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays.<Object>asList(
              accumulator.getBigDecimal(0).add(new BigDecimal(input.toString())),
              accumulator.getLong(1) + 1));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      BigDecimal v = new BigDecimal(0);
      long s = 0;
      while (accumulators.iterator().hasNext()) {
        BeamSqlRow r = accumulators.iterator().next();
        v.add(r.getBigDecimal(0));
        s += r.getLong(1);
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v, s));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      Object result = null;
      BigDecimal decimalAvg = accumulator.getBigDecimal(0).divide(
          new BigDecimal(accumulator.getLong(1)));
      switch (outputFieldType) {
      case Types.INTEGER:
        result = decimalAvg.intValue();
        break;
      case Types.BIGINT:
        result = decimalAvg.longValue();
        break;
      case Types.SMALLINT:
        result = decimalAvg.shortValue();
        break;
      case Types.TINYINT:
        result = decimalAvg.byteValue();
        break;
      case Types.DOUBLE:
        result = decimalAvg.doubleValue();
        break;
      case Types.FLOAT:
        result = decimalAvg.floatValue();
        break;
      case Types.DECIMAL:
        result = decimalAvg;
        break;
      default:
        break;
      }
      return (T) result;
    }

  }

}
