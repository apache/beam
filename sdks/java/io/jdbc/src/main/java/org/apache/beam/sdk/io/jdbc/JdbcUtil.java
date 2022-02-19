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
package org.apache.beam.sdk.io.jdbc;

import java.io.Serializable;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadWithPartitions;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides utility functions for working with {@link JdbcIO}. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class JdbcUtil {

  /** Generates an insert statement based on {@link Schema.Field}. * */
  static String generateStatement(String tableName, List<Schema.Field> fields) {
    String fieldNames =
        IntStream.range(0, fields.size())
            .mapToObj((index) -> fields.get(index).getName())
            .collect(Collectors.joining(", "));

    String valuePlaceholder =
        IntStream.range(0, fields.size())
            .mapToObj((index) -> "?")
            .collect(Collectors.joining(", "));

    return String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, fieldNames, valuePlaceholder);
  }

  /** PreparedStatementSetCaller for Schema Field types. * */
  private static Map<Schema.TypeName, JdbcIO.PreparedStatementSetCaller> typeNamePsSetCallerMap =
      new EnumMap<>(
          ImmutableMap.<Schema.TypeName, JdbcIO.PreparedStatementSetCaller>builder()
              .put(
                  Schema.TypeName.BYTE,
                  (element, ps, i, fieldWithIndex) -> {
                    Byte value = element.getByte(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setByte(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.INT16,
                  (element, ps, i, fieldWithIndex) -> {
                    Short value = element.getInt16(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setInt(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.INT64,
                  (element, ps, i, fieldWithIndex) -> {
                    Long value = element.getInt64(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setLong(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.DECIMAL,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setBigDecimal(i + 1, element.getDecimal(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.FLOAT,
                  (element, ps, i, fieldWithIndex) -> {
                    Float value = element.getFloat(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setFloat(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.DOUBLE,
                  (element, ps, i, fieldWithIndex) -> {
                    Double value = element.getDouble(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setDouble(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.DATETIME,
                  (element, ps, i, fieldWithIndex) -> {
                    ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
                    ps.setTimestamp(i + 1, value == null ? null : new Timestamp(value.getMillis()));
                  })
              .put(
                  Schema.TypeName.BOOLEAN,
                  (element, ps, i, fieldWithIndex) -> {
                    Boolean value = element.getBoolean(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setBoolean(i + 1, value);
                    }
                  })
              .put(Schema.TypeName.BYTES, createBytesCaller())
              .put(
                  Schema.TypeName.INT32,
                  (element, ps, i, fieldWithIndex) -> {
                    Integer value = element.getInt32(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i);
                    } else {
                      ps.setInt(i + 1, value);
                    }
                  })
              .put(Schema.TypeName.STRING, createStringCaller())
              .build());

  /** PreparedStatementSetCaller for Schema Field Logical types. * */
  static JdbcIO.PreparedStatementSetCaller getPreparedStatementSetCaller(
      Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        return (element, ps, i, fieldWithIndex) -> {
          Collection<Object> value = element.getArray(fieldWithIndex.getIndex());
          if (value == null) {
            ps.setArray(i + 1, null);
          } else {
            ps.setArray(
                i + 1,
                ps.getConnection()
                    .createArrayOf(
                        fieldType.getCollectionElementType().getTypeName().name(),
                        value.toArray()));
          }
        };
      case LOGICAL_TYPE:
        {
          if (Objects.equals(
              fieldType.getLogicalType(), LogicalTypes.JDBC_UUID_TYPE.getLogicalType())) {
            return (element, ps, i, fieldWithIndex) ->
                ps.setObject(
                    i + 1, element.getLogicalTypeValue(fieldWithIndex.getIndex(), UUID.class));
          }

          String logicalTypeName = fieldType.getLogicalType().getIdentifier();
          JDBCType jdbcType = JDBCType.valueOf(logicalTypeName);
          switch (jdbcType) {
            case DATE:
              return (element, ps, i, fieldWithIndex) -> {
                ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
                ps.setDate(
                    i + 1,
                    value == null
                        ? null
                        : new Date(
                            getDateOrTimeOnly(value.toDateTime(), true).getTime().getTime()));
              };
            case TIME:
              return (element, ps, i, fieldWithIndex) -> {
                ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
                ps.setTime(
                    i + 1,
                    value == null
                        ? null
                        : new Time(
                            getDateOrTimeOnly(
                                    element.getDateTime(fieldWithIndex.getIndex()).toDateTime(),
                                    false)
                                .getTime()
                                .getTime()));
              };
            case TIMESTAMP_WITH_TIMEZONE:
              return (element, ps, i, fieldWithIndex) -> {
                ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
                if (value == null) {
                  ps.setTimestamp(i + 1, null);
                } else {
                  Calendar calendar = withTimestampAndTimezone(value.toDateTime());
                  ps.setTimestamp(i + 1, new Timestamp(calendar.getTime().getTime()), calendar);
                }
              };
            case OTHER:
              return (element, ps, i, fieldWithIndex) ->
                  ps.setObject(
                      i + 1, element.getValue(fieldWithIndex.getIndex()), java.sql.Types.OTHER);
            default:
              return getPreparedStatementSetCaller(fieldType.getLogicalType().getBaseType());
          }
        }
      default:
        {
          if (typeNamePsSetCallerMap.containsKey(fieldType.getTypeName())) {
            return typeNamePsSetCallerMap.get(fieldType.getTypeName());
          } else {
            throw new RuntimeException(
                fieldType.getTypeName().name()
                    + " in schema is not supported while writing. Please provide statement and preparedStatementSetter");
          }
        }
    }
  }

  static void setNullToPreparedStatement(PreparedStatement ps, int i) throws SQLException {
    ps.setNull(i + 1, JDBCType.NULL.getVendorTypeNumber());
  }

  static class BeamRowPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<Row> {
    @Override
    public void setParameters(Row row, PreparedStatement statement) {
      Schema schema = row.getSchema();
      List<Schema.Field> fieldTypes = schema.getFields();
      IntStream.range(0, fieldTypes.size())
          .forEachOrdered(
              i -> {
                Schema.FieldType type = fieldTypes.get(i).getType();
                try {
                  JdbcUtil.getPreparedStatementSetCaller(type)
                      .set(row, statement, i, SchemaUtil.FieldWithIndex.of(schema.getField(i), i));
                } catch (SQLException throwables) {
                  throwables.printStackTrace();
                  throw new RuntimeException(
                      String.format("Unable to create prepared statement for type: %s", type),
                      throwables);
                }
              });
    }
  }

  private static JdbcIO.PreparedStatementSetCaller createBytesCaller() {
    return (element, ps, i, fieldWithIndex) -> {
      byte[] value = element.getBytes(fieldWithIndex.getIndex());
      if (value != null) {
        validateLogicalTypeLength(fieldWithIndex.getField(), value.length);
      }
      ps.setBytes(i + 1, value);
    };
  }

  private static JdbcIO.PreparedStatementSetCaller createStringCaller() {
    return (element, ps, i, fieldWithIndex) -> {
      String value = element.getString(fieldWithIndex.getIndex());
      if (value != null) {
        validateLogicalTypeLength(fieldWithIndex.getField(), value.length());
      }
      ps.setString(i + 1, value);
    };
  }

  private static void validateLogicalTypeLength(Schema.Field field, Integer length) {
    try {
      if (field.getType().getTypeName().isLogicalType()
          && field.getType().getLogicalType().getArgument() != null) {
        int maxLimit = (Integer) field.getType().getLogicalType().getArgument();
        if (length > maxLimit) {
          throw new RuntimeException(
              String.format(
                  "Length of Schema.Field[%s] data exceeds database column capacity",
                  field.getName()));
        }
      }
    } catch (NumberFormatException e) {
      // if argument is not set or not integer then do nothing and proceed with the insertion
    }
  }

  private static Calendar getDateOrTimeOnly(DateTime dateTime, boolean wantDateOnly) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone(dateTime.getZone().getID()));

    if (wantDateOnly) { // return date only
      cal.set(Calendar.YEAR, dateTime.getYear());
      cal.set(Calendar.MONTH, dateTime.getMonthOfYear() - 1);
      cal.set(Calendar.DATE, dateTime.getDayOfMonth());

      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
    } else { // return time only
      cal.set(Calendar.YEAR, 1970);
      cal.set(Calendar.MONTH, Calendar.JANUARY);
      cal.set(Calendar.DATE, 1);

      cal.set(Calendar.HOUR_OF_DAY, dateTime.getHourOfDay());
      cal.set(Calendar.MINUTE, dateTime.getMinuteOfHour());
      cal.set(Calendar.SECOND, dateTime.getSecondOfMinute());
      cal.set(Calendar.MILLISECOND, dateTime.getMillisOfSecond());
    }

    return cal;
  }

  private static Calendar withTimestampAndTimezone(DateTime dateTime) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(dateTime.getZone().getID()));
    calendar.setTimeInMillis(dateTime.getMillis());

    return calendar;
  }

  /**
   * A helper for {@link ReadWithPartitions} that handles range calculations.
   *
   * @param <PartitionT>
   */
  interface JdbcReadWithPartitionsHelper<PartitionT>
      extends PreparedStatementSetter<StartEndRange<PartitionT>>,
          RowMapper<KV<Long, KV<PartitionT, PartitionT>>> {
    static <T> JdbcReadWithPartitionsHelper<T> getPartitionsHelper(TypeDescriptor<T> type) {
      // This cast is unchecked, thus this is a small type-checking risk. We just need
      // to make sure that all preset helpers in `JdbcUtil.PRESET_HELPERS` are matched
      // in type from their Key and their Value.
      return (JdbcReadWithPartitionsHelper<T>) PRESET_HELPERS.get(type.getRawType());
    }

    Iterable<StartEndRange<PartitionT>> calculateRanges(
        PartitionT lowerBound, PartitionT upperBound, Long partitions, String columnName);

    @Override
    void setParameters(StartEndRange<PartitionT> element, PreparedStatement preparedStatement);

    @Override
    KV<Long, KV<PartitionT, PartitionT>> mapRow(ResultSet resultSet) throws Exception;
  }

  public static class StartEndRange<T> implements Serializable {
    private final T start;
    private final T end;
    private final TypeDescriptor<T> typeDescriptor;
    private final String columnName;

    StartEndRange(T start, T end, TypeDescriptor<T> typeDescriptor, String columnName) {
      this.start = start;
      this.end = end;
      this.typeDescriptor = typeDescriptor;
      this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StartEndRange)) {
        return false;
      }
      StartEndRange<?> that = (StartEndRange<?>) o;
      return Objects.equals(start, that.start)
          && Objects.equals(end, that.end)
          && Objects.equals(typeDescriptor, that.typeDescriptor)
          && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, end, typeDescriptor, columnName);
    }

    @Override
    public String toString() {
      return "StartEndRange{"
          + "start="
          + start
          + ", end="
          + end
          + ", typeDescriptor="
          + typeDescriptor
          + ", columnName='"
          + columnName
          + '\''
          + '}';
    }

    T start() {
      return start;
    }

    T end() {
      return end;
    }

    String columnName() {
      return columnName;
    }

    TypeDescriptor<T> type() {
      return typeDescriptor;
    }

    KV<StartEndRange<T>, StartEndRange<T>> split(double fraction) {
      List<StartEndRange<T>> ranges =
          Lists.newArrayList(
              ((JdbcReadWithPartitionsHelper<T>) PRESET_HELPERS.get(type().getRawType()))
                  .calculateRanges(start(), end(), 2L, columnName));
      return KV.of(
          new StartEndRange<T>(
              ranges.get(0).start(),
              ranges.get(0).end(),
              typeDescriptor,
              ranges.get(0).columnName()),
          new StartEndRange<T>(
              ranges.get(1).start(),
              ranges.get(ranges.size() - 1).end(),
              typeDescriptor,
              ranges.get(1).columnName()));
    }
  }

  /** Create partitions on a table. */
  static class PartitioningFn<T> extends DoFn<KV<Long, KV<T, T>>, StartEndRange<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitioningFn.class);
    final TypeDescriptor<T> partitioningColumnType;
    final String partitioningColumnName;

    PartitioningFn(TypeDescriptor<T> partitioningColumnType, String partitioningColumnName) {
      this.partitioningColumnType = partitioningColumnType;
      this.partitioningColumnName = partitioningColumnName;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      T lowerBound = c.element().getValue().getKey();
      T upperBound = c.element().getValue().getValue();
      JdbcReadWithPartitionsHelper<T> helper =
          JdbcReadWithPartitionsHelper.getPartitionsHelper(partitioningColumnType);
      if (c.element().getKey() == 1L) {
        c.output(
            new StartEndRange<T>(
                c.element().getValue().getKey(),
                c.element().getValue().getValue(),
                partitioningColumnType,
                partitioningColumnName));
        return;
      }
      List<StartEndRange<T>> ranges =
          Lists.newArrayList(
              helper.calculateRanges(
                  lowerBound, upperBound, c.element().getKey(), partitioningColumnName));
      LOG.info(
          "Partitioned range {} into a total of {} ranges: {}",
          c.element().getValue(),
          ranges.size(),
          ranges);
      for (StartEndRange<T> e : ranges) {
        c.output(e);
      }
    }
  }

  public static final Map<Class<?>, JdbcReadWithPartitionsHelper<?>> PRESET_HELPERS =
      ImmutableMap.of(
          Long.class,
          new JdbcReadWithPartitionsHelper<Long>() {
            @Override
            public Iterable<StartEndRange<Long>> calculateRanges(
                Long lowerBound, Long upperBound, Long partitions, String columnName) {
              List<StartEndRange<Long>> ranges = new ArrayList<>();
              // We divide by partitions FIRST to make sure that we can cover the whole LONG range.
              // If we substract first, then we may end up with Long.MAX - Long.MIN, which is 2*MAX,
              // and we'd have trouble with the pipeline.
              long stride = (upperBound / partitions - lowerBound / partitions) + 1;
              long highest = lowerBound;
              for (long i = lowerBound; i < upperBound - stride; i += stride) {
                ranges.add(
                    new StartEndRange<Long>(i, i + stride, TypeDescriptors.longs(), columnName));
                highest = i + stride;
              }
              if (highest < upperBound + 1) {
                ranges.add(
                    new StartEndRange<Long>(
                        highest, upperBound, TypeDescriptors.longs(), columnName));
              }
              return ranges;
            }

            @Override
            public void setParameters(
                StartEndRange<Long> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setLong(1, element.start());
                preparedStatement.setLong(2, element.end());
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public KV<Long, KV<Long, Long>> mapRow(ResultSet resultSet) throws Exception {
              if (resultSet.getMetaData().getColumnCount() == 3) {
                return KV.of(
                    resultSet.getLong(3), KV.of(resultSet.getLong(1), resultSet.getLong(2) + 1));
              } else {
                return KV.of(0L, KV.of(resultSet.getLong(1), resultSet.getLong(2) + 1));
              }
            }
          },
          DateTime.class,
          new JdbcReadWithPartitionsHelper<DateTime>() {
            @Override
            public Iterable<StartEndRange<DateTime>> calculateRanges(
                DateTime lowerBound, DateTime upperBound, Long partitions, String columnName) {
              final List<StartEndRange<DateTime>> result = new ArrayList<>();

              final long intervalMillis = upperBound.getMillis() - lowerBound.getMillis();
              final Duration stride = Duration.millis(Math.max(1, intervalMillis / partitions));
              // Add the first advancement
              DateTime currentLowerBound = lowerBound;
              // Zero output in a comparison means that elements are equal
              while (currentLowerBound.compareTo(upperBound) <= 0) {
                DateTime currentUpper = currentLowerBound.plus(stride);
                if (currentUpper.compareTo(upperBound) >= 0) {
                  // If we hit the upper bound directly, then we want to be just-above it, so that
                  // it will be captured by the less-than query.
                  currentUpper = upperBound.plusMillis(1);
                  result.add(
                      new StartEndRange<DateTime>(
                          currentLowerBound,
                          currentUpper,
                          TypeDescriptor.of(DateTime.class),
                          columnName));
                  return result;
                }
                result.add(
                    new StartEndRange<DateTime>(
                        currentLowerBound,
                        currentUpper,
                        TypeDescriptor.of(DateTime.class),
                        columnName));
                currentLowerBound = currentLowerBound.plus(stride);
              }
              return result;
            }

            @Override
            public void setParameters(
                StartEndRange<DateTime> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setTimestamp(1, new Timestamp(element.start().getMillis()));
                preparedStatement.setTimestamp(2, new Timestamp(element.end().getMillis()));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public KV<Long, KV<DateTime, DateTime>> mapRow(ResultSet resultSet) throws Exception {
              if (resultSet.getMetaData().getColumnCount() == 3) {
                return KV.of(
                    resultSet.getLong(3),
                    KV.of(
                        new DateTime(resultSet.getTimestamp(1)),
                        new DateTime(resultSet.getTimestamp(2).getTime() + 1)));
              } else {
                return KV.of(
                    0L,
                    KV.of(
                        new DateTime(resultSet.getTimestamp(1)),
                        new DateTime(resultSet.getTimestamp(2).getTime() + 1)));
              }
            }
          });
}
