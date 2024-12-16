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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.value.AutoValue;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides utility functions for working with {@link JdbcIO}. */
class JdbcUtil {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

  static final Map<String, String> JDBC_DRIVER_MAP =
      new HashMap<>(
          ImmutableMap.of(
              "mysql",
              "com.mysql.cj.jdbc.Driver",
              "postgres",
              "org.postgresql.Driver",
              "oracle",
              "oracle.jdbc.driver.OracleDriver",
              "mssql",
              "com.microsoft.sqlserver.jdbc.SQLServerDriver"));

  @VisibleForTesting
  static void registerJdbcDriver(Map<String, String> jdbcType) {
    JDBC_DRIVER_MAP.putAll(jdbcType);
  }

  /** Utility method to save jar files locally in the worker. */
  static URL[] saveFilesLocally(String driverJars) {
    List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(driverJars);

    final String destRoot = Files.createTempDir().getAbsolutePath();
    List<URL> driverJarUrls = new ArrayList<>();
    listOfJarPaths.stream()
        .forEach(
            jarPath -> {
              try {
                ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                @SuppressWarnings("nullness")
                File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                ResourceId destResourceId =
                    FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                copy(sourceResourceId, destResourceId);
                LOG.info("Localized jar: " + sourceResourceId + " to: " + destResourceId);
                driverJarUrls.add(destFile.toURI().toURL());
              } catch (IOException e) {
                LOG.warn("Unable to copy " + jarPath, e);
              }
            });
    return driverJarUrls.stream().toArray(URL[]::new);
  }

  /** utility method to copy binary (jar file) data from source to dest. */
  private static void copy(ResourceId source, ResourceId dest) throws IOException {
    try (ReadableByteChannel rbc = FileSystems.open(source)) {
      try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
        ByteStreams.copy(rbc, wbc);
      }
    }
  }

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
                      setNullToPreparedStatement(ps, i, JDBCType.TINYINT);
                    } else {
                      ps.setByte(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.INT16,
                  (element, ps, i, fieldWithIndex) -> {
                    Short value = element.getInt16(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i, JDBCType.SMALLINT);
                    } else {
                      ps.setInt(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.INT64,
                  (element, ps, i, fieldWithIndex) -> {
                    Long value = element.getInt64(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i, JDBCType.BIGINT);
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
                      setNullToPreparedStatement(ps, i, JDBCType.FLOAT);
                    } else {
                      ps.setFloat(i + 1, value);
                    }
                  })
              .put(
                  Schema.TypeName.DOUBLE,
                  (element, ps, i, fieldWithIndex) -> {
                    Double value = element.getDouble(fieldWithIndex.getIndex());
                    if (value == null) {
                      setNullToPreparedStatement(ps, i, JDBCType.DOUBLE);
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
                      setNullToPreparedStatement(ps, i, JDBCType.BOOLEAN);
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
                      setNullToPreparedStatement(ps, i, JDBCType.INTEGER);
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
            setArrayNull(ps, i);
          } else {
            Schema.FieldType collectionElementType =
                Preconditions.checkArgumentNotNull(fieldType.getCollectionElementType());
            ps.setArray(
                i + 1,
                ps.getConnection()
                    .createArrayOf(collectionElementType.getTypeName().name(), value.toArray()));
          }
        };
      case LOGICAL_TYPE:
        {
          Schema.LogicalType<?, ?> logicalType = checkArgumentNotNull(fieldType.getLogicalType());
          if (Objects.equals(logicalType, LogicalTypes.JDBC_UUID_TYPE.getLogicalType())) {
            return (element, ps, i, fieldWithIndex) ->
                ps.setObject(
                    i + 1, element.getLogicalTypeValue(fieldWithIndex.getIndex(), UUID.class));
          }

          String logicalTypeName = logicalType.getIdentifier();

          // Special case of Timestamp and Numeric which are logical types in Portable framework
          // but have their own fieldType in Java.
          if (logicalTypeName.equals(MicrosInstant.IDENTIFIER)) {
            // Process timestamp of MicrosInstant kind, which should only be passed from other type
            // systems such as SQL and other Beam SDKs.
            return (element, ps, i, fieldWithIndex) -> {
              // MicrosInstant uses native java.time.Instant instead of joda.Instant.
              java.time.Instant value =
                  element.getLogicalTypeValue(fieldWithIndex.getIndex(), java.time.Instant.class);
              ps.setTimestamp(i + 1, value == null ? null : new Timestamp(value.toEpochMilli()));
            };
          } else if (logicalTypeName.equals(FixedPrecisionNumeric.IDENTIFIER)) {
            return (element, ps, i, fieldWithIndex) -> {
              ps.setBigDecimal(i + 1, element.getDecimal(fieldWithIndex.getIndex()));
            };
          } else if (logicalTypeName.equals("DATE")) {
            return (element, ps, i, fieldWithIndex) -> {
              ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
              ps.setDate(
                  i + 1,
                  value == null
                      ? null
                      : new Date(getDateOrTimeOnly(value.toDateTime(), true).getTime().getTime()));
            };
          } else if (logicalTypeName.equals("TIME")) {
            return (element, ps, i, fieldWithIndex) -> {
              ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
              ps.setTime(
                  i + 1,
                  value == null
                      ? null
                      : new Time(getDateOrTimeOnly(value.toDateTime(), false).getTime().getTime()));
            };
          } else if (logicalTypeName.equals("TIMESTAMP_WITH_TIMEZONE")) {
            return (element, ps, i, fieldWithIndex) -> {
              ReadableDateTime value = element.getDateTime(fieldWithIndex.getIndex());
              if (value == null) {
                ps.setTimestamp(i + 1, null);
              } else {
                Calendar calendar = withTimestampAndTimezone(value.toDateTime());
                ps.setTimestamp(i + 1, new Timestamp(calendar.getTime().getTime()), calendar);
              }
            };
          } else if (logicalTypeName.equals("OTHER")) {
            return (element, ps, i, fieldWithIndex) ->
                ps.setObject(
                    i + 1, element.getValue(fieldWithIndex.getIndex()), java.sql.Types.OTHER);
          } else {
            // generic beam logic type (such as portable logical types)
            return getPreparedStatementSetCaller(logicalType.getBaseType());
          }
        }
      default:
        {
          JdbcIO.PreparedStatementSetCaller pssc =
              typeNamePsSetCallerMap.get(fieldType.getTypeName());
          if (pssc != null) {
            return pssc;
          } else {
            throw new RuntimeException(
                fieldType.getTypeName().name()
                    + " in schema is not supported while writing. Please provide statement and"
                    + " preparedStatementSetter");
          }
        }
    }
  }

  @SuppressWarnings("nullness") // ps.setArray not annotated to allow a null
  private static void setArrayNull(PreparedStatement ps, int i) throws SQLException {
    ps.setArray(i + 1, null);
  }

  static void setNullToPreparedStatement(PreparedStatement ps, int i, JDBCType type)
      throws SQLException {
    ps.setNull(i + 1, type.getVendorTypeNumber());
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
      if (!field.getType().getTypeName().isLogicalType()) {
        return;
      }

      Integer maxLimit =
          (Integer) checkArgumentNotNull(field.getType().getLogicalType()).getArgument();
      if (maxLimit == null) {
        return;
      }

      if (length > maxLimit) {
        throw new RuntimeException(
            String.format(
                "Length of Schema.Field[%s] data exceeds database column capacity",
                field.getName()));
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

  /** @return a {@code JdbcReadPartitionsHelper} instance associated with the given {@param type} */
  static <T> @Nullable JdbcReadWithPartitionsHelper<T> getPartitionsHelper(TypeDescriptor<T> type) {
    // This cast is unchecked, thus this is a small type-checking risk. We just need
    // to make sure that all preset helpers in `JdbcUtil.PRESET_HELPERS` are matched
    // in type from their Key and their Value.
    return (JdbcReadWithPartitionsHelper<T>) PRESET_HELPERS.get(type.getRawType());
  }

  /** Create partitions on a table. */
  static class PartitioningFn<T> extends DoFn<KV<Long, KV<T, T>>, KV<T, T>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitioningFn.class);
    final JdbcReadWithPartitionsHelper<T> partitionsHelper;

    PartitioningFn(JdbcReadWithPartitionsHelper<T> partitionsHelper) {
      this.partitionsHelper = partitionsHelper;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      T lowerBound = c.element().getValue().getKey();
      T upperBound = c.element().getValue().getValue();
      List<KV<T, T>> ranges =
          Lists.newArrayList(
              partitionsHelper.calculateRanges(lowerBound, upperBound, c.element().getKey()));
      LOG.warn("Total of {} ranges: {}", ranges.size(), ranges);
      for (KV<T, T> e : ranges) {
        c.output(e);
      }
    }
  }

  public static final Map<Class<?>, JdbcReadWithPartitionsHelper<?>> PRESET_HELPERS =
      ImmutableMap.of(
          Long.class,
          new JdbcReadWithPartitionsHelper<Long>() {
            @Override
            public Iterable<KV<Long, Long>> calculateRanges(
                Long lowerBound, Long upperBound, Long partitions) {
              List<KV<Long, Long>> ranges = new ArrayList<>();
              // We divide by partitions FIRST to make sure that we can cover the whole LONG range.
              // If we substract first, then we may end up with Long.MAX - Long.MIN, which is 2*MAX,
              // and we'd have trouble with the pipeline.
              long stride = (upperBound / partitions - lowerBound / partitions) + 1;
              long highest = lowerBound;
              for (long i = lowerBound; i < upperBound - stride; i += stride) {
                ranges.add(KV.of(i, i + stride));
                highest = i + stride;
              }
              if (highest < upperBound + 1) {
                ranges.add(KV.of(highest, upperBound + 1));
              }
              return ranges;
            }

            @Override
            public void setParameters(KV<Long, Long> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setLong(1, element.getKey());
                preparedStatement.setLong(2, element.getValue());
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public KV<Long, KV<Long, Long>> mapRow(ResultSet resultSet) throws Exception {
              if (resultSet.getMetaData().getColumnCount() == 3) {
                return KV.of(
                    resultSet.getLong(3), KV.of(resultSet.getLong(1), resultSet.getLong(2)));
              } else {
                return KV.of(0L, KV.of(resultSet.getLong(1), resultSet.getLong(2)));
              }
            }
          },
          DateTime.class,
          new JdbcReadWithPartitionsHelper<DateTime>() {
            @Override
            public Iterable<KV<DateTime, DateTime>> calculateRanges(
                DateTime lowerBound, DateTime upperBound, Long partitions) {
              final List<KV<DateTime, DateTime>> result = new ArrayList<>();

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
                  result.add(KV.of(currentLowerBound, currentUpper));
                  return result;
                }
                result.add(KV.of(currentLowerBound, currentUpper));
                currentLowerBound = currentLowerBound.plus(stride);
              }
              return result;
            }

            @Override
            public void setParameters(
                KV<DateTime, DateTime> element, PreparedStatement preparedStatement) {
              try {
                preparedStatement.setTimestamp(1, new Timestamp(element.getKey().getMillis()));
                preparedStatement.setTimestamp(2, new Timestamp(element.getValue().getMillis()));
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
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(1))),
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(2)))));
              } else {
                return KV.of(
                    0L,
                    KV.of(
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(1))),
                        new DateTime(checkArgumentNotNull(resultSet.getTimestamp(2)))));
              }
            }
          });

  @AutoValue
  abstract static class JdbcUrl {
    abstract String getScheme();

    abstract @Nullable String getHostAndPort();

    abstract String getDatabase();

    /**
     * Parse Jdbc Url String and return an {@link JdbcUrl} object, or return null for unsupported
     * formats.
     *
     * <p>Example of supported format:
     *
     * <ul>
     *   <li>"jdbc:postgresql://localhost:5432/postgres"
     *   <li>"jdbc:mysql://127.0.0.1:3306/db"
     *   <li>"jdbc:oracle:thin:HR/hr@localhost:5221:orcl"
     *   <li>"jdbc:derby:memory:testDB;create=true"
     *   <li>"jdbc:oracle:thin:@//myhost.example.com:1521/my_service"
     *   <li>"jdbc:mysql:///cloud_sql" (GCP CloudSQL, supported if Connection name setup via
     *       HikariDataSource)
     * </ul>
     */
    static @Nullable JdbcUrl of(String url) {
      if (Strings.isNullOrEmpty(url) || !url.startsWith("jdbc:")) {
        return null;
      }
      String cleanUri = url.substring(5);

      // 1. Resolve the scheme
      // handle sub-schemes e.g. oracle:thin (RAC)
      int start = cleanUri.indexOf("//");
      if (start != -1) {
        List<String> subschemes = Splitter.on(':').splitToList(cleanUri.substring(0, start));
        cleanUri = subschemes.get(0) + ":" + cleanUri.substring(start);
      } else {
        // not a URI format e.g. oracle:thin (non-RAC); derby in memory
        if (cleanUri.startsWith("derby:")) {
          String scheme = "derby";
          int endUrl = cleanUri.indexOf(";");
          if (endUrl == -1) {
            endUrl = cleanUri.length();
          }
          List<String> components =
              Splitter.on(':').splitToList(cleanUri.substring("derby:".length(), endUrl));
          if (components.size() < 2) {
            return null;
          }
          return new AutoValue_JdbcUtil_JdbcUrl(scheme, components.get(0), components.get(1));
        } else if (cleanUri.startsWith("oracle:thin:")) {
          String scheme = "oracle";

          int startHost = cleanUri.indexOf("@");
          if (startHost == -1) {
            return null;
          }
          List<String> components = Splitter.on(':').splitToList(cleanUri.substring(startHost + 1));
          if (components.size() < 3) {
            return null;
          }
          return new AutoValue_JdbcUtil_JdbcUrl(
              scheme, components.get(0) + ":" + components.get(1), components.get(2));
        } else {
          return null;
        }
      }

      URI uri = URI.create(cleanUri);
      String scheme = uri.getScheme();

      // 2. resolve database
      @Nullable String path = uri.getPath();
      if (path != null && path.startsWith("/")) {
        path = path.substring(1);
      }
      if (path == null) {
        return null;
      }

      // 3. resolve host and port
      // treat as self-managed SQL instance
      @Nullable String hostAndPort = null;
      @Nullable String host = uri.getHost();
      if (host != null) {
        int port = uri.getPort();
        hostAndPort = port != -1 ? host + ":" + port : null;
      }
      return new AutoValue_JdbcUtil_JdbcUrl(scheme, hostAndPort, path);
    }
  }

  /** Jdbc fully qualified name components. */
  @AutoValue
  abstract static class FQNComponents {
    abstract String getScheme();

    abstract Iterable<String> getSegments();

    void reportLineage(Lineage lineage, @Nullable String table) {
      ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(getSegments());
      if (table != null && !table.isEmpty()) {
        builder.add(table);
      }
      lineage.add(getScheme(), builder.build());
    }

    /** Fail-safely extract FQN from supported DataSource. Return null if failed. */
    static @Nullable FQNComponents of(DataSource dataSource) {
      // Supported case CloudSql using HikariDataSource
      // Had to retrieve properties via Reflection to avoid introduce mandatory Hikari dependencies
      String maybeSqlInstance;
      String url;
      try {
        Class<?> hikariClass = Class.forName("com.zaxxer.hikari.HikariDataSource");
        if (!hikariClass.isInstance(dataSource)) {
          return null;
        }
        Method getProperties = hikariClass.getMethod("getDataSourceProperties");
        Properties properties = (Properties) getProperties.invoke(dataSource);
        if (properties == null) {
          return null;
        }
        maybeSqlInstance = properties.getProperty("cloudSqlInstance");
        if (maybeSqlInstance == null) {
          // not a cloudSqlInstance
          return null;
        }
        Method getUrl = hikariClass.getMethod("getJdbcUrl");
        url = (String) getUrl.invoke(dataSource);
        if (url == null) {
          return null;
        }
      } catch (ClassNotFoundException
          | InvocationTargetException
          | IllegalAccessException
          | NoSuchMethodException e) {
        return null;
      }

      JdbcUrl jdbcUrl = JdbcUrl.of(url);
      if (jdbcUrl == null) {
        LOG.info("Failed to parse JdbcUrl {}. Lineage will not be reported.", url);
        return null;
      }

      String scheme = "cloudsql_" + jdbcUrl.getScheme();
      ImmutableList.Builder<String> segments = ImmutableList.builder();
      List<String> sqlInstance = Arrays.asList(maybeSqlInstance.split(":"));
      if (sqlInstance.size() > 3) {
        // project name contains ":"
        segments
            .add(String.join(":", sqlInstance.subList(0, sqlInstance.size() - 2)))
            .add(sqlInstance.get(sqlInstance.size() - 2))
            .add(sqlInstance.get(sqlInstance.size() - 1));
      } else {
        segments.addAll(Arrays.asList(maybeSqlInstance.split(":")));
      }
      segments.add(jdbcUrl.getDatabase());
      return new AutoValue_JdbcUtil_FQNComponents(scheme, segments.build());
    }

    /** Fail-safely extract FQN from an active connection. Return null if failed. */
    static @Nullable FQNComponents of(Connection connection) {
      try {
        DatabaseMetaData metadata = connection.getMetaData();
        if (metadata == null) {
          // usually not-null, but can be null when running a mock
          return null;
        }
        String url = metadata.getURL();
        if (url == null) {
          // usually not-null, but can be null when running a mock
          return null;
        }
        return of(url);
      } catch (Exception e) {
        // suppressed
        return null;
      }
    }

    /**
     * Fail-safely parse FQN from a Jdbc URL. Return null if failed.
     *
     * <p>e.g.
     *
     * <p>jdbc:postgresql://localhost:5432/postgres -> (postgresql, [localhost:5432, postgres])
     *
     * <p>jdbc:mysql://127.0.0.1:3306/db -> (mysql, [127.0.0.1:3306, db])
     */
    @VisibleForTesting
    static @Nullable FQNComponents of(String url) {
      JdbcUrl jdbcUrl = JdbcUrl.of(url);
      if (jdbcUrl == null || jdbcUrl.getHostAndPort() == null) {
        LOG.info("Failed to parse JdbcUrl {}. Lineage will not be reported.", url);
        return null;
      }
      String hostAndPort = jdbcUrl.getHostAndPort();
      if (hostAndPort == null) {
        LOG.info("Failed to parse host/port from JdbcUrl {}. Lineage will not be reported.", url);
        return null;
      }

      return new AutoValue_JdbcUtil_FQNComponents(
          jdbcUrl.getScheme(), ImmutableList.of(hostAndPort, jdbcUrl.getDatabase()));
    }
  }

  private static final Pattern READ_STATEMENT_PATTERN =
      Pattern.compile(
          "SELECT\\s+.+?\\s+FROM\\s+\\[?(?<tableName>[^\\s\\[\\]]+)\\]?", Pattern.CASE_INSENSITIVE);

  private static final Pattern WRITE_STATEMENT_PATTERN =
      Pattern.compile(
          "INSERT\\s+INTO\\s+\\[?(?<tableName>[^\\s\\[\\]]+)\\]?", Pattern.CASE_INSENSITIVE);

  /** Extract table name a SELECT statement. Return empty string if fail to extract. */
  static String extractTableFromReadQuery(@Nullable String query) {
    if (query == null) {
      return "";
    }
    Matcher matchRead = READ_STATEMENT_PATTERN.matcher(query);
    if (matchRead.find()) {
      String matched = matchRead.group("tableName");
      if (matched != null) {
        return matched;
      }
    }
    return "";
  }

  /** Extract table name from an INSERT statement. Return empty string if fail to extract. */
  static String extractTableFromWriteQuery(@Nullable String query) {
    if (query == null) {
      return "";
    }
    Matcher matchRead = WRITE_STATEMENT_PATTERN.matcher(query);
    if (matchRead.find()) {
      String matched = matchRead.group("tableName");
      if (matched != null) {
        return matched;
      }
    }
    return "";
  }
}
