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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import javax.sql.DataSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test JdbcUtil. */
@RunWith(JUnit4.class)
public class JdbcUtilTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // TODO(BEAM-13846): Support string-based partitioning once the transform supports modifying
  //      range properties (inclusive/exclusive).
  static final JdbcReadWithPartitionsHelper<String> PROTOTYPE_STRING_PARTITIONER =
      new JdbcReadWithPartitionsHelper<String>() {
        @Override
        public Iterable<KV<String, String>> calculateRanges(
            String lowerBound, String upperBound, Long partitions) {
          List<KV<String, String>> ranges = new ArrayList<>();
          // For now, we create ranges based on the very first letter of each string.
          // For cases where we have empty strings, we use that as the bottom of one range,
          // and generate other ranges from that point.
          if (lowerBound.length() == 0) {
            lowerBound = String.valueOf(Character.toChars(1)[0]);
            ranges.add(KV.of("", lowerBound));
          }
          int dif = upperBound.charAt(0) - lowerBound.charAt(0);
          int stride = dif / partitions != 0 ? Long.valueOf(dif / partitions).intValue() : 1;
          String currentLowerBound = lowerBound; // String.valueOf(lowerBound.charAt(0));
          while (currentLowerBound.charAt(0) <= upperBound.charAt(0)) {
            int upperBoundCharPoint = currentLowerBound.charAt(0) + stride;
            upperBoundCharPoint =
                upperBoundCharPoint > upperBound.charAt(0)
                    ? Character.toChars(upperBound.charAt(0) + 1)[0]
                    : upperBoundCharPoint;
            char currentUpperBound = Character.toChars(upperBoundCharPoint)[0];
            if (currentUpperBound >= upperBound.charAt(0)) {
              // This means that we have reached the end, and that we want to use our upper bound
              // as our final upper bound.
              int finalChar = upperBound.charAt(upperBound.length() - 1);
              upperBound =
                  upperBound.substring(0, upperBound.length() - 1)
                      + Character.toChars(finalChar)[0];
              ranges.add(KV.of(currentLowerBound, upperBound));
              return ranges;
            }
            ranges.add(KV.of(currentLowerBound, String.valueOf(currentUpperBound)));
            currentLowerBound = String.valueOf(currentUpperBound);
          }
          return ranges;
        }

        @Override
        public void setParameters(KV<String, String> element, PreparedStatement preparedStatement) {
          try {
            preparedStatement.setString(1, element.getKey());
            preparedStatement.setString(2, element.getValue());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public KV<Long, KV<String, String>> mapRow(ResultSet resultSet) throws Exception {
          if (resultSet.getMetaData().getColumnCount() == 3) {
            return KV.of(
                resultSet.getLong(3), KV.of(resultSet.getString(1), resultSet.getString(2)));
          } else {
            return KV.of(0L, KV.of(resultSet.getString(1), resultSet.getString(2)));
          }
        }
      };

  @Test
  public void testGetPreparedStatementSetCaller() throws Exception {
    Schema wantSchema =
        Schema.builder()
            .addField("col1", Schema.FieldType.INT64)
            .addField("col2", Schema.FieldType.INT64)
            .addField("col3", Schema.FieldType.INT64)
            .build();

    String generatedStmt = JdbcUtil.generateStatement("test_table", wantSchema.getFields());
    String expectedStmt = "INSERT INTO test_table(col1, col2, col3) VALUES(?, ?, ?)";
    assertEquals(expectedStmt, generatedStmt);
  }

  @Test
  public void testStringPartitioningWithSingleKeyFn() {
    JdbcReadWithPartitionsHelper<String> helper = PROTOTYPE_STRING_PARTITIONER;
    List<KV<String, String>> expectedRanges =
        Lists.<KV<String, String>>newArrayList(KV.of("a", "a"));
    List<KV<String, String>> ranges = Lists.newArrayList(helper.calculateRanges("a", "a", 10L));
    // It is not possible to generate any more than one range, because the lower and upper range are
    // exactly the same.
    // The range is "a" to the very next element after it, which would be "a"+1 -> "b".
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(1, ranges.size());
    assertArrayEquals(expectedRanges.toArray(), ranges.toArray());
  }

  @Test
  public void testStringPartitioningWithSingleKeyMultiletterFn() {
    JdbcReadWithPartitionsHelper<String> helper = PROTOTYPE_STRING_PARTITIONER;
    List<KV<String, String>> expectedRanges =
        Lists.<KV<String, String>>newArrayList(KV.of("afar", "afar"));
    List<KV<String, String>> ranges =
        Lists.newArrayList(helper.calculateRanges("afar", "afar", 10L));
    // It is not possible to generate any more than one range, because the lower and upper range are
    // exactly the same.
    // The range is "afar" to the very next element after it, which would be "afar"+1 -> "afas".
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(1, ranges.size());
    assertArrayEquals(expectedRanges.toArray(), ranges.toArray());
  }

  @Test
  public void testStringPartitioningWithMultiletter() {
    JdbcReadWithPartitionsHelper<String> helper = PROTOTYPE_STRING_PARTITIONER;
    List<KV<String, String>> ranges =
        Lists.newArrayList(helper.calculateRanges("afarisade", "zfastaridoteaf", 10L));
    // The upper bound is "zfastaridoteaf" to the very next element after it, which would
    // be "zfastaridoteaf"+1 -> "zfastaridoteaf".
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(13L, ranges.size());
    assertThat(
        ranges,
        containsInAnyOrder(
            KV.of("afarisade", "c"),
            KV.of("c", "e"),
            KV.of("e", "g"),
            KV.of("g", "i"),
            KV.of("i", "k"),
            KV.of("k", "m"),
            KV.of("m", "o"),
            KV.of("o", "q"),
            KV.of("q", "s"),
            KV.of("s", "u"),
            KV.of("u", "w"),
            KV.of("w", "y"),
            KV.of("y", "zfastaridoteaf")));
  }

  @Test
  public void testDatetimePartitioningWithSingleKey() {
    JdbcReadWithPartitionsHelper<DateTime> helper =
        JdbcUtil.getPartitionsHelper(TypeDescriptor.of(DateTime.class));
    DateTime onlyPoint = DateTime.now();
    List<KV<DateTime, DateTime>> expectedRanges =
        Lists.newArrayList(KV.of(onlyPoint, onlyPoint.plusMillis(1)));
    List<KV<DateTime, DateTime>> ranges =
        Lists.newArrayList(helper.calculateRanges(onlyPoint, onlyPoint, 10L));
    // It is not possible to generate any more than one range, because the lower and upper range are
    // exactly the same.
    // The range goes from the current DateTime to ONE MILISECOND AFTER.
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(1, ranges.size());
    assertArrayEquals(expectedRanges.toArray(), ranges.toArray());
  }

  @Test
  public void testDatetimePartitioningWithMultiKey() {
    JdbcReadWithPartitionsHelper<DateTime> helper =
        JdbcUtil.getPartitionsHelper(TypeDescriptor.of(DateTime.class));
    DateTime lastPoint = DateTime.now();
    // At least 10ms in the past, or more.
    DateTime firstPoint = lastPoint.minusMillis(10 + new Random().nextInt(Integer.MAX_VALUE));
    List<KV<DateTime, DateTime>> ranges =
        Lists.newArrayList(helper.calculateRanges(firstPoint, lastPoint, 10L));
    // DateTime ranges are able to work out 10-11 ranges because they split in miliseconds which is
    // very small granularity.
    assertThat(Double.valueOf(ranges.size()), closeTo(10, 1));
  }

  @Test
  public void testLongPartitioningWithSingleKey() {
    JdbcReadWithPartitionsHelper<Long> helper =
        JdbcUtil.getPartitionsHelper(TypeDescriptors.longs());
    List<KV<Long, Long>> expectedRanges = Lists.newArrayList(KV.of(12L, 13L));
    List<KV<Long, Long>> ranges = Lists.newArrayList(helper.calculateRanges(12L, 12L, 10L));
    // It is not possible to generate any more than one range, because the lower and upper range are
    // exactly the same.
    // The range goes from the current Long element to ONE ELEMENT AFTER.
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(1, ranges.size());
    assertArrayEquals(expectedRanges.toArray(), ranges.toArray());
  }

  @Test
  public void testLongPartitioningNotEnoughRanges() {
    JdbcReadWithPartitionsHelper<Long> helper =
        JdbcUtil.getPartitionsHelper(TypeDescriptors.longs());
    // The minimum stride is one, which is what causes this sort of partitioning.
    List<KV<Long, Long>> expectedRanges =
        Lists.newArrayList(KV.of(12L, 14L), KV.of(14L, 16L), KV.of(16L, 18L), KV.of(18L, 21L));
    List<KV<Long, Long>> ranges = Lists.newArrayList(helper.calculateRanges(12L, 20L, 10L));
    // The ranges go from the current lowerBound to ONE ELEMENT AFTER the upper bound.
    // Because the query's filter statement is : WHERE column >= lowerBound AND column < upperBound.
    assertEquals(4, ranges.size());
    assertArrayEquals(expectedRanges.toArray(), ranges.toArray());
  }

  @Test
  public void testSavesFilesAsExpected() throws IOException {
    File tempFile1 = temporaryFolder.newFile();
    File tempFile2 = temporaryFolder.newFile();
    String expectedContent1 = "hello world";
    String expectedContent2 = "hello world 2";
    Files.write(tempFile1.toPath(), expectedContent1.getBytes(StandardCharsets.UTF_8));
    Files.write(tempFile2.toPath(), expectedContent2.getBytes(StandardCharsets.UTF_8));

    URL[] urls =
        JdbcUtil.saveFilesLocally(tempFile1.getAbsolutePath() + "," + tempFile2.getAbsolutePath());

    assertEquals(2, urls.length);
    assertEquals(
        expectedContent1,
        new String(Files.readAllBytes(Paths.get(urls[0].getFile())), StandardCharsets.UTF_8));
    assertEquals(
        expectedContent2,
        new String(Files.readAllBytes(Paths.get(urls[1].getFile())), StandardCharsets.UTF_8));
  }

  @Test
  public void testJdbcUrl() {
    ImmutableMap<String, ImmutableList<String>> testCases =
        ImmutableMap.<String, ImmutableList<String>>builder()
            .put(
                "jdbc:postgresql://localhost:5432/postgres",
                ImmutableList.of("postgresql", "localhost:5432", "postgres"))
            .put(
                "jdbc:mysql://127.0.0.1:3306/db", ImmutableList.of("mysql", "127.0.0.1:3306", "db"))
            .put(
                "jdbc:oracle:thin:HR/hr@localhost:5221:orcl",
                ImmutableList.of("oracle", "localhost:5221", "orcl"))
            .put(
                "jdbc:derby:memory:testDB;create=true",
                ImmutableList.of("derby", "memory", "testDB"))
            .put(
                "jdbc:oracle:thin:@//myhost.example.com:1521/my_service",
                ImmutableList.of("oracle", "myhost.example.com:1521", "my_service"))
            .put("jdbc:mysql:///cloud_sql", ImmutableList.of("mysql", "", "cloud_sql"))
            .put("invalid", ImmutableList.of())
            .build();
    for (Entry<String, ImmutableList<String>> entry : testCases.entrySet()) {
      JdbcUtil.JdbcUrl jdbcUrl = JdbcUtil.JdbcUrl.of(entry.getKey());

      System.out.println(entry.getKey());
      if (entry.getValue().equals(ImmutableList.of())) {
        assertNull(jdbcUrl);
      } else {
        assertEquals(entry.getValue().get(0), jdbcUrl.getScheme());
        assertEquals(
            entry.getValue().get(1),
            jdbcUrl.getHostAndPort() == null ? "" : jdbcUrl.getHostAndPort());
        assertEquals(entry.getValue().get(2), jdbcUrl.getDatabase());
      }
    }
  }

  @Test
  public void testFqnFromHikariDataSourcePostgreSql() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql:///postgres");
    config.setUsername("postgres");
    config.addDataSourceProperty(
        "cloudSqlInstance", "example.com:project:some-region:instance-name");
    // instead of `new HikariDataSource(config)`, initialize an empty source to avoid creation
    // of actual connection pool
    DataSource dataSource = new HikariDataSource();
    config.validate();
    config.copyStateTo((HikariConfig) dataSource);
    JdbcUtil.FQNComponents components = JdbcUtil.FQNComponents.of(dataSource);
    assertEquals("cloudsql_postgresql", components.getScheme());
    assertEquals(
        ImmutableList.of("example.com:project", "some-region", "instance-name", "postgres"),
        components.getSegments());
  }

  @Test
  public void testFqnFromBasicDataSourcePostgreSql() {
    BasicDataSource source = new BasicDataSource();
    source.setUrl("jdbc:postgresql:///postgres");
    source.setUsername("postgres");
    source.setConnectionProperties(
        "cloudSqlInstance=example.com:project:some-region:instance-name");
    JdbcUtil.FQNComponents components = JdbcUtil.FQNComponents.of(source);
    assertEquals("cloudsql_postgresql", components.getScheme());
    assertEquals(
        ImmutableList.of("example.com:project", "some-region", "instance-name", "postgres"),
        components.getSegments());
  }

  @Test
  public void testFqnFromHikariDataSourceMySql() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql:///db");
    config.setUsername("root");
    config.addDataSourceProperty("cloudSqlInstance", "some-project:US:instance-name");
    // instead of `new HikariDataSource(config)`, initialize an empty source to avoid creation
    // of actual connection pool
    DataSource dataSource = new HikariDataSource();
    config.validate();
    config.copyStateTo((HikariConfig) dataSource);
    JdbcUtil.FQNComponents components = JdbcUtil.FQNComponents.of(dataSource);
    assertEquals("cloudsql_mysql", components.getScheme());
    assertEquals(
        ImmutableList.of("some-project", "US", "instance-name", "db"), components.getSegments());
  }

  @Test
  public void testExtractTableFromQuery() {
    ImmutableList<KV<String, @Nullable KV<String, String>>> readCases =
        ImmutableList.of(
            KV.of("select * from table_1", KV.of(null, "table_1")),
            KV.of("select * from public.table_1", KV.of("public", "table_1")),
            KV.of("select * from `select`", KV.of(null, "select")),
            KV.of("select * from `public`.`select`", KV.of("public", "select")),
            KV.of("SELECT a, b FROM [table-2]", KV.of(null, "table-2")),
            KV.of("SELECT a, b FROM [public].[table-2]", KV.of("public", "table-2")),
            KV.of("drop table not-select", null));
    for (KV<String, @Nullable KV<String, String>> testCase : readCases) {
      assertEquals(testCase.getValue(), JdbcUtil.extractTableFromReadQuery(testCase.getKey()));
    }
    ImmutableList<KV<String, @Nullable KV<String, String>>> writeCases =
        ImmutableList.of(
            KV.of("insert into table_1 values ...", KV.of(null, "table_1")),
            KV.of("INSERT INTO [table-2] values ...", KV.of(null, "table-2")),
            KV.of("INSERT INTO [foo].[table-2] values ...", KV.of("foo", "table-2")),
            KV.of("drop table not-select", null));
    for (KV<String, @Nullable KV<String, String>> testCase : writeCases) {
      assertEquals(testCase.getValue(), JdbcUtil.extractTableFromWriteQuery(testCase.getKey()));
    }
  }
}
