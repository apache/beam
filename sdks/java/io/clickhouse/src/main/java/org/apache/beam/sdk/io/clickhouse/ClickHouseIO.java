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
package org.apache.beam.sdk.io.clickhouse;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

/** An IO to write to ClickHouse. */
public class ClickHouseIO {

  /** A {@link PTransform} to write to ClickHouse. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Row>, PDone> {
    public abstract String jdbcUrl();

    public abstract String table();

    @Override
    public PDone expand(PCollection<Row> input) {
      input.apply(ParDo.of(WriteFn.create(jdbcUrl(), table())));

      return PDone.in(input.getPipeline());
    }

    public static Builder builder() {
      return new AutoValue_ClickHouseIO_Write.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder jdbcUrl(String jdbcUrl);

      public abstract Builder table(String table);

      public abstract Write build();
    }
  }

  @AutoValue
  abstract static class WriteFn extends DoFn<Row, Void> {

    private ClickHouseConnection connection;

    public abstract String jdbcUrl();

    public abstract String table();

    public static WriteFn create(String jdbcUrl, String table) {
      return new AutoValue_ClickHouseIO_WriteFn(jdbcUrl, table);
    }

    public static void writeRow(ClickHouseRowBinaryStream stream, Row row) throws IOException {
      long value = row.getInt64(0);
      stream.writeInt64(value);
    }

    public void write(Iterator<Row> rows) throws SQLException {
      String sql = "INSERT INTO " + table() + " (\"f0\")";

      System.out.println(sql);

      try (ClickHouseStatement statement = connection.createStatement()) {
        statement.sendRowBinaryStream(
            sql,
            stream -> {
              while (rows.hasNext()) {
                Row row = rows.next();
                writeRow(stream, row);
              }
            });
      }
    }

    @Setup
    public void setup() throws SQLException {
      connection = new ClickHouseDataSource(jdbcUrl()).getConnection();
    }

    @Teardown
    public void tearDown() throws Exception {
      connection.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws SQLException {
      Row row = c.element();
      write(Collections.singletonList(row).iterator());
    }
  }
}
