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
package org.apache.beam.sdk.io.singlestore;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

/**
 * IO to read and write data on SingleStoreDB.
 *
 * <h3>Reading from SingleStoreDB datasource</h3>
 *
 * <p>SingleStoreIO source returns a bounded collection of {@code T} as a {@code PCollection<T>}. T
 * is the type returned by the provided {@link RowMapper}.
 *
 * <p>To configure the SingleStoreDB source, you have to provide a {@link DataSourceConfiguration}
 * using {@link DataSourceConfiguration#create(String)}(endpoint). Optionally, {@link
 * DataSourceConfiguration#withUsername(String)} and {@link
 * DataSourceConfiguration#withPassword(String)} allows you to define username and password.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline.apply(SingleStoreIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *        .withUsername("username")
 *        .withPassword("password"))
 *   .withQuery("select id, name from Person")
 *   .withRowMapper(new RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * );
 * }</pre>
 *
 * <p>Query parameters can be configured using a user-provided {@link StatementPreparator}. For
 * example:
 *
 * <pre>{@code
 * pipeline.apply(SingleStoreIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306"))
 *   .withQuery("select id,name from Person where name = ?")
 *   .withStatementPreparator(new StatementPreparator() {
 *     public void setParameters(PreparedStatement preparedStatement) throws Exception {
 *       preparedStatement.setString(1, "Darwin");
 *     }
 *   })
 *   .withRowMapper(new RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * );
 * }</pre>
 *
 * <h4>Parallel reading from a SingleStoreDB datasource</h4>
 *
 * <p>SingleStoreIO supports partitioned reading of all data from a query. To enable this, use
 * {@link SingleStoreIO#readWithPartitions()}.
 *
 * <p>The partitioning scheme depends on these parameters, which can be user-provided, or
 * automatically inferred by Beam (for the supported types):
 *
 * <p>The following example shows usage of {@link SingleStoreIO#readWithPartitions()}
 *
 * <pre>{@code
 * pipeline.apply(SingleStoreIO.<Row>readWithPartitions()
 *  .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *       .withUsername("username")
 *       .withPassword("password"))
 *  .withTable("Person")
 *  .withRowMapper(new RowMapper<KV<Integer, String>>() {
 *    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *      return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *    }
 *  })
 * );
 * }</pre>
 *
 * <h3>Writing to SingleStoreDB datasource</h3>
 *
 * <p>SingleStoreIO supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting data to CSV and sending it to the database with LOAD DATA query.
 *
 * <p>Like the source, to configure the sink, you have to provide a {@link DataSourceConfiguration}.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(SingleStoreIO.<KV<Integer, String>>write()
 *      .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *          .withUsername("username")
 *          .withPassword("password"))
 *      .withStatement("insert into Person values(?, ?)")
 *      .withUserDataMapper(new UserDataMapper<KV<Integer, String>>() {
 *        @Override
 *        public List<String> mapRow(KV<Integer, String> element) {
 *          List<String> res = new ArrayList<>();
 *          res.add(element.getKey().toString());
 *          res.add(element.getValue());
 *          return res;
 *        }
 *      })
 *    );
 * }</pre>
 */
public class SingleStoreIO {
  /**
   * Read data from a SingleStoreDB datasource.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_Read.Builder<T>()
        .setOutputParallelization(ValueProvider.StaticValueProvider.of(true))
        .build();
  }

  /**
   * Like {@link #read}, but executes multiple instances of the query on the same table for each
   * database partition.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> ReadWithPartitions<T> readWithPartitions() {
    return new AutoValue_ReadWithPartitions.Builder<T>().build();
  }

  /**
   * Write data to a SingleStoreDB datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new AutoValue_Write.Builder<T>().build();
  }
}
