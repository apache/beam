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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>IO to read and write data on JDBC.</p>
 * <p>
 * <h3>Reading from JDBC datasource</h3>
 * <p>
 * JdbcIO source returns a bounded collection of {@code T} as a
 * {@code PCollection<T>}. T is the type returned by the provided {@link RowMapper}.
 * </p>
 * <p>
 * To configure the JDBC source, you have to provide a {@link DataSource}. The username and
 * password to connect to the database are optionals. The following example illustrates how to
 * configure a JDBC source:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline.apply(JdbcIO.read()
 *   .withDataSource(myDataSource)
 *   .withQuery("select id,name from Person")
 *   .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       KV<Integer, String> kv = KV.of(resultSet.getInt(1), resultSet.getString(2));
 *       return kv;
 *     }
 *   })
 *
 *   }
 * </pre>
 * <h3>Writing to JDBC datasource</h3>
 * <p>
 * JDBC sink supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting each T into a {@link PreparedStatement} via a user-provided
 * {@link PreparedStatementSetter}.
 * </p>
 * <p>
 * Like the source, to configure JDBC sink, you have to provide a datasource. For instance:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.write()
 *      .withDataSource(myDataSource)
 *      .withQuery("insert into Person values(?, ?)")
 *      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
 *        public void setParameters(KV<Integer, String> element, PreparedStatement query) {
 *          // use the PCollection element to set parameters of the SQL query used to insert
 *          // in the database
 *          // for instance:
 *          query.setInt(1, kv.getKey());
 *          query.setString(2, kv.getValue());
 *        }
 *      })
 *
 *   }
 * </pre>
 */
public class JdbcIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @return a {@link Read} {@link PTransform}.
   */
  public static Read<?> read() {
    return new Read(new Read.JdbcOptions(null, null, null, null), null);
  }

  /**
   * Write data to a JDBC datasource.
   *
   * @return a {@link Write} {@link PTransform}.
   */
  public static Write<?> write() {
    return new Write(new Write.WriteFn(null, null, null, null,
        null, 1024L));
  }

  private JdbcIO() {
  }

  /**
   * An interface used by the JdbcIO Read for mapping rows of a ResultSet on a per-row basis.
   * Implementations of this interface perform the actual work of mapping each row to a result
   * object used in the {@link PCollection}.
   */
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  /**
   * A {@link PTransform} to read data from a JDBC datasource.
   */
  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    public Read<T> withDataSource(DataSource dataSource) {
      return new Read<T>(options.withDataSource(dataSource), rowMapper);
    }

    public Read<T> withQuery(String query) {
      return new Read<T>(options.withQuery(query), rowMapper);
    }

    public <X> Read<X> withRowMapper(RowMapper<X> rowMapper) {
      return new Read<X>(options, rowMapper);
    }

    public Read<T> withUsername(String username) {
      return new Read<T>(options.withUsername(username), rowMapper);
    }

    public Read<T> withPassword(String password) {
      return new Read<T>(options.withPassword(password), rowMapper);
    }

    private final JdbcOptions options;
    private final RowMapper<T> rowMapper;

    private Read(JdbcOptions options, RowMapper<T> rowMapper) {
      this.options = options;
      this.rowMapper = rowMapper;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      PCollection<T> output = input.apply(Create.of(options))
          .apply(ParDo.of(new ReadFn<>(rowMapper)));

      return output;
    }

    @Override
    public void validate(PBegin input) {
      Preconditions.checkNotNull(rowMapper, "rowMapper");
      options.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      options.populateDisplayData(builder);
    }

    @VisibleForTesting
    static class JdbcOptions implements Serializable {

      private final DataSource dataSource;
      private final String query;
      @Nullable
      private final String username;
      @Nullable
      private final String password;

      private JdbcOptions(DataSource dataSource, String query,
                          @Nullable String username,
                          @Nullable String password) {
        this.dataSource = dataSource;
        this.query = query;
        this.username = username;
        this.password = password;
      }

      public JdbcOptions withDataSource(DataSource dataSource) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withQuery(String query) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withRowMapper(RowMapper rowMapper) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withUsername(String username) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withPassword(String password) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(query, "query");
      }

      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("dataSource", dataSource.getClass().getName()));
        builder.add(DisplayData.item("query", query));
        builder.addIfNotNull(DisplayData.item("username", username));
      }

    }

    /**
     * A {@link DoFn} executing the SQL query to read from the database.
     */
    static class ReadFn<T> extends DoFn<JdbcOptions, T> {

      private final RowMapper<T> rowMapper;

      private ReadFn(RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        JdbcOptions options = context.element();

        try (Connection connection = (options.username != null)
            ? options.dataSource.getConnection(options.username, options.password)
            : options.dataSource.getConnection()) {

          try (PreparedStatement statement = connection.prepareStatement(options.query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                T record = rowMapper.mapRow(resultSet);
                context.output(record);
              }
            }
          }
        }
      }
    }
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  public interface PreparedStatementSetter<T> extends Serializable {
    void setParameters(T element, PreparedStatement preparedStatement) throws Exception;
  }

  /**
   * A {@link PTransform} to write to a JDBC datasource.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    public Write<T> withDataSource(DataSource dataSource) {
      return new Write<>(writeFn.withDataSource(dataSource));
    }

    public Write<T> withStatement(String statement) {
      return new Write<>(writeFn.withStatement(statement));
    }

    public Write<T> withUsername(String username) {
      return new Write<>(writeFn.withUsername(username));
    }

    public Write<T> withPassword(String password) {
      return new Write<>(writeFn.withPassword(password));
    }

    public <X> Write<X> withPreparedStatementSetter(
        PreparedStatementSetter<X> preparedStatementSetter) {
      return new Write<>(writeFn.withPreparedStatementSetter(preparedStatementSetter));
    }

    public Write<T> withBatchSize(long batchSize) {
      return new Write<>(writeFn.withBatchSize(batchSize));
    }

    private final WriteFn writeFn;

    private Write(WriteFn writeFn) {
      this.writeFn = writeFn;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      input.apply(ParDo.of(writeFn));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<T> input) {
      writeFn.validate();
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final DataSource dataSource;
      private final String statement;
      private final String username;
      private final String password;
      private final PreparedStatementSetter<T> preparedStatementSetter;
      private long batchSize;

      private Connection connection;
      private PreparedStatement preparedStatement;
      private long batchCount;

      public WriteFn(DataSource dataSource, String statement, String username, String password,
                     PreparedStatementSetter<T> preparedStatementSetter, long batchSize) {
        this.dataSource = dataSource;
        this.statement = statement;
        this.username = username;
        this.password = password;
        this.preparedStatementSetter = preparedStatementSetter;
        this.batchSize = batchSize;
      }

      public WriteFn<T> withDataSource(DataSource dataSource) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public WriteFn<T> withStatement(String statement) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public WriteFn<T> withUsername(String username) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public WriteFn<T> withPassword(String password) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public WriteFn<T> withPreparedStatementSetter(
          PreparedStatementSetter<T> preparedStatementSetter) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public WriteFn<T> withBatchSize(long batchSize) {
        return new WriteFn<>(dataSource, statement, username, password, preparedStatementSetter,
            batchSize);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(statement, "query");
        Preconditions.checkNotNull(preparedStatementSetter, "preparedStatementSetter");
      }

      @Setup
      public void connectToDatabase() throws Exception {
        if (username != null) {
          connection = dataSource.getConnection(username, password);
        } else {
          connection = dataSource.getConnection();
        }
        connection.setAutoCommit(false);
        preparedStatement = connection.prepareStatement(statement);
      }

      @StartBundle
      public void startBundle(Context context) {
        batchCount = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        T record = context.element();

        preparedStatement.clearParameters();
        preparedStatementSetter.setParameters(record, preparedStatement);
        preparedStatement.addBatch();

        batchCount++;

        if (batchCount >= batchSize) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        preparedStatement.executeBatch();
        connection.commit();
        batchCount = 0;
      }

      @Teardown
      public void closeConnection() throws Exception {
        if (preparedStatement != null) {
          preparedStatement.close();
        }
        if (connection != null) {
          connection.close();
        }
      }

    }

  }

}
