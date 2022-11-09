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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadWithPartitions<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWithPartitions.class);

  abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

  abstract @Nullable String getQuery();

  abstract @Nullable String getTable();

  abstract @Nullable RowMapper<T> getRowMapper();

  abstract @Nullable Integer getInitialNumReaders();

  abstract Builder<T> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration dataSourceConfiguration);

    abstract Builder<T> setQuery(String query);

    abstract Builder<T> setTable(String table);

    abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

    abstract Builder<T> setInitialNumReaders(Integer initialNumReaders);

    abstract ReadWithPartitions<T> build();
  }

  public ReadWithPartitions<T> withDataSourceConfiguration(DataSourceConfiguration config) {
    checkNotNull(config, "dataSourceConfiguration can not be null");
    return toBuilder().setDataSourceConfiguration(config).build();
  }

  public ReadWithPartitions<T> withQuery(String query) {
    checkNotNull(query, "query can not be null");
    return toBuilder().setQuery(query).build();
  }

  public ReadWithPartitions<T> withTable(String table) {
    checkNotNull(table, "table can not be null");
    return toBuilder().setTable(table).build();
  }

  public ReadWithPartitions<T> withRowMapper(RowMapper<T> rowMapper) {
    checkNotNull(rowMapper, "rowMapper can not be null");
    return toBuilder().setRowMapper(rowMapper).build();
  }

  /** Pre-split initial restriction and start initialNumReaders reading at the very beginning. */
  public ReadWithPartitions<T> withInitialNumReaders(Integer initialNumReaders) {
    checkNotNull(initialNumReaders, "initialNumReaders can not be null");
    return toBuilder().setInitialNumReaders(initialNumReaders).build();
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
    Preconditions.checkArgumentNotNull(
        dataSourceConfiguration, "withDataSourceConfiguration() is required");
    String database = dataSourceConfiguration.getDatabase();
    Preconditions.checkArgumentNotNull(
        database,
        "withDatabase() is required for DataSourceConfiguration in order to perform readWithPartitions");
    RowMapper<T> rowMapper = getRowMapper();
    Preconditions.checkArgumentNotNull(rowMapper, "withRowMapper() is required");

    int initialNumReaders = SingleStoreUtil.getArgumentWithDefault(getInitialNumReaders(), 1);
    checkArgument(
        initialNumReaders >= 1, "withInitialNumReaders() should be greater or equal to 1");

    String actualQuery = SingleStoreUtil.getSelectQuery(getTable(), getQuery());

    Coder<T> coder =
        SingleStoreUtil.inferCoder(
            rowMapper,
            input.getPipeline().getCoderRegistry(),
            input.getPipeline().getSchemaRegistry(),
            LOG);

    return input
        .apply(Create.of((Void) null))
        .apply(
            ParDo.of(
                new ReadWithPartitions.ReadWithPartitionsFn<>(
                    dataSourceConfiguration, actualQuery, database, rowMapper, initialNumReaders)))
        .setCoder(coder);
  }

  private static class ReadWithPartitionsFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    DataSourceConfiguration dataSourceConfiguration;
    String query;
    String database;
    RowMapper<OutputT> rowMapper;
    int initialNumReaders;

    ReadWithPartitionsFn(
        DataSourceConfiguration dataSourceConfiguration,
        String query,
        String database,
        RowMapper<OutputT> rowMapper,
        int initialNumReaders) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.query = query;
      this.database = database;
      this.rowMapper = rowMapper;
      this.initialNumReaders = initialNumReaders;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      Connection conn = dataSource.getConnection();
      try {
        for (long partition = tracker.currentRestriction().getFrom();
            tracker.tryClaim(partition);
            partition++) {
          PreparedStatement stmt =
              conn.prepareStatement(
                  String.format("SELECT * FROM (%s) WHERE partition_id()=%d", query, partition));
          try {
            ResultSet res = stmt.executeQuery();
            try {
              while (res.next()) {
                context.output(rowMapper.mapRow(res));
              }
            } finally {
              res.close();
            }
          } finally {
            stmt.close();
          }
        }
      } finally {
        conn.close();
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element ParameterT element) throws Exception {
      return new OffsetRange(0L, getNumPartitions());
    }

    @SplitRestriction
    public void splitRange(
        @Element ParameterT element,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> receiver) {
      long numPartitions = range.getTo() - range.getFrom();
      checkArgument(
          initialNumReaders <= numPartitions,
          "withInitialNumReaders() should not be greater then number of partitions in the database.\n"
              + String.format(
                  "InitialNumReaders is %d, number of partitions in the database is %d",
                  initialNumReaders, range.getTo()));

      for (int i = 0; i < initialNumReaders; i++) {
        receiver.output(
            new OffsetRange(
                range.getFrom() + numPartitions * i / initialNumReaders,
                range.getFrom() + numPartitions * (i + 1) / initialNumReaders));
      }
    }

    private int getNumPartitions() throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      Connection conn = dataSource.getConnection();
      try {
        Statement stmt = conn.createStatement();
        try {
          ResultSet res =
              stmt.executeQuery(
                  String.format(
                      "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s",
                      SingleStoreUtil.escapeString(database)));
          try {
            if (!res.next()) {
              throw new Exception("Failed to get number of partitions in the database");
            }

            return res.getInt(1);
          } finally {
            res.close();
          }
        } finally {
          stmt.close();
        }
      } finally {
        conn.close();
      }
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    DataSourceConfiguration.populateDisplayData(getDataSourceConfiguration(), builder);
    builder.addIfNotNull(DisplayData.item("query", getQuery()));
    builder.addIfNotNull(DisplayData.item("table", getTable()));
    builder.addIfNotNull(
        DisplayData.item("rowMapper", SingleStoreUtil.getClassNameOrNull(getRowMapper())));
    builder.addIfNotNull(DisplayData.item("initialNumReaders", getInitialNumReaders()));
  }
}
