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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadWithPartitions<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWithPartitions.class);

  abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

  abstract @Nullable ValueProvider<String> getQuery();

  abstract @Nullable ValueProvider<String> getTable();

  abstract @Nullable RowMapper<T> getRowMapper();

  abstract @Nullable ValueProvider<Integer> getInitialNumReaders();

  abstract @Nullable Coder<T> getCoder();

  abstract Builder<T> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration dataSourceConfiguration);

    abstract Builder<T> setQuery(ValueProvider<String> query);

    abstract Builder<T> setTable(ValueProvider<String> table);

    abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

    abstract Builder<T> setInitialNumReaders(ValueProvider<Integer> initialNumReaders);

    abstract Builder<T> setCoder(Coder<T> coder);

    abstract ReadWithPartitions<T> build();
  }

  public ReadWithPartitions<T> withDataSourceConfiguration(DataSourceConfiguration config) {
    checkNotNull(config, "dataSourceConfiguration can not be null");
    return toBuilder().setDataSourceConfiguration(config).build();
  }

  public ReadWithPartitions<T> withQuery(String query) {
    checkNotNull(query, "query can not be null");
    return withQuery(ValueProvider.StaticValueProvider.of(query));
  }

  public ReadWithPartitions<T> withQuery(ValueProvider<String> query) {
    checkNotNull(query, "query can not be null");
    return toBuilder().setQuery(query).build();
  }

  public ReadWithPartitions<T> withTable(String table) {
    checkNotNull(table, "table can not be null");
    return withTable(ValueProvider.StaticValueProvider.of(table));
  }

  public ReadWithPartitions<T> withTable(ValueProvider<String> table) {
    checkNotNull(table, "table can not be null");
    return toBuilder().setTable(table).build();
  }

  public ReadWithPartitions<T> withRowMapper(RowMapper<T> rowMapper) {
    checkNotNull(rowMapper, "rowMapper can not be null");
    return toBuilder().setRowMapper(rowMapper).build();
  }

  public ReadWithPartitions<T> withInitialNumReaders(Integer initialNumReaders) {
    checkNotNull(initialNumReaders, "initialNumReaders can not be null");
    return withInitialNumReaders(ValueProvider.StaticValueProvider.of(initialNumReaders));
  }

  public ReadWithPartitions<T> withInitialNumReaders(ValueProvider<Integer> initialNumReaders) {
    checkNotNull(initialNumReaders, "initialNumReaders can not be null");
    return toBuilder().setInitialNumReaders(initialNumReaders).build();
  }

  public ReadWithPartitions<T> withCoder(Coder<T> coder) {
    checkNotNull(coder, "coder cen not be null");
    return toBuilder().setCoder(coder).build();
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
    if (dataSourceConfiguration == null) {
      throw new IllegalArgumentException("withDataSourceConfiguration() is required");
    }

    ValueProvider<String> databaseProvider = dataSourceConfiguration.getDatabase();
    if (databaseProvider == null) {
      throw new IllegalArgumentException("withDatabase() is required for DataSourceConfiguration in order to perform readWithPartitions");
    }

    String database = databaseProvider.get();
    if (database == null) {
      throw new IllegalArgumentException("withDatabase() is required for DataSourceConfiguration in order to perform readWithPartitions");
    }

    RowMapper<T> rowMapper = getRowMapper();
    if (rowMapper == null) {
      throw new IllegalArgumentException("withRowMapper() is required");
    }

    int initialNumReaders = 1;
    ValueProvider<Integer> initialNumReadersProvider = getInitialNumReaders();
    if (initialNumReadersProvider != null && initialNumReadersProvider.get() != null) {
      initialNumReaders = initialNumReadersProvider.get();
    }
    checkArgument(initialNumReaders < 1, "withInitialNumReaders() can not be less then 1");

    String actualQuery = Util.getSelectQuery(getTable(), getQuery());

    Coder<T> coder =
        Util.inferCoder(
            getCoder(),
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
      try (Connection conn = dataSource.getConnection()) {
        for (long partition = tracker.currentRestriction().getFrom();
            tracker.tryClaim(partition);
            partition++) {
          try (PreparedStatement stmt =
              conn.prepareStatement(
                  String.format("SELECT * FROM (%s) WHERE partition_id()=%d", query, partition))) {
            try (ResultSet res = stmt.executeQuery()) {
              while (res.next()) {
                context.output(rowMapper.mapRow(res));
              }
            }
          }
        }
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element ParameterT element) throws Exception {
      return new OffsetRange(0L, getNumPartitions());
    }

    @SplitRestriction
    public void splitRange(
        @Element String element,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> receiver) {
      long numPartitions = range.getTo() - range.getFrom();
      if (initialNumReaders > numPartitions) {
        throw new IllegalArgumentException("withInitialNumReaders() should not be greater then number of partitions in the database.\n" +
            String.format("InitialNumReaders is %d, number of partitions in the database is %d", initialNumReaders, range.getTo()));
      }

      for (int i = 0; i < initialNumReaders; i++) {
        receiver.output(new OffsetRange(range.getFrom() + numPartitions*i/initialNumReaders, range.getFrom() + numPartitions*(i+1)/initialNumReaders));
      }
    }

    private int getNumPartitions() throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      try (Connection conn = dataSource.getConnection()) {
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet res =
              stmt.executeQuery(
                  String.format(
                      "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s",
                      Util.escapeString(database)))) {
            if (!res.next()) {
              throw new Exception("Failed to get number of partitions in the database");
            }

            return res.getInt(1);
          }
        }
      }
    }
  }
}
