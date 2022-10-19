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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadWithPartitions<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWithPartitions.class);

  abstract DataSourceConfiguration getDataSourceConfiguration();

  abstract ValueProvider<String> getQuery();

  abstract ValueProvider<String> getTable();

  abstract RowMapper<T> getRowMapper();

  abstract ValueProvider<Integer> getInitialNumReaders();

  abstract Coder<T> getCoder();

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
    checkArgument(
        getDataSourceConfiguration() != null, "withDataSourceConfiguration() is required");
    checkArgument(
        getDataSourceConfiguration().getDatabase() != null
            && getDataSourceConfiguration().getDatabase().get() != null,
        "withDatabase() is required for DataSourceConfiguration in order to perform readWithPartitions");
    checkArgument(getRowMapper() != null, "withRowMapper() is required");

    String table = (getTable() == null) ? null : getTable().get();
    String query = (getQuery() == null) ? null : getQuery().get();

    checkArgument(
        !(table == null && query == null), "One of withTable() or withQuery() is required");
    checkArgument(
        !(table != null && query != null), "withTable() can not be used together with withQuery()");

    Coder<T> coder =
        Util.inferCoder(
            getCoder(),
            getRowMapper(),
            input.getPipeline().getCoderRegistry(),
            input.getPipeline().getSchemaRegistry(),
            LOG);
    query = (table != null) ? "SELECT * FROM " + Util.escapeIdentifier(table): query;
    int initialNumReaders =
        (getInitialNumReaders() != null && getInitialNumReaders().get() != null)
            ? getInitialNumReaders().get()
            : 1;

    return input
        .apply(Create.of((Void) null))
        .apply(
            ParDo.of(
                new ReadWithPartitions.ReadWithPartitionsFn<>(
                    getDataSourceConfiguration(), query, getRowMapper(), initialNumReaders)))
        .setCoder(coder);
  }

  private static class ReadWithPartitionsFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    DataSourceConfiguration dataSourceConfiguration;
    String query;
    RowMapper<OutputT> rowMapper;
    int initialNumReaders;

    ReadWithPartitionsFn(
        DataSourceConfiguration dataSourceConfiguration,
        String query,
        RowMapper<OutputT> rowMapper,
        int initialNumReaders) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.query = query;
      this.rowMapper = rowMapper;
      this.initialNumReaders = initialNumReaders;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      try (Connection conn = dataSource.getConnection()) {
        System.out.println("READING PARTITION : " + tracker.currentRestriction().getFrom());
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

    private int getNumPartitions() throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      try (Connection conn = dataSource.getConnection()) {
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet res =
              stmt.executeQuery(
                  String.format(
                      "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s",
                      Util.escapeString(dataSourceConfiguration.getDatabase().get())))) {
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
