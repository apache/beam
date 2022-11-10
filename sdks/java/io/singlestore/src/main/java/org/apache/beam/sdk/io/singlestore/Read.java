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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for reading data from SingleStoreDB. It is used by {@link
 * SingleStoreIO#read()}.
 */
@AutoValue
public abstract class Read<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(Read.class);

  abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

  abstract @Nullable String getQuery();

  abstract @Nullable String getTable();

  abstract @Nullable StatementPreparator getStatementPreparator();

  abstract @Nullable Boolean getOutputParallelization();

  abstract @Nullable RowMapper<T> getRowMapper();

  abstract Builder<T> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration dataSourceConfiguration);

    abstract Builder<T> setQuery(String query);

    abstract Builder<T> setTable(String table);

    abstract Builder<T> setStatementPreparator(StatementPreparator statementPreparator);

    abstract Builder<T> setOutputParallelization(Boolean outputParallelization);

    abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

    abstract Read<T> build();
  }

  public Read<T> withDataSourceConfiguration(DataSourceConfiguration config) {
    checkNotNull(config, "dataSourceConfiguration can not be null");
    return toBuilder().setDataSourceConfiguration(config).build();
  }

  public Read<T> withQuery(String query) {
    checkNotNull(query, "query can not be null");
    return toBuilder().setQuery(query).build();
  }

  public Read<T> withTable(String table) {
    checkNotNull(table, "table can not be null");
    return toBuilder().setTable(table).build();
  }

  public Read<T> withStatementPreparator(StatementPreparator statementPreparator) {
    checkNotNull(statementPreparator, "statementPreparator can not be null");
    return toBuilder().setStatementPreparator(statementPreparator).build();
  }

  /**
   * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
   * default is to parallelize and should only be changed if this is known to be unnecessary.
   */
  public Read<T> withOutputParallelization(Boolean outputParallelization) {
    checkNotNull(outputParallelization, "outputParallelization can not be null");
    return toBuilder().setOutputParallelization(outputParallelization).build();
  }

  public Read<T> withRowMapper(RowMapper<T> rowMapper) {
    checkNotNull(rowMapper, "rowMapper can not be null");
    return toBuilder().setRowMapper(rowMapper).build();
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
    Preconditions.checkArgumentNotNull(
        dataSourceConfiguration, "withDataSourceConfiguration() is required");
    RowMapper<T> rowMapper = getRowMapper();
    Preconditions.checkArgumentNotNull(rowMapper, "withRowMapper() is required");
    String actualQuery = SingleStoreUtil.getSelectQuery(getTable(), getQuery());

    Coder<T> coder =
        SingleStoreUtil.inferCoder(
            rowMapper,
            input.getPipeline().getCoderRegistry(),
            input.getPipeline().getSchemaRegistry(),
            LOG);

    PCollection<T> output =
        input
            .apply(Create.of((Void) null))
            .apply(
                ParDo.of(
                    new ReadFn<>(
                        dataSourceConfiguration, actualQuery, getStatementPreparator(), rowMapper)))
            .setCoder(coder);

    if (SingleStoreUtil.getArgumentWithDefault(getOutputParallelization(), true)) {
      output = output.apply(new Reparallelize<>());
    }

    return output;
  }

  private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    DataSourceConfiguration dataSourceConfiguration;
    String query;
    @Nullable StatementPreparator statementPreparator;
    RowMapper<OutputT> rowMapper;

    ReadFn(
        DataSourceConfiguration dataSourceConfiguration,
        String query,
        @Nullable StatementPreparator statementPreparator,
        RowMapper<OutputT> rowMapper) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.query = query;
      this.statementPreparator = statementPreparator;
      this.rowMapper = rowMapper;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      Connection conn = dataSource.getConnection();
      try {
        PreparedStatement stmt = conn.prepareStatement(query);
        try {
          if (statementPreparator != null) {
            statementPreparator.setParameters(stmt);
          }

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
      } finally {
        conn.close();
      }
    }
  }

  // Reparallelize PTransform is copied from JdbcIO
  // https://github.com/apache/beam/blob/9d118bde5fe5a93c5f559ac3227758aea88185b8/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L2115
  private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      // See https://issues.apache.org/jira/browse/BEAM-2803
      // We use a combined approach to "break fusion" here:
      // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
      // 1) force the data to be materialized by passing it as a side input to an identity fn,
      // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
      // and ensures that data to be shuffled can be generated in parallel, while reshuffling
      // provides perfect parallelism.
      // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
      // The current approach is necessary only to support the particular case of SingleStoreIO
      // where
      // a single query may produce many gigabytes of query results.
      PCollectionView<Iterable<T>> empty =
          input
              .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
              .apply(View.asIterable());
      PCollection<T> materialized =
          input.apply(
              "Identity",
              ParDo.of(
                      new DoFn<T, T>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(c.element());
                        }
                      })
                  .withSideInputs(empty));
      return materialized.apply(Reshuffle.viaRandomKey());
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    DataSourceConfiguration.populateDisplayData(getDataSourceConfiguration(), builder);
    builder.addIfNotNull(DisplayData.item("query", getQuery()));
    builder.addIfNotNull(DisplayData.item("table", getTable()));
    builder.addIfNotNull(
        DisplayData.item(
            "statementPreparator", SingleStoreUtil.getClassNameOrNull(getStatementPreparator())));
    builder.addIfNotNull(DisplayData.item("outputParallelization", getOutputParallelization()));
    builder.addIfNotNull(
        DisplayData.item("rowMapper", SingleStoreUtil.getClassNameOrNull(getRowMapper())));
  }
}
