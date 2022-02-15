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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.io.jdbc.JdbcUtil.JdbcReadWithPartitionsHelper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BoundedPerElement
public class ReadSDF<ParameterT, OutputT> extends DoFn<KV<ParameterT, ParameterT>, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadSDF.class);
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private final String query;
  private final PreparedStatementSetter<KV<ParameterT, ParameterT>> parameterSetter;
  private final RowMapper<OutputT> rowMapper;
  private final int fetchSize;
  private final JdbcReadWithPartitionsHelper<ParameterT> partitioningHelper;
  private final SerializableFunction<ResultSet, ParameterT> rowPartitionValueGetter;
  private final Coder<KV<ParameterT, ParameterT>> restrictionCoder;

  private DataSource dataSource;
  private Connection connection;

  ReadSDF(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String query,
      PreparedStatementSetter<KV<ParameterT, ParameterT>> parameterSetter,
      RowMapper<OutputT> rowMapper,
      int fetchSize,
      JdbcReadWithPartitionsHelper<ParameterT> partitioningHelper,
      SerializableFunction<ResultSet, ParameterT> rowPartitionValueGetter,
      Coder<KV<ParameterT, ParameterT>> restrictionCoder) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.query = query;
    this.parameterSetter = parameterSetter;
    this.rowMapper = rowMapper;
    this.fetchSize = fetchSize;
    this.partitioningHelper = partitioningHelper;
    this.rowPartitionValueGetter = rowPartitionValueGetter;
    this.restrictionCoder = restrictionCoder;
  }

  static class ReadRestrictionTracker<ParameterT>
      extends RestrictionTracker<KV<ParameterT, ParameterT>, ParameterT> {
    private ParameterT lowerBound;
    private ParameterT lastClaimed;
    private ParameterT upperBound;
    private final JdbcReadWithPartitionsHelper<ParameterT> helper;

    ReadRestrictionTracker(
        ParameterT lowerBound,
        ParameterT upperBound,
        JdbcReadWithPartitionsHelper<ParameterT> helper) {
      this.lowerBound = lowerBound;
      this.lastClaimed = lowerBound;
      this.upperBound = upperBound;
      this.helper = helper;
    }

    @Override
    public boolean tryClaim(ParameterT position) {
      System.out.println(
          this.toString()
              + " CLAIMING "
              + position.toString()
              + " lowbound: "
              + lowerBound.toString()
              + " highbound: "
              + upperBound.toString());
      if (upperBound instanceof DateTime && position instanceof java.sql.Timestamp) {
        this.lastClaimed = (ParameterT) new DateTime(position);
        return ((DateTime) upperBound).getMillis() >= new DateTime(position).getMillis();
      } else {
        this.lastClaimed = position;
        return ((Comparable<ParameterT>) position).compareTo(upperBound) <= 0;
      }
    }

    @Override
    public KV<ParameterT, ParameterT> currentRestriction() {
      return KV.of(lastClaimed, upperBound);
    }

    @Override
    public @Nullable SplitResult<KV<ParameterT, ParameterT>> trySplit(double fractionOfRemainder) {
      List<KV<ParameterT, ParameterT>> ranges =
          Lists.newArrayList(this.helper.calculateRanges(this.lastClaimed, this.upperBound, 2L));
      System.out.println("SPLITTING RESULT !" + ranges.toString());

      if (ranges.size() > 1) {
        this.upperBound = ranges.get(0).getValue();
        return SplitResult.of(
            ranges.get(0), KV.of(ranges.get(1).getKey(), ranges.get(ranges.size() - 1).getValue()));
      } else {
        return null;
      }
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  @DoFn.GetInitialRestriction
  public KV<ParameterT, ParameterT> getInitialRestriction(
      @DoFn.Element KV<ParameterT, ParameterT> range) {
    // The input ParameterT is a Range.
    return range;
  }

  @GetRestrictionCoder
  public Coder<KV<ParameterT, ParameterT>> getRestrictionCoder() {
    return restrictionCoder;
  }

  @NewTracker
  public ReadRestrictionTracker<ParameterT> restrictionTracker(
      @Restriction KV<ParameterT, ParameterT> restriction) {
    return new ReadRestrictionTracker<ParameterT>(
        restriction.getKey(), restriction.getValue(), partitioningHelper);
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
  }

  @ProcessElement
  // Spotbugs seems to not understand the nested try-with-resources
  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  public void processElement(
      @Element KV<ParameterT, ParameterT> range,
      RestrictionTracker<KV<ParameterT, ParameterT>, ParameterT> tracker,
      OutputReceiver<OutputT> outputReceiver)
      throws Exception {
    // Only acquire the connection if we need to perform a read.
    if (connection == null) {
      connection = dataSource.getConnection();
    }
    // PostgreSQL requires autocommit to be disabled to enable cursor streaming
    // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
    LOG.info("Autocommit has been disabled");
    connection.setAutoCommit(false);
    try (PreparedStatement statement =
        connection.prepareStatement(
            query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      statement.setFetchSize(fetchSize);
      parameterSetter.setParameters(tracker.currentRestriction(), statement);
      try (ResultSet resultSet = statement.executeQuery()) {
        System.out.println("STATEMENT: " + statement.toString());
        while (resultSet.next()) {
          if (tracker.tryClaim(rowPartitionValueGetter.apply(resultSet))) {
            System.out.println("OUTPUTTING " + rowMapper.mapRow(resultSet).toString());
            outputReceiver.output(rowMapper.mapRow(resultSet));
          } else {
            // We have arrived to the end of the restriction, and we must end the query
            return;
          }
        }
      }
      tracker.tryClaim(tracker.currentRestriction().getValue());
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    cleanUpConnection();
  }

  @Override
  protected void finalize() throws Throwable {
    cleanUpConnection();
  }

  private void cleanUpConnection() throws Exception {
    if (connection != null) {
      try {
        connection.close();
      } finally {
        connection = null;
      }
    }
  }
}
