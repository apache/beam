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
import org.apache.beam.sdk.io.jdbc.JdbcUtil.StartEndRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} executing the SQL query to read from the database. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
@BoundedPerElement
public class ReadSDF<ParameterT, PartitionT, OutputT> extends DoFn<ParameterT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadSDF.class);
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private final String query;
  private final PreparedStatementSetter<ParameterT> parameterSetter;
  private final RowMapper<OutputT> rowMapper;
  private final Coder<ParameterT> restrictionCoder;
  private final int fetchSize;

  private DataSource dataSource;
  private Connection connection;

  ReadSDF(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String query,
      PreparedStatementSetter<ParameterT> parameterSetter,
      RowMapper<OutputT> rowMapper,
      int fetchSize,
      Coder<ParameterT> restrictionCoder) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.query = query;
    this.parameterSetter = parameterSetter;
    this.rowMapper = rowMapper;
    this.fetchSize = fetchSize;
    this.restrictionCoder = restrictionCoder;
  }

  static class ReadRestrictionTracker<ParameterT, PositionT>
      extends RestrictionTracker<ParameterT, PositionT> {
    private PositionT lowerBound;
    private PositionT lastClaimed;
    private PositionT upperBound;
    private final JdbcReadWithPartitionsHelper<PositionT> helper;
    private final TypeDescriptor<PositionT> typeDescriptor;
    private final String columnName;

    ReadRestrictionTracker(
        PositionT lowerBound,
        PositionT upperBound,
        JdbcReadWithPartitionsHelper<PositionT> helper,
        TypeDescriptor<PositionT> typeDescriptor,
        String columnName) {
      this.lowerBound = lowerBound;
      this.lastClaimed = lowerBound;
      this.upperBound = upperBound;
      this.helper = helper;
      this.typeDescriptor = typeDescriptor;
      this.columnName = columnName;
    }

    @Override
    public boolean tryClaim(PositionT position) {
      if (position == null) {
        return true;
      } else {
        // Position is a value that we want to claim.
        if (upperBound instanceof DateTime && position instanceof java.sql.Timestamp) {
          this.lastClaimed = (PositionT) new DateTime(position);
          return ((DateTime) upperBound).getMillis() >= new DateTime(position).getMillis();
        } else {
          assert ((Comparable<PositionT>) position).compareTo(lowerBound) >= 0;
          this.lastClaimed = position;
          return ((Comparable<PositionT>) position).compareTo(upperBound) < 0;
        }
      }
    }

    @Override
    public ParameterT currentRestriction() {
      if (columnName == null) {
        return null;
      } else {
        // TODO: Validate that ParameterT is `StartEndRange<PositionT>` somehow.
        return (ParameterT)
            new StartEndRange<PositionT>(lastClaimed, upperBound, typeDescriptor, columnName);
      }
    }

    @Override
    public @Nullable SplitResult<ParameterT> trySplit(double fractionOfRemainder) {
      // If the column name is null, then this means that we are not trying to partition this
      // query based on a particular function, so we reject any split requests.
      if (columnName != null) {
        List<StartEndRange<PositionT>> ranges =
            Lists.newArrayList(
                this.helper.calculateRanges(this.lastClaimed, this.upperBound, 2L, columnName));

        // TODO: Validate that ParameterT is `StartEndRange<PositionT>` somehow.
        if (ranges.size() > 1) {
          this.upperBound = ranges.get(0).end();
          SplitResult<ParameterT> result =
              SplitResult.<ParameterT>of(
                  (ParameterT) ranges.get(0),
                  (ParameterT)
                      new StartEndRange<PositionT>(
                          ranges.get(1).start(),
                          ranges.get(ranges.size() - 1).end(),
                          ranges.get(0).type(),
                          ranges.get(0).columnName()));
          LOG.info(
              "Splitting range ({},{}),{} into {}",
              this.lowerBound,
              this.lastClaimed,
              this.upperBound,
              result);
          return result;
        }
        LOG.info(
            "Unable to split range ({},{}){}", this.lowerBound, this.lastClaimed, this.upperBound);
      }
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  @DoFn.GetInitialRestriction
  public ParameterT getInitialRestriction(@DoFn.Element ParameterT elm) {
    return elm;
  }

  @GetRestrictionCoder
  public Coder<ParameterT> getRestrictionCoder() {
    return restrictionCoder;
  }

  @NewTracker
  public ReadRestrictionTracker<ParameterT, PartitionT> restrictionTracker(
      @Restriction ParameterT restriction) {
    if (restriction instanceof StartEndRange) {
      StartEndRange<PartitionT> rangeRestriction = (StartEndRange<PartitionT>) restriction;
      return new ReadRestrictionTracker<ParameterT, PartitionT>(
          rangeRestriction.start(),
          rangeRestriction.end(),
          (JdbcReadWithPartitionsHelper<PartitionT>)
              JdbcUtil.PRESET_HELPERS.get(rangeRestriction.type().getRawType()),
          rangeRestriction.type(),
          ((StartEndRange<?>) restriction).columnName());
    } else {
      return new ReadRestrictionTracker<ParameterT, PartitionT>(null, null, null, null, null);
    }
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
  }

  @ProcessElement
  // Spotbugs seems to not understand the nested try-with-resources
  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  public void processElement(
      @Element ParameterT range,
      RestrictionTracker<ParameterT, PartitionT> tracker,
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
      // This DoFn can be called with non-splittable parameters, or with splittable parameters.
      // If the parameters are splittable, then the restriction will not be null, and the parameters
      // will be part of the restriction (therefore tracker.currentRestriction() != null).
      // A "splittable parameter" wil represent a StartEndRange produced within
      // JdbcIO.ReadWithPartitions.
      // If the parameters are non-splittable (i.e. a normal, user-provided parameter from
      // JdbcIO.Read or JdbcIO.ReadVoid.
      if (tracker.currentRestriction() != null) {
        parameterSetter.setParameters(tracker.currentRestriction(), statement);
      } else {
        parameterSetter.setParameters(range, statement);
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          Object position = null;
          if (range instanceof StartEndRange) {
            position = resultSet.getObject(((StartEndRange<?>) range).columnName());
          }
          // For null positions (i.e. a non-splittable parameter), the restriction tracker will
          // always allow us to claim said position, while for non-null positions, we may have
          // performed a split.
          if (tracker.tryClaim((PartitionT) position)) {
            outputReceiver.output(rowMapper.mapRow(resultSet));
          } else {
            // We have arrived to the end of the restriction, and we must end the query
            return;
          }
        }
      }
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
