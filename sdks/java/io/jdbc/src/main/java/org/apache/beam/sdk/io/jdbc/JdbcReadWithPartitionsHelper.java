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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.values.KV;

/**
 * A helper for {@link JdbcIO.ReadWithPartitions} that handles range calculations.
 *
 * @param <PartitionT> Element type of the column used for partition.
 */
public interface JdbcReadWithPartitionsHelper<PartitionT>
    extends PreparedStatementSetter<KV<PartitionT, PartitionT>>,
        RowMapper<KV<Long, KV<PartitionT, PartitionT>>> {

  /**
   * Calculate the range of each partition from the lower and upper bound, and number of partitions.
   *
   * <p>Return a list of pairs for each lower and upper bound within each partition.
   */
  Iterable<KV<PartitionT, PartitionT>> calculateRanges(
      PartitionT lowerBound, PartitionT upperBound, Long partitions);

  @Override
  void setParameters(KV<PartitionT, PartitionT> element, PreparedStatement preparedStatement);

  @Override
  KV<Long, KV<PartitionT, PartitionT>> mapRow(ResultSet resultSet) throws Exception;
}
