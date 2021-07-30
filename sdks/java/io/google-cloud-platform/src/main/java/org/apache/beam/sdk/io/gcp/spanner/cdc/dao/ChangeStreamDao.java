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
package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.apache.beam.sdk.io.gcp.spanner.cdc.InitialPartition;

// TODO: Add java docs
public class ChangeStreamDao {

  private final String changeStreamName;
  private final DatabaseClient databaseClient;
  private final RpcPriority rpcPriority;

  public ChangeStreamDao(
      String changeStreamName, DatabaseClient databaseClient, RpcPriority rpcPriority) {
    this.changeStreamName = changeStreamName;
    this.databaseClient = databaseClient;
    this.rpcPriority = rpcPriority;
  }

  public ChangeStreamResultSet changeStreamQuery(
      String partitionToken,
      Timestamp startTimestamp,
      boolean isInclusiveStart,
      Timestamp endTimestamp,
      boolean isInclusiveEnd,
      long heartbeatMillis) {
    // For the initial partition we query with a null partition token
    final String partitionTokenOrNull =
        InitialPartition.isInitialPartition(partitionToken) ? null : partitionToken;
    final String startTimeOptions =
        isInclusiveStart ? "'INCLUDE_START_TIME'" : "'EXCLUDE_START_TIME'";
    final String endTimeOptions = isInclusiveEnd ? "'INCLUDE_END_TIME'" : "'EXCLUDE_END_TIME'";

    // FIXME: Add the options when possible
    final String query =
        "SELECT * FROM READ_"
            + changeStreamName
            + "("
            + "   start_timestamp => @startTimestamp,"
            + "   end_timestamp => @endTimestamp,"
            + "   partition_token => @partitionToken,"
            + "   read_options => null,"
            + "   heartbeat_milliseconds => @heartbeatMillis"
            + ")";
    final ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(query)
                    .bind("startTimestamp")
                    .to(startTimestamp)
                    .bind("endTimestamp")
                    .to(endTimestamp)
                    .bind("partitionToken")
                    .to(partitionTokenOrNull)
                    .bind("heartbeatMillis")
                    .to(heartbeatMillis)
                    .build(),
                Options.priority(rpcPriority));

    return new ChangeStreamResultSet(resultSet);
  }
}
