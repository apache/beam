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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.PipelineInitializer.DEFAULT_PARENT_PARTITION_TOKEN;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

// TODO: Add java docs
public class ChangeStreamDao {

  private final String changeStreamName;
  private final DatabaseClient databaseClient;

  public ChangeStreamDao(String changeStreamName, DatabaseClient databaseClient) {
    this.changeStreamName = changeStreamName;
    this.databaseClient = databaseClient;
  }

  public ResultSet changeStreamQuery(
      String partitionToken,
      Timestamp startTimestamp,
      boolean isInclusiveStart,
      Timestamp endTimestamp,
      boolean isInclusiveEnd,
      long heartbeatMillis) {
    // For the initial partition we query with a null partition token
    final String partitionTokenOrNull =
        partitionToken.equals(DEFAULT_PARENT_PARTITION_TOKEN) ? null : partitionToken;
    final String startTimeOptions =
        isInclusiveStart ? "'INCLUDE_START_TIME'" : "'EXCLUDE_START_TIME'";
    final String endTimeOptions = isInclusiveEnd ? "'INCLUDE_END_TIME'" : "'EXCLUDE_END_TIME'";

    // FIXME: Use parameterized query when possible
    return databaseClient
        .singleUse()
        .executeQuery(
            Statement.newBuilder(
                    "SELECT *"
                        + " FROM READ_"
                        + changeStreamName
                        + "("
                        + "   start_timestamp => @startTimestamp,"
                        + "   end_timestamp => @endTimestamp,"
                        + "   partition_token => @partitionToken,"
                        // FIXME: Add the options when possible
                        + "   read_options => null,"
                        // + "   read_options => [" + startTimeOptions + ", " + endTimeOptions +
                        // "],"
                        + "   heartbeat_milliseconds => @heartbeatMillis"
                        + ")")
                .bind("startTimestamp")
                .to(startTimestamp)
                .bind("endTimestamp")
                .to(endTimestamp)
                .bind("partitionToken")
                .to(partitionTokenOrNull)
                .bind("heartbeatMillis")
                .to(heartbeatMillis)
                .build());
  }
}
