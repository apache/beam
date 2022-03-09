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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;

/**
 * Responsible for making change stream queries for a given partition. The result will be given back
 * as a {@link ResultSet}, which can be consumed until the stream is finished.
 */
public class ChangeStreamDao {

  private static final String REQUEST_TAG = "change_stream";

  private final String changeStreamName;
  private final DatabaseClient databaseClient;
  private final RpcPriority rpcPriority;
  private final String jobName;

  /**
   * Constructs a change stream dao. All the queries performed by this class will be for the given
   * change stream name with the specified rpc priority. The job name will be used to tag all the
   * queries made.
   *
   * @param changeStreamName the name of the change stream to be queried
   * @param databaseClient a spanner {@link DatabaseClient}
   * @param rpcPriority the priority to be used for the change stream queries
   * @param jobName the name of the job performing the query
   */
  ChangeStreamDao(
      String changeStreamName,
      DatabaseClient databaseClient,
      RpcPriority rpcPriority,
      String jobName) {
    this.changeStreamName = changeStreamName;
    this.databaseClient = databaseClient;
    this.rpcPriority = rpcPriority;
    this.jobName = jobName;
  }

  /**
   * Performs a change stream query. If the partition token given is the initial partition null will
   * be used in the query instead. The change stream query will be tagged as following: {@code
   * "action=<REQUEST_TAG>, job=<jobName>"}. The result will be given as a {@link
   * ChangeStreamResultSet} which can be consumed as a stream, yielding records until no more are
   * available for the query made. Note that one needs to call {@link ChangeStreamResultSet#next()}
   * to initiate the change stream query.
   *
   * @param partitionToken the unique partition token to be queried. If {@link
   *     InitialPartition#PARTITION_TOKEN} is given, null will be used in the change stream query
   *     instead.
   * @param startTimestamp the inclusive start time for the change stream query
   * @param endTimestamp the inclusive end time for the change stream query
   * @param heartbeatMillis the number of milliseconds after the stream is idle, which a heartbeat
   *     record will be emitted in the change stream query
   * @return a {@link ChangeStreamResultSet} that will produce a stream of records for the change
   *     stream query
   */
  public ChangeStreamResultSet changeStreamQuery(
      String partitionToken,
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatMillis) {
    // For the initial partition we query with a null partition token
    final String partitionTokenOrNull =
        InitialPartition.isInitialPartition(partitionToken) ? null : partitionToken;

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
                Options.priority(rpcPriority),
                Options.tag("action=" + REQUEST_TAG + ",job=" + jobName));

    return new ChangeStreamResultSet(resultSet);
  }
}
