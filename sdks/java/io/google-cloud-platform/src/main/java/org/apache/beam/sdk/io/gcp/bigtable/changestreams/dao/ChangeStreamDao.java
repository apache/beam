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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.toThreetenInstant;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.ReadChangeStreamQuery;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Data access object to list and read stream partitions of a table. */
@Internal
public class ChangeStreamDao {
  private final BigtableDataClient dataClient;
  private final String tableId;

  public ChangeStreamDao(BigtableDataClient dataClient, String tableId) {
    this.dataClient = dataClient;
    this.tableId = tableId;
  }

  /**
   * Returns the result from GenerateInitialChangeStreamPartitions API.
   *
   * @return list of StreamPartition
   */
  public List<ByteStringRange> generateInitialChangeStreamPartitions() {
    return dataClient.generateInitialChangeStreamPartitionsCallable().all().call(tableId);
  }

  /**
   * Streams a partition.
   *
   * @param partition the partition to stream
   * @param streamProgress may contain a continuation token for the stream request
   * @param endTime time to end the stream, may be null
   * @param heartbeatDuration period between heartbeat messages
   * @return stream of ReadChangeStreamResponse
   * @throws IOException if the stream could not be started
   */
  public ServerStream<ChangeStreamRecord> readChangeStreamPartition(
      PartitionRecord partition,
      StreamProgress streamProgress,
      @Nullable Instant endTime,
      Duration heartbeatDuration)
      throws IOException {
    ReadChangeStreamQuery query =
        ReadChangeStreamQuery.create(tableId).streamPartition(partition.getPartition());

    ChangeStreamContinuationToken currentToken = streamProgress.getCurrentToken();
    Instant startTime = partition.getStartTime();
    List<ChangeStreamContinuationToken> changeStreamContinuationTokenList =
        partition.getChangeStreamContinuationTokens();
    if (currentToken != null) {
      query.continuationTokens(Collections.singletonList(currentToken));
    } else if (startTime != null) {
      query.startTime(toThreetenInstant(startTime));
    } else if (changeStreamContinuationTokenList != null) {
      query.continuationTokens(changeStreamContinuationTokenList);
    } else {
      throw new IOException("Something went wrong");
    }
    if (endTime != null) {
      query.endTime(TimestampConverter.toThreetenInstant(endTime));
    }
    query.heartbeatDuration(org.threeten.bp.Duration.ofMillis(heartbeatDuration.getMillis()));
    return dataClient.readChangeStream(query);
  }
}
