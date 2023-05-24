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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.action;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.joda.time.Instant;

/**
 * Class to generate first set of outputs for {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn}.
 */
@Internal
public class GenerateInitialPartitionsAction {
  private final ChangeStreamMetrics metrics;
  private final ChangeStreamDao changeStreamDao;
  @Nullable private final Instant endTime;

  public GenerateInitialPartitionsAction(
      ChangeStreamMetrics metrics, ChangeStreamDao changeStreamDao, @Nullable Instant endTime) {
    this.metrics = metrics;
    this.changeStreamDao = changeStreamDao;
    this.endTime = endTime;
  }

  /**
   * The very first step of the pipeline when there are no partitions being streamed yet. We want to
   * get an initial list of partitions to stream and output them.
   */
  public void run(OutputReceiver<PartitionRecord> receiver, Instant startTime) {
    List<ByteStringRange> streamPartitions =
        changeStreamDao.generateInitialChangeStreamPartitions();
    for (ByteStringRange partition : streamPartitions) {
      metrics.incListPartitionsCount();
      String uid = UniqueIdGenerator.getNextId();
      PartitionRecord partitionRecord =
          new PartitionRecord(
              partition, startTime, uid, startTime, Collections.emptyList(), endTime);
      // We are outputting elements with timestamp of 0 to prevent reliance on event time. This
      // limits the ability to window on commit time of any data changes. It is still possible to
      // window on processing time.
      receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
    }
  }
}
