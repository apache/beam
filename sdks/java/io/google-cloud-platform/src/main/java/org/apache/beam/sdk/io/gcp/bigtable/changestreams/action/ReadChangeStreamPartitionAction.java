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

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMutation;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF.
 */
@SuppressWarnings({"UnusedVariable", "UnusedMethod"})
public class ReadChangeStreamPartitionAction {
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionAction.class);

  private final MetadataTableDao metadataTableDao;
  private final ChangeStreamDao changeStreamDao;
  private final ChangeStreamMetrics metrics;
  private final ChangeStreamAction changeStreamAction;
  private final Duration heartbeatDurationSeconds;

  public ReadChangeStreamPartitionAction(
      MetadataTableDao metadataTableDao,
      ChangeStreamDao changeStreamDao,
      ChangeStreamMetrics metrics,
      ChangeStreamAction changeStreamAction,
      Duration heartbeatDurationSeconds) {
    this.metadataTableDao = metadataTableDao;
    this.changeStreamDao = changeStreamDao;
    this.metrics = metrics;
    this.changeStreamAction = changeStreamAction;
    this.heartbeatDurationSeconds = heartbeatDurationSeconds;
  }

  /**
   * Streams changes from a specific partition. This function is responsible to maintaining the
   * lifecycle of streaming the partition. We delegate to {@link ChangeStreamAction} to process
   * individual response from the change stream.
   *
   * <p>Before we send a request to Cloud Bigtable to stream the partition, we need to perform a few
   * things.
   *
   * <ol>
   *   <li>Lock the partition. Due to the design of the change streams connector, it is possible
   *       that multiple DoFn are started trying to stream the same partition. However, only 1 DoFn
   *       should be streaming a partition. So we solve this by using the metadata table as a
   *       distributed lock. We attempt to lock the partition for this DoFn's UUID. If it is
   *       successful, it means this DoFn is the only one that can stream the partition and
   *       continue. Otherwise, terminate this DoFn because another DoFn is streaming this partition
   *       already.
   *   <li>Process CloseStream if it exists. In order to solve a possible inconsistent state
   *       problem, we do not process CloseStream after receiving it. We claim the CloseStream in
   *       the RestrictionTracker so it persists after a checkpoint. We checkpoint to flush all the
   *       DataChanges. Then on resume, we process the CloseStream. There are only 2 expected Status
   *       for CloseStream: OK and Out of Range.
   *       <ol>
   *         <li>OK status is returned when the predetermined endTime has been reached. In this
   *             case, we update the watermark and update the metadata table. {@link
   *             DetectNewPartitionsDoFn} aggregates the watermark from all the streams to ensure
   *             all the streams have reached beyond endTime so it can also terminate and end the
   *             beam job.
   *         <li>Out of Range is returned when the partition has either been split into more
   *             partitions or merged into a larger partition. In this case, we write to the
   *             metadata table the new partitions' information so that {@link
   *             DetectNewPartitionsDoFn} can read and output those new partitions to be streamed.
   *             We also need to ensure we clean up this partition's metadata to release the lock.
   *       </ol>
   *   <li>Update the metadata table with the watermark and additional debugging info.
   *   <li>Stream the partition.
   * </ol>
   *
   * @param partitionRecord partition information used to identify this stream
   * @param tracker restriction tracker of {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @param receiver output receiver for {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @param watermarkEstimator watermark estimator {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @return {@link ProcessContinuation#stop} if a checkpoint is required or the stream has
   *     completed. Or {@link ProcessContinuation#resume} if a checkpoint is required.
   * @throws IOException when stream fails.
   */
  public ProcessContinuation run(
      PartitionRecord partitionRecord,
      RestrictionTracker<StreamProgress, StreamProgress> tracker,
      DoFn.OutputReceiver<KV<ByteString, ChangeStreamMutation>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws IOException {

    return ProcessContinuation.stop();
  }
}
