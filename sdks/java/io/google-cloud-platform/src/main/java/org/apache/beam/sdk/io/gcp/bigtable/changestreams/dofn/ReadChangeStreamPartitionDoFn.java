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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ChangeStreamAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ReadChangeStreamPartitionAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Allows for readChangeStreamPartitionAction setup
@SuppressWarnings({"initialization.fields.uninitialized", "UnusedVariable"})
@Internal
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn
    extends DoFn<PartitionRecord, KV<ByteString, ChangeStreamMutation>> {
  private static final long serialVersionUID = 4418739381635104479L;

  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);

  private final Duration heartbeatDuration;
  private final DaoFactory daoFactory;
  private final ChangeStreamMetrics metrics;
  private final ActionFactory actionFactory;

  private ReadChangeStreamPartitionAction readChangeStreamPartitionAction;

  public ReadChangeStreamPartitionDoFn(
      Duration heartbeatDuration,
      DaoFactory daoFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics) {
    this.heartbeatDuration = heartbeatDuration;
    this.daoFactory = daoFactory;
    this.metrics = metrics;
    this.actionFactory = actionFactory;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Element PartitionRecord partitionRecord) {
    return partitionRecord.getParentLowWatermark();
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public StreamProgress initialRestriction() {
    metrics.incPartitionStreamCount();
    return new StreamProgress();
  }

  @NewTracker
  public ReadChangeStreamPartitionProgressTracker restrictionTracker(
      @Restriction StreamProgress restriction) {
    return new ReadChangeStreamPartitionProgressTracker(restriction);
  }

  @Setup
  public void setup() throws IOException {
    MetadataTableDao metadataTableDao = daoFactory.getMetadataTableDao();
    ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
    ChangeStreamAction changeStreamAction = actionFactory.changeStreamAction(this.metrics);
    readChangeStreamPartitionAction =
        actionFactory.readChangeStreamPartitionAction(
            metadataTableDao, changeStreamDao, metrics, changeStreamAction, heartbeatDuration);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionRecord partitionRecord,
      RestrictionTracker<StreamProgress, StreamProgress> tracker,
      OutputReceiver<KV<ByteString, ChangeStreamMutation>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws InterruptedException, IOException {
    return readChangeStreamPartitionAction.run(
        partitionRecord, tracker, receiver, watermarkEstimator);
  }
}
