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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.DetectNewPartitionsAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.GenerateInitialPartitionsAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ProcessNewPartitionsAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ResumeFromPreviousPipelineAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.InitialPipelineState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.DetectNewPartitionsTracker;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Instant;

// Allows for detectNewPartitionsAction setup
@SuppressWarnings("initialization.fields.uninitialized")
@Internal
@UnboundedPerElement
public class DetectNewPartitionsDoFn extends DoFn<InitialPipelineState, PartitionRecord> {
  private static final long serialVersionUID = 8052524268978107367L;
  @Nullable private final Instant endTime;

  private final DaoFactory daoFactory;
  private final ChangeStreamMetrics metrics;
  private final ActionFactory actionFactory;
  private DetectNewPartitionsAction detectNewPartitionsAction;

  public DetectNewPartitionsDoFn(
      @Nullable Instant endTime,
      ActionFactory actionFactory,
      DaoFactory daoFactory,
      ChangeStreamMetrics metrics) {
    this.actionFactory = actionFactory;
    this.daoFactory = daoFactory;
    this.endTime = endTime;
    this.metrics = metrics;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(
      @Element InitialPipelineState initialPipelineState) {
    return initialPipelineState.getStartTime();
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction() {
    return new OffsetRange(0, Long.MAX_VALUE);
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(@Restriction OffsetRange restriction) {
    return new DetectNewPartitionsTracker(restriction.getFrom());
  }

  // We never want to scale based on this DoFn, so we return a constant backlog estimate of zero.
  @GetSize
  public double getSize() {
    return 0d;
  }

  @Setup
  public void setup() throws IOException {
    final MetadataTableDao metadataTableDao = daoFactory.getMetadataTableDao();
    final ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
    ProcessNewPartitionsAction processNewPartitionsAction =
        actionFactory.processNewPartitionsAction(metrics, metadataTableDao, endTime);
    GenerateInitialPartitionsAction generateInitialPartitionsAction =
        actionFactory.generateInitialPartitionsAction(metrics, changeStreamDao, endTime);
    ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction =
        actionFactory.resumeFromPreviousPipelineAction(
            metrics, metadataTableDao, endTime, processNewPartitionsAction);
    detectNewPartitionsAction =
        actionFactory.detectNewPartitionsAction(
            metrics,
            metadataTableDao,
            endTime,
            generateInitialPartitionsAction,
            resumeFromPreviousPipelineAction,
            processNewPartitionsAction);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element InitialPipelineState initialPipelineState,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws Exception {
    return detectNewPartitionsAction.run(
        tracker, receiver, watermarkEstimator, initialPipelineState);
  }
}
