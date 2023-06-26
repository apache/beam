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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.SizeEstimator;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Factory class for creating instances that will handle different functions of DoFns. The instances
 * created are all singletons.
 */
// Allows for transient fields to be initialized later
@SuppressWarnings("initialization.field.uninitialized")
@Internal
public class ActionFactory implements Serializable {
  private static final long serialVersionUID = -6780082495458582986L;

  private transient ChangeStreamAction changeStreamAction;
  private transient DetectNewPartitionsAction detectNewPartitionsAction;
  private transient ProcessNewPartitionsAction processNewPartitionsAction;
  private transient GenerateInitialPartitionsAction generateInitialPartitionsAction;
  private transient ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction;
  private transient ReadChangeStreamPartitionAction readChangeStreamPartitionAction;

  /**
   * Creates and returns a singleton instance of an action class for processing individual
   * ChangeStreamMutation in {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link ChangeStreamAction}
   */
  public synchronized ChangeStreamAction changeStreamAction(ChangeStreamMetrics metrics) {
    if (changeStreamAction == null) {
      changeStreamAction = new ChangeStreamAction(metrics);
    }
    return changeStreamAction;
  }

  /**
   * Creates and returns a singleton instance of an action class for processing {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn}.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link DetectNewPartitionsAction}
   */
  public synchronized DetectNewPartitionsAction detectNewPartitionsAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      @Nullable Instant endTime,
      GenerateInitialPartitionsAction generateInitialPartitionsAction,
      ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction,
      ProcessNewPartitionsAction processNewPartitionsAction) {
    if (detectNewPartitionsAction == null) {
      detectNewPartitionsAction =
          new DetectNewPartitionsAction(
              metrics,
              metadataTableDao,
              endTime,
              generateInitialPartitionsAction,
              resumeFromPreviousPipelineAction,
              processNewPartitionsAction);
    }
    return detectNewPartitionsAction;
  }

  public synchronized ProcessNewPartitionsAction processNewPartitionsAction(
      ChangeStreamMetrics metrics, MetadataTableDao metadataTableDao, @Nullable Instant endTime) {
    if (processNewPartitionsAction == null) {
      processNewPartitionsAction =
          new ProcessNewPartitionsAction(metrics, metadataTableDao, endTime);
    }
    return processNewPartitionsAction;
  }

  /**
   * Creates and returns a singleton instance of an action class for processing {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn}
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link GenerateInitialPartitionsAction}
   */
  public synchronized GenerateInitialPartitionsAction generateInitialPartitionsAction(
      ChangeStreamMetrics metrics, ChangeStreamDao changeStreamDao, @Nullable Instant endTime) {
    if (generateInitialPartitionsAction == null) {
      generateInitialPartitionsAction =
          new GenerateInitialPartitionsAction(metrics, changeStreamDao, endTime);
    }
    return generateInitialPartitionsAction;
  }

  public synchronized ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      @Nullable Instant endTime,
      ProcessNewPartitionsAction processNewPartitionsAction) {
    if (resumeFromPreviousPipelineAction == null) {
      resumeFromPreviousPipelineAction =
          new ResumeFromPreviousPipelineAction(
              metrics, metadataTableDao, endTime, processNewPartitionsAction);
    }
    return resumeFromPreviousPipelineAction;
  }

  /**
   * Creates and returns a singleton instance of an action class for processing {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link ReadChangeStreamPartitionAction}
   */
  public synchronized ReadChangeStreamPartitionAction readChangeStreamPartitionAction(
      MetadataTableDao metadataTableDao,
      ChangeStreamDao changeStreamDao,
      ChangeStreamMetrics metrics,
      ChangeStreamAction changeStreamAction,
      Duration heartbeatDuration,
      SizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator) {
    if (readChangeStreamPartitionAction == null) {
      readChangeStreamPartitionAction =
          new ReadChangeStreamPartitionAction(
              metadataTableDao,
              changeStreamDao,
              metrics,
              changeStreamAction,
              heartbeatDuration,
              sizeEstimator);
    }
    return readChangeStreamPartitionAction;
  }
}
