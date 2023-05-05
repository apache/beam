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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.ThroughputEstimator;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

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
  private transient GenerateInitialPartitionsAction generateInitialPartitionsAction;
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
  public synchronized ChangeStreamAction changeStreamAction(
      ChangeStreamMetrics metrics,
      ThroughputEstimator<KV<ByteString, ChangeStreamMutation>> throughputEstimator) {

    if (changeStreamAction == null) {
      changeStreamAction = new ChangeStreamAction(metrics, throughputEstimator);
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
      GenerateInitialPartitionsAction generateInitialPartitionsAction) {
    if (detectNewPartitionsAction == null) {
      detectNewPartitionsAction =
          new DetectNewPartitionsAction(metrics, metadataTableDao, generateInitialPartitionsAction);
    }
    return detectNewPartitionsAction;
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
      ChangeStreamMetrics metrics, ChangeStreamDao changeStreamDao) {
    if (generateInitialPartitionsAction == null) {
      generateInitialPartitionsAction =
          new GenerateInitialPartitionsAction(metrics, changeStreamDao);
    }
    return generateInitialPartitionsAction;
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
      Duration heartbeatDuration) {
    if (readChangeStreamPartitionAction == null) {
      readChangeStreamPartitionAction =
          new ReadChangeStreamPartitionAction(
              metadataTableDao, changeStreamDao, metrics, changeStreamAction, heartbeatDuration);
    }
    return readChangeStreamPartitionAction;
  }
}
