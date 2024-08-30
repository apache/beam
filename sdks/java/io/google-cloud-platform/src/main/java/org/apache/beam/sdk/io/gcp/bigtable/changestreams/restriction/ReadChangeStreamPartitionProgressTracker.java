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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RestrictionTracker used by {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn} to keep
 * track of the progress of the stream and to split the restriction for runner initiated
 * checkpoints.
 *
 * <p>StreamProgress usually is a continuation token which represents a position in time of the
 * stream of a specific partition. The token is used to resume streaming a partition.
 *
 * <p>On ChangeStreamMutation or Heartbeat response, the tracker will try to claim the continuation
 * token from the response. The tracker stores that continuation token (wrapped in a StreamProgress)
 * so that if the DoFn checkpoints or restarts, the token can be used to resume the stream.
 *
 * <p>The tracker will fail to claim a token if runner has initiated a checkpoint (by calling <code>
 * trySplit(0)</code>). This signals to the DoFn to stop.
 *
 * <p>When runner initiates a checkpoint, the tracker returns null for the primary split and the
 * residual split includes the entire token. The next time the DoFn try to claim a new
 * StreamProgress, it will fail, and stop. The residual will be scheduled on a new DoFn to resume
 * the work from the previous StreamProgress
 */
@Internal
public class ReadChangeStreamPartitionProgressTracker
    extends RestrictionTracker<StreamProgress, StreamProgress> {
  StreamProgress streamProgress;
  boolean shouldStop = false;

  /**
   * Constructs a restriction tracker with the streamProgress.
   *
   * @param streamProgress represents a position in time of the stream.
   */
  public ReadChangeStreamPartitionProgressTracker(StreamProgress streamProgress) {
    this.streamProgress = streamProgress;
  }

  /**
   * This restriction tracker is for unbounded streams.
   *
   * @return {@link IsBounded.UNBOUNDED}
   */
  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  /**
   * This is to signal to the runner that this restriction has completed. Throw an exception if
   * there is more work to be done, and it should not stop. A restriction tracker stops after a
   * runner initiated checkpoint or the streamProgress contains a closeStream response and not a
   * token.
   *
   * @throws IllegalStateException when the restriction is not done and there is more work to be
   *     done.
   */
  @Override
  public void checkDone() throws IllegalStateException {
    boolean done =
        shouldStop || streamProgress.getCloseStream() != null || streamProgress.isFailToLock();
    Preconditions.checkState(done, "There's more work to be done");
  }

  /**
   * Claims a new StreamProgress to be processed. StreamProgress can either be a ContinuationToken
   * or a CloseStream.
   *
   * <p>The claim fails if the runner has previously initiated a checkpoint. The restriction tracker
   * respects the runner initiated checkpoint and fails to claim this streamProgress. The new split
   * will start from the previously successfully claimed streamProgress.
   *
   * @param streamProgress position in time of the stream that is being claimed.
   * @return true if claim was successful, otherwise false.
   */
  @Override
  public boolean tryClaim(StreamProgress streamProgress) {
    if (shouldStop) {
      return false;
    }
    // We perform copy instead of assignment because we want to ensure all references of
    // streamProgress gets updated.
    this.streamProgress = streamProgress;
    return true;
  }

  /**
   * Returns the streamProgress that was successfully claimed.
   *
   * @return the streamProgress that was successfully claimed.
   */
  @Override
  public StreamProgress currentRestriction() {
    return streamProgress;
  }

  /**
   * Splits the work that's left. Since the position in the stream isn't a contiguous value, we
   * cannot estimate how much work is left or breakdown the work into smaller chunks. Therefore,
   * there's no way to split the work. To conform to the API, we return null for the primary split
   * and then continue the work on the residual split.
   *
   * <p>Also note that, we only accept checkpoints (fractionOfRemainder = 0). Any other value, we
   * reject (by returning <code>null</code>) the request to split since StreamProgress cannot be
   * broken down into fractions.
   *
   * @param fractionOfRemainder the fraction of work remaining, where 0 is a request to checkpoint
   *     current work.
   * @return split result when fractionOfRemainder = 0, otherwise null.
   */
  @Override
  public @Nullable SplitResult<StreamProgress> trySplit(double fractionOfRemainder) {
    // When asked for fractionOfRemainder = 0, which means the runner is asking for a
    // split/checkpoint. We can terminate the current worker and continue the rest of the work in a
    // new worker.
    if (fractionOfRemainder == 0) {
      // shouldStop is only true if we have trySplit before. We don't want to trySplit again if we
      // have already returned to the runner a split result. Future split should return null. To
      // think of it another way, after trySplit has been called, the primary restriction tracker is
      // null. Trying to split it is impossible, so we are returning null.
      if (shouldStop) {
        return null;
      }
      shouldStop = true;
      return SplitResult.of(null, streamProgress);
    }
    return null;
  }

  @Override
  public String toString() {
    return "CustomRestrictionTracker{"
        + "streamProgress="
        + streamProgress
        + ", shouldStop="
        + shouldStop
        + '}';
  }
}
