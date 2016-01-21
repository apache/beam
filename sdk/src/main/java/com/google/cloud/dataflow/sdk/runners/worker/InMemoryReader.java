/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.splitRequestToApproximateSplitRequest;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A source that yields a set of precomputed elements.
 *
 * @param <T> the type of the elements read from the source
 */
public class InMemoryReader<T> extends NativeReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryReader.class);

  final List<String> encodedElements;
  final int startIndex;
  final int endIndex;
  final Coder<T> coder;

  public InMemoryReader(
      List<String> encodedElements,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex,
      Coder<T> coder) {
    checkNotNull(encodedElements);
    this.encodedElements = encodedElements;
    int maxIndex = encodedElements.size();
    this.startIndex = Math.min(maxIndex, firstNonNull(startIndex, 0));
    this.endIndex = Math.min(maxIndex, firstNonNull(endIndex, maxIndex));
    checkArgument(this.startIndex >= 0, "negative start index: " + startIndex);
    checkArgument(
        this.endIndex >= this.startIndex,
        "end index before start: [" + this.startIndex + ", " + this.endIndex + ")");
    this.coder = coder;
  }

  @Override
  public InMemoryReaderIterator iterator() throws IOException {
    return new InMemoryReaderIterator();
  }

  @Override
  public double getTotalParallelism() {
    return this.endIndex - this.startIndex;
  }

  /**
   * A ReaderIterator that yields an in-memory list of elements.
   */
  class InMemoryReaderIterator extends NativeReaderIterator<T> {
    @VisibleForTesting
    OffsetRangeTracker tracker;
    @Nullable private Integer lastReturnedIndex;
    private T current;

    public InMemoryReaderIterator() {
      this.tracker = new OffsetRangeTracker(startIndex, endIndex);
      this.lastReturnedIndex = null;
    }

    @Override
    public boolean start() throws IOException {
      Preconditions.checkState(lastReturnedIndex == null, "Already started");
      if (!tracker.tryReturnRecordAt(true, startIndex)) {
        return false;
      }
      current = decode(encodedElements.get(startIndex));
      lastReturnedIndex = startIndex;
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      Preconditions.checkNotNull(lastReturnedIndex, "Not started");
      if (!tracker.tryReturnRecordAt(true, (long) lastReturnedIndex + 1)) {
        return false;
      }
      ++lastReturnedIndex;
      current = decode(encodedElements.get(lastReturnedIndex));
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current;
    }

    private T decode(String encodedElementString) throws CoderException {
      // TODO: Replace with the real encoding used by the
      // front end, when we know what it is.
      byte[] encodedElement = StringUtils.jsonStringToByteArray(encodedElementString);
      notifyElementRead(encodedElement.length);
      return CoderUtils.decodeFromByteArray(coder, encodedElement);
    }

    @Override
    public Progress getProgress() {
      if (lastReturnedIndex == null) {
        return null;
      }
      // Currently we assume that only a record index position is reported as
      // current progress. An implementer can override this method to update
      // other metrics, e.g. completion percentage or remaining time.
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      currentPosition.setRecordIndex((long) lastReturnedIndex);

      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      progress.setPosition(currentPosition);

      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public double getRemainingParallelism() {
      // Use the starting index if no elements have yet been returned.
      return tracker.getStopPosition() - firstNonNull(lastReturnedIndex, startIndex);
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);

      com.google.api.services.dataflow.model.Position splitPosition =
          splitRequestToApproximateSplitRequest(splitRequest).getPosition();
      if (splitPosition == null) {
        LOG.warn("InMemoryReader only supports split at a Position. Requested: {}",
            splitRequest);
        return null;
      }

      Long splitIndex = splitPosition.getRecordIndex();
      if (splitIndex == null) {
        LOG.warn("InMemoryReader only supports split at a record index. Requested: {}",
            splitPosition);
        return null;
      }

      if (!tracker.trySplitAtPosition(splitIndex)) {
        return null;
      }
      return new DynamicSplitResultWithPosition(cloudPositionToReaderPosition(splitPosition));
    }
  }
}
