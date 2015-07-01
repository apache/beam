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
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.splitRequestToApproximateProgress;
import static java.lang.Math.min;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.AbstractBoundedReaderIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A source that yields a set of precomputed elements.
 *
 * @param <T> the type of the elements read from the source
 */
public class InMemoryReader<T> extends Reader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryReader.class);

  final List<String> encodedElements;
  final int startIndex;
  final int endIndex;
  final Coder<T> coder;

  public InMemoryReader(List<String> encodedElements, @Nullable Long startIndex,
      @Nullable Long endIndex, Coder<T> coder) {
    this.encodedElements = encodedElements;
    int maxIndex = encodedElements.size();
    if (startIndex == null) {
      this.startIndex = 0;
    } else {
      if (startIndex < 0) {
        throw new IllegalArgumentException("start index should be >= 0");
      }
      this.startIndex = (int) min(startIndex, maxIndex);
    }
    if (endIndex == null) {
      this.endIndex = maxIndex;
    } else {
      if (endIndex < this.startIndex) {
        throw new IllegalArgumentException("end index should be >= start index");
      }
      this.endIndex = (int) min(endIndex, maxIndex);
    }
    this.coder = coder;
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    return new InMemoryReaderIterator();
  }

  /**
   * A ReaderIterator that yields an in-memory list of elements.
   */
  class InMemoryReaderIterator extends AbstractBoundedReaderIterator<T> {
    @VisibleForTesting
    OffsetRangeTracker tracker;
    private int nextIndex;

    public InMemoryReaderIterator() {
      this.tracker = new OffsetRangeTracker(startIndex, endIndex);
      this.nextIndex = startIndex;
    }

    @Override
    protected boolean hasNextImpl() {
      return tracker.tryReturnRecordAt(true, (long) nextIndex);
    }

    @Override
    protected T nextImpl() throws IOException {
      String encodedElementString = encodedElements.get(nextIndex++);
      // TODO: Replace with the real encoding used by the
      // front end, when we know what it is.
      byte[] encodedElement = StringUtils.jsonStringToByteArray(encodedElementString);
      notifyElementRead(encodedElement.length);
      return CoderUtils.decodeFromByteArray(coder, encodedElement);
    }

    @Override
    public Progress getProgress() {
      // Currently we assume that only a record index position is reported as
      // current progress. An implementer can override this method to update
      // other metrics, e.g. completion percentage or remaining time.
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      currentPosition.setRecordIndex((long) nextIndex);

      ApproximateProgress progress = new ApproximateProgress();
      progress.setPosition(currentPosition);

      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);

      com.google.api.services.dataflow.model.Position splitPosition =
          splitRequestToApproximateProgress(splitRequest).getPosition();
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
