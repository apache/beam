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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.range.OffsetRangeTracker;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** A ReaderIterator that yields an in-memory list of elements. */
  class InMemoryReaderIterator extends NativeReaderIterator<T> {
    @VisibleForTesting OffsetRangeTracker tracker;
    private @Nullable Integer lastReturnedIndex;
    private Optional<T> current;

    public InMemoryReaderIterator() {
      this.tracker = new OffsetRangeTracker(startIndex, endIndex);
      this.lastReturnedIndex = null;
    }

    @Override
    public boolean start() throws IOException {
      Preconditions.checkState(lastReturnedIndex == null, "Already started");
      if (!tracker.tryReturnRecordAt(true, startIndex)) {
        current = Optional.empty();
        return false;
      }
      current = Optional.of(decode(encodedElements.get(startIndex)));
      lastReturnedIndex = startIndex;
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      Preconditions.checkNotNull(lastReturnedIndex, "Not started");
      if (!tracker.tryReturnRecordAt(true, (long) lastReturnedIndex + 1)) {
        current = Optional.empty();
        return false;
      }
      ++lastReturnedIndex;
      current = Optional.of(decode(encodedElements.get(lastReturnedIndex)));
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current.get();
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

      return SourceTranslationUtils.cloudProgressToReaderProgress(progress);
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
          SourceTranslationUtils.splitRequestToApproximateSplitRequest(splitRequest).getPosition();
      if (splitPosition == null) {
        LOG.warn("InMemoryReader only supports split at a Position. Requested: {}", splitRequest);
        return null;
      }

      Long splitIndex = splitPosition.getRecordIndex();
      if (splitIndex == null) {
        LOG.warn(
            "InMemoryReader only supports split at a record index. Requested: {}", splitPosition);
        return null;
      }

      if (!tracker.trySplitAtPosition(splitIndex)) {
        return null;
      }
      return new DynamicSplitResultWithPosition(
          SourceTranslationUtils.cloudPositionToReaderPosition(splitPosition));
    }

    @Nullable
    @Override
    public DynamicSplitResult requestCheckpoint() {
      if (!tracker.trySplitAtPosition(lastReturnedIndex + 1)) {
        return null;
      }
      com.google.api.services.dataflow.model.Position splitPosition =
          new com.google.api.services.dataflow.model.Position();
      splitPosition.setRecordIndex((long) lastReturnedIndex + 1);
      return new DynamicSplitResultWithPosition(
          SourceTranslationUtils.cloudPositionToReaderPosition(splitPosition));
    }
  }
}
