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

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.splitRequestToApproximateSplitRequest;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.ConcatPosition;
import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.DataflowReaderProgress;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A {@link NativeReader} that reads elements from a given set of encoded {@link Source}s. Creates
 * {@link NativeReader}s for sources lazily, i.e. only when elements from the particular
 * {@code NativeReader} are about to be read.
 *
 * <p>This class does does not cache {@link NativeReader}s and instead creates new set of
 * {@link NativeReader}s for every new {@link ConcatIterator}. Because of this, multiple
 * {@link ConcatIterator}s created using the same {@link ConcatReader} will not be able to share
 * any state between each other. This design was chosen since keeping a large number of
 * {@link NativeReader} objects alive within a single {@link ConcatReader} could be highly
 * memory consuming.
 *
 * <p> For progress reporting and dynamic work rebalancing purposes, {@link ConcatIterator} uses
 * a position of type {@link ConcatPosition}. Progress reporting and dynamic work rebalancing
 * currently work only at the granularity of full sources being concatenated.
 *
 * @param <T> Type of the elements read by the {@link NativeReader}s.
 */
public class ConcatReader<T> extends NativeReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ConcatReader.class);

  public static final String SOURCE_NAME = "ConcatSource";

  private final List<Source> sources;
  private final PipelineOptions options;
  private final ExecutionContext executionContext;
  private final CounterSet.AddCounterMutator addCounterMutator;
  private final String operationName;
  private final ReaderFactory.Registry registry;

  /**
   * Create a {@link ConcatReader} using a given list of encoded {@link Source}s.
   */
  public ConcatReader(
      ReaderFactory.Registry registry,
      PipelineOptions options,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      String operationName,
      List<Source> sources) {
    Preconditions.checkNotNull(sources);
    this.registry = registry;
    this.sources = sources;
    this.options = options;
    this.executionContext = executionContext;
    this.addCounterMutator = addCounterMutator;
    this.operationName = operationName;
  }

  public Iterator<Source> getSources() {
    return sources.iterator();
  }

  @Override
  public ConcatIterator<T> iterator() throws IOException {
    return new ConcatIterator<T>(
        registry,
        options,
        executionContext,
        addCounterMutator,
        operationName,
        sources);
  }

  @VisibleForTesting
  static class ConcatIterator<T> extends NativeReaderIterator<T> {
    private int currentIteratorIndex = -1;
    @Nullable private NativeReaderIterator<T> currentIterator = null;
    private final List<Source> sources;
    private final PipelineOptions options;
    private final ExecutionContext executionContext;
    private final CounterSet.AddCounterMutator addCounterMutator;
    private final String operationName;
    private final OffsetRangeTracker rangeTracker;
    private final ReaderFactory.Registry registry;

    public ConcatIterator(
        ReaderFactory.Registry registry,
        PipelineOptions options,
        ExecutionContext executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        String operationName,
        List<Source> sources) {
      this.registry = registry;
      this.sources = sources;
      this.options = options;
      this.executionContext = executionContext;
      this.addCounterMutator = addCounterMutator;
      this.operationName = operationName;
      this.rangeTracker = new OffsetRangeTracker(0, sources.size());
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      while (true) {
        // Invariant: we call currentIterator.start() immediately when opening an iterator
        // (below). So if currentIterator != null, then start() has already been called on it.
        if (currentIterator != null && currentIterator.advance()) {
          // Happy case: current iterator has a next record.
          return true;
        }
        // Now current iterator is either non-existent or exhausted.
        // Close it, and try opening a new one.
        if (currentIterator != null) {
          currentIterator.close();
          currentIterator = null;
        }

        if (!rangeTracker.tryReturnRecordAt(true, currentIteratorIndex + 1)) {
          return false;
        }
        currentIteratorIndex++;
        if (currentIteratorIndex == sources.size()) {
          // All sources were read.
          return false;
        }

        Source currentSource = sources.get(currentIteratorIndex);
        try {
          @SuppressWarnings("unchecked")
          NativeReader<T> currentReader =
              (NativeReader<T>)
                  registry.create(
                      currentSource, options, executionContext, addCounterMutator, operationName);
          currentIterator = currentReader.iterator();
        } catch (Exception e) {
          throw new IOException("Failed to create a reader for source: " + currentSource, e);
        }
        if (!currentIterator.start()) {
          currentIterator.close();
          currentIterator = null;
          continue;
        }
        // Happy case: newly opened iterator has a first record.
        return true;
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (currentIterator == null) {
        throw new NoSuchElementException();
      }
      return currentIterator.getCurrent();
    }

    @Override
    public void close() throws IOException {
      if (currentIterator != null) {
        currentIterator.close();
      }
    }

    @Override
    public Progress getProgress() {
      if (currentIteratorIndex < 0) {
        // Reading has not been started yet.
        return null;
      }

      ConcatPosition concatPosition = new ConcatPosition();
      concatPosition.setIndex(currentIteratorIndex);
      Progress progressOfCurrentIterator = currentIterator.getProgress();
      if (!(progressOfCurrentIterator instanceof DataflowReaderProgress)) {
        throw new IllegalArgumentException("Cannot process progress " + progressOfCurrentIterator
            + " since ConcatReader can only handle readers that generate a progress of type "
            + "DataflowReaderProgress");
      }
      com.google.api.services.dataflow.model.Position positionOfCurrentIterator =
          ((DataflowReaderProgress) progressOfCurrentIterator).cloudProgress.getPosition();

      if (positionOfCurrentIterator != null) {
        concatPosition.setPosition(positionOfCurrentIterator);
      }

      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      currentPosition.setConcatPosition(concatPosition);
      progress.setPosition(currentPosition);

      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);

      ApproximateSplitRequest splitProgress = splitRequestToApproximateSplitRequest(splitRequest);
      com.google.api.services.dataflow.model.Position cloudPosition = splitProgress.getPosition();
      if (cloudPosition == null) {
        LOG.warn("Concat only supports split at a Position. Requested: {}", splitRequest);
        return null;
      }

      ConcatPosition concatPosition = cloudPosition.getConcatPosition();
      if (concatPosition == null) {
        LOG.warn(
            "ConcatReader only supports split at a ConcatPosition. Requested: {}", cloudPosition);
        return null;
      }

      if (rangeTracker.trySplitAtPosition(concatPosition.getIndex())) {
        com.google.api.services.dataflow.model.Position positionToSplit =
            new com.google.api.services.dataflow.model.Position();
        positionToSplit.setConcatPosition(
            new ConcatPosition().setIndex((int) rangeTracker.getStopPosition().longValue()));
        return new DynamicSplitResultWithPosition(cloudPositionToReaderPosition(positionToSplit));
      } else {
        LOG.debug("Could not perform the dynamic split request " + splitRequest);
        return null;
      }
    }
  }
}
