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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.ConcatPosition;
import com.google.api.services.dataflow.model.Source;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link NativeReader} that reads elements from a given set of encoded {@link Source}s. Creates
 * {@link NativeReader}s for sources lazily, i.e. only when elements from the particular {@code
 * NativeReader} are about to be read.
 *
 * <p>This class does does not cache {@link NativeReader}s and instead creates new set of {@link
 * NativeReader}s for every new {@link ConcatIterator}. Because of this, multiple {@link
 * ConcatIterator}s created using the same {@link ConcatReader} will not be able to share any state
 * between each other. This design was chosen since keeping a large number of {@link NativeReader}
 * objects alive within a single {@link ConcatReader} could be highly memory consuming.
 *
 * <p>For progress reporting and dynamic work rebalancing purposes, {@link ConcatIterator} uses a
 * position of type {@link ConcatPosition}. Progress reporting and dynamic work rebalancing
 * currently work only at the granularity of full sources being concatenated.
 *
 * @param <T> Type of the elements read by the {@link NativeReader}s.
 */
public class ConcatReader<T> extends NativeReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ConcatReader.class);

  public static final String SOURCE_NAME = "ConcatSource";

  private final List<Source> sources;
  private final PipelineOptions options;
  private final DataflowExecutionContext executionContext;
  private final ReaderFactory readerFactory;
  private final DataflowOperationContext operationContext;

  /** Create a {@link ConcatReader} using a given list of encoded {@link Source}s. */
  public ConcatReader(
      ReaderFactory readerFactory,
      PipelineOptions options,
      DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext,
      List<Source> sources) {
    this.operationContext = operationContext;
    Preconditions.checkNotNull(sources);
    this.readerFactory = readerFactory;
    this.sources = sources;
    this.options = options;
    this.executionContext = executionContext;
  }

  public Iterator<Source> getSources() {
    return sources.iterator();
  }

  @Override
  public ConcatIterator<T> iterator() throws IOException {
    return new ConcatIterator<T>(
        readerFactory, options, executionContext, operationContext, sources);
  }

  @VisibleForTesting
  static class ConcatIterator<T> extends NativeReaderIterator<T> {
    private int currentIteratorIndex = -1;
    private @Nullable NativeReaderIterator<T> currentIterator = null;
    private final List<Source> sources;
    private final PipelineOptions options;
    private final DataflowExecutionContext executionContext;
    private final DataflowOperationContext operationContext;
    private final OffsetRangeTracker rangeTracker;
    private final ReaderFactory readerFactory;

    public ConcatIterator(
        ReaderFactory readerFactory,
        PipelineOptions options,
        DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext,
        List<Source> sources) {
      this.readerFactory = readerFactory;
      this.sources = sources;
      this.options = options;
      this.executionContext = executionContext;
      this.operationContext = operationContext;
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
          Coder<?> coder = null;
          if (currentSource.getCodec() != null) {
            coder =
                CloudObjects.coderFromCloudObject(CloudObject.fromSpec(currentSource.getCodec()));
          }
          @SuppressWarnings("unchecked")
          NativeReader<T> currentReader =
              (NativeReader<T>)
                  readerFactory.create(
                      CloudObject.fromSpec(currentSource.getSpec()),
                      coder,
                      options,
                      executionContext,
                      operationContext);
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

      if (currentIterator == null) {
        // Reading has been finished.
        throw new UnsupportedOperationException("Can't report progress of a finished iterator");
      }

      ConcatPosition concatPosition = new ConcatPosition();
      concatPosition.setIndex(currentIteratorIndex);
      Progress progressOfCurrentIterator = currentIterator.getProgress();
      if (!(progressOfCurrentIterator instanceof SourceTranslationUtils.DataflowReaderProgress)) {
        throw new IllegalArgumentException(
            "Cannot process progress "
                + progressOfCurrentIterator
                + " since ConcatReader can only handle readers that generate a progress of type "
                + "DataflowReaderProgress");
      }
      com.google.api.services.dataflow.model.Position positionOfCurrentIterator =
          ((SourceTranslationUtils.DataflowReaderProgress) progressOfCurrentIterator)
              .cloudProgress.getPosition();

      if (positionOfCurrentIterator != null) {
        concatPosition.setPosition(positionOfCurrentIterator);
      }

      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      currentPosition.setConcatPosition(concatPosition);
      progress.setPosition(currentPosition);

      return SourceTranslationUtils.cloudProgressToReaderProgress(progress);
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);

      ApproximateSplitRequest splitProgress =
          SourceTranslationUtils.splitRequestToApproximateSplitRequest(splitRequest);
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

      java.lang.Integer index = concatPosition.getIndex();
      if (index == null) {
        index = 0;
      }

      if (rangeTracker.trySplitAtPosition(index)) {
        com.google.api.services.dataflow.model.Position positionToSplit =
            new com.google.api.services.dataflow.model.Position();
        positionToSplit.setConcatPosition(
            new ConcatPosition().setIndex((int) rangeTracker.getStopPosition().longValue()));
        return new DynamicSplitResultWithPosition(
            SourceTranslationUtils.cloudPositionToReaderPosition(positionToSplit));
      } else {
        LOG.debug("Could not perform the dynamic split request " + splitRequest);
        return null;
      }
    }
  }
}
