/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.forkRequestToApproximateProgress;
import static java.lang.Math.min;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

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
  class InMemoryReaderIterator extends AbstractReaderIterator<T> {
    int index;
    int endPosition;

    public InMemoryReaderIterator() {
      index = startIndex;
      endPosition = endIndex;
    }

    @Override
    public boolean hasNext() {
      return index < endPosition;
    }

    @Override
    public T next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      String encodedElementString = encodedElements.get(index++);
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
      currentPosition.setRecordIndex((long) index);

      ApproximateProgress progress = new ApproximateProgress();
      progress.setPosition(currentPosition);

      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public ForkResult requestFork(ForkRequest forkRequest) {
      checkNotNull(forkRequest);

      com.google.api.services.dataflow.model.Position forkPosition =
          forkRequestToApproximateProgress(forkRequest).getPosition();
      if (forkPosition == null) {
        LOG.warn("InMemoryReader only supports fork at a Position. Requested: {}", forkRequest);
        return null;
      }

      Long forkIndex = forkPosition.getRecordIndex();
      if (forkIndex == null) {
        LOG.warn("InMemoryReader only supports fork at a record index. Requested: {}",
            forkPosition);
        return null;
      }
      if (forkIndex <= index) {
        LOG.info("Already progressed to index {} which is after the requested fork index {}",
            index, forkIndex);
        return null;
      }
      if (forkIndex >= endPosition) {
        throw new IllegalArgumentException(
            "Fork requested at an index beyond the end of the current range: " + forkIndex
            + " >= " + endPosition);
      }

      this.endPosition = forkIndex.intValue();
      LOG.info("Forked InMemoryReader at index {}", forkIndex);

      return new ForkResultWithPosition(cloudPositionToReaderPosition(forkPosition));
    }
  }
}
