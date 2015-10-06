/*
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
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.OffsetBasedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.AbstractBoundedReaderIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * A source that reads Avro files.
 *
 * @param <T> the type of the elements read from the source
 */
public class AvroReader<T> extends Reader<WindowedValue<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroReader.class);

  @Nullable
  final Long startPosition;
  @Nullable
  final Long endPosition;
  final String filename;
  final AvroSource<T> avroSource;
  final AvroCoder<T> avroCoder;
  final PipelineOptions options;

  @SuppressWarnings("unchecked")
  public AvroReader(String filename, @Nullable Long startPosition, @Nullable Long endPosition,
      AvroCoder<T> coder, @Nullable PipelineOptions options) {

    this.avroCoder = coder;

    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.filename = filename;
    this.options = options;

    Class<T> type = avroCoder.getType();
    AvroSource<T> source;
    if (type.equals(GenericRecord.class)) {
      source = (AvroSource<T>) AvroSource.from(filename).withSchema(avroCoder.getSchema());
    } else {
      source = AvroSource.from(filename).withSchema(type);
    }


    this.avroSource = source;
  }

  @Override
  public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
    Long endPosition = this.endPosition;
    Long startPosition = this.startPosition;
    if (endPosition == null) {
      endPosition = Long.MAX_VALUE;
    }
    if (startPosition == null) {
      startPosition = 0L;
    }
    BoundedSource.BoundedReader<T> reader;
    if (startPosition == 0 && endPosition == Long.MAX_VALUE) {
      // Read entire file (or collection of files).
      reader = avroSource.createReader(options);
    } else {
      // Read a subrange of file.
      reader = avroSource.createForSubrangeOfFile(filename, startPosition, endPosition)
          .createReader(options);
    }
    return new AvroFileIterator((AvroSource.AvroReader<T>) reader);
  }

  class AvroFileIterator extends AbstractBoundedReaderIterator<WindowedValue<T>> {
    final AvroSource.AvroReader<T> reader;
    boolean hasStarted = false;
    long blockOffset = -1;

    public AvroFileIterator(AvroSource.AvroReader<T> reader) {
      this.reader = reader;
    }

    @Override
    protected WindowedValue<T> nextImpl() throws IOException {
      T next = reader.getCurrent();
      // Coarse-grained reporting of input bytes consumed.
      // After completing reading a block, the block offset changes.
      long currentOffset = reader.getCurrentBlockOffset();
      if (currentOffset != blockOffset) {
        notifyElementRead(reader.getCurrentBlockSize());
        blockOffset = currentOffset;
      }
      return WindowedValue.valueInGlobalWindow(next);
    }

    @Override
    protected boolean hasNextImpl() throws IOException {
      if (!hasStarted) {
        hasStarted = true;
        return reader.start();
      }
      return reader.advance();
    }

    @Override
    public Progress getProgress() {
      Double readerProgress = reader.getFractionConsumed();
      if (readerProgress == null) {
        return null;
      }
      ApproximateProgress progress = new ApproximateProgress();
      progress.setPercentComplete(readerProgress.floatValue());
      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      ApproximateProgress splitProgress =
          SourceTranslationUtils.splitRequestToApproximateProgress(splitRequest);
      double splitAtFraction = splitProgress.getPercentComplete();
      LOG.info("Received request for dynamic split at {}", splitAtFraction);
      OffsetBasedSource<T> residual = reader.splitAtFraction(splitAtFraction);
      if (residual == null) {
        LOG.info("Rejected split request for split at {}", splitAtFraction);
        return null;
      }
      com.google.api.services.dataflow.model.Position acceptedPosition =
          new com.google.api.services.dataflow.model.Position();
      acceptedPosition.setByteOffset(residual.getStartOffset());
      LOG.info("Accepted split for position {} which resulted in a new source with byte offset {}",
          splitAtFraction, residual.getStartOffset());
      return new DynamicSplitResultWithPosition(
          SourceTranslationUtils.cloudPositionToReaderPosition(acceptedPosition));
    }
  }
}
