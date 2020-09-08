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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.ReportedParallelism;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A source that reads PCollections that have been materialized as Avro files. Records are read from
 * the Avro file as a series of byte arrays. The coder provided is used to deserialize each record
 * from a byte array.
 *
 * @param <T> the type of the elements read from the source
 */
public class AvroByteReader<T> extends NativeReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroByteReader.class);

  final long startPosition;
  final long endPosition;
  final String filename;
  final AvroSource<ByteBuffer> avroSource;
  final PipelineOptions options;

  final Coder<T> coder;
  private final Schema schema = Schema.create(Schema.Type.BYTES);

  AvroByteReader(
      String filename,
      long startPosition,
      long endPosition,
      Coder<T> coder,
      PipelineOptions options) {
    checkArgument(filename != null, "filename must not be null");
    checkArgument(coder != null, "coder must not be null");
    checkArgument(options != null, "options must not be null");
    this.filename = filename;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.coder = coder;
    this.options = options;
    this.avroSource =
        (AvroSource<ByteBuffer>) ((AvroSource) AvroSource.from(filename).withSchema(schema));
  }

  @Override
  public AvroByteFileIterator iterator() throws IOException {
    BoundedSource.BoundedReader<ByteBuffer> reader;
    if (startPosition == 0 && endPosition == Long.MAX_VALUE) {
      // Read entire file (or collection of files).
      reader = avroSource.createReader(options);
    } else {
      // Read a subrange of file.
      reader =
          avroSource
              .createForSubrangeOfFile(
                  FileSystems.matchSingleFileSpec(filename), startPosition, endPosition)
              .createReader(options);
    }
    return new AvroByteFileIterator((AvroSource.AvroReader<ByteBuffer>) reader);
  }

  class AvroByteFileIterator extends NativeReaderIterator<T> {
    private final AvroSource.AvroReader<ByteBuffer> reader;
    private Optional<T> current;

    public AvroByteFileIterator(AvroSource.AvroReader<ByteBuffer> reader) {
      this.reader = reader;
    }

    @Override
    public boolean start() throws IOException {
      if (!reader.start()) {
        current = Optional.empty();
        return false;
      }
      updateCurrent();
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      if (!reader.advance()) {
        current = Optional.empty();
        return false;
      }
      updateCurrent();
      return true;
    }

    private void updateCurrent() throws IOException {
      ByteBuffer inBuffer = reader.getCurrent();
      notifyElementRead(inBuffer.remaining());
      byte[] encodedElem = new byte[inBuffer.remaining()];
      inBuffer.get(encodedElem);
      current = Optional.of(CoderUtils.decodeFromByteArray(coder, encodedElem));
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current.get();
    }

    @Override
    public Progress getProgress() {
      Double readerProgress = reader.getFractionConsumed();
      if (readerProgress == null) {
        return null;
      }
      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      progress.setFractionConsumed(readerProgress);
      double consumedParallelism = reader.getSplitPointsConsumed();
      double remainingParallelism = reader.getSplitPointsRemaining();
      progress.setConsumedParallelism(new ReportedParallelism().setValue(consumedParallelism));
      if (remainingParallelism >= 0) {
        progress.setRemainingParallelism(new ReportedParallelism().setValue(remainingParallelism));
      }
      return SourceTranslationUtils.cloudProgressToReaderProgress(progress);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      ApproximateSplitRequest splitProgress =
          SourceTranslationUtils.splitRequestToApproximateSplitRequest(splitRequest);
      double splitAtFraction = splitProgress.getFractionConsumed();
      LOG.info("Received request for dynamic split at {}", splitAtFraction);
      OffsetBasedSource<ByteBuffer> residual = reader.splitAtFraction(splitAtFraction);
      if (residual == null) {
        LOG.info("Rejected split request for split at {}", splitAtFraction);
        return null;
      }
      com.google.api.services.dataflow.model.Position acceptedPosition =
          new com.google.api.services.dataflow.model.Position();
      acceptedPosition.setByteOffset(residual.getStartOffset());
      LOG.info(
          "Accepted split for position {} which resulted in a new source with byte offset {}",
          splitAtFraction,
          residual.getStartOffset());
      return new DynamicSplitResultWithPosition(
          SourceTranslationUtils.cloudPositionToReaderPosition(acceptedPosition));
    }
  }
}
