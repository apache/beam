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
import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.cloud.dataflow.sdk.io.AvroSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.OffsetBasedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.AvroUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * A reader for Avro files exported by BigQuery, which converts them to {@link TableRow} records.
 */
public class BigQueryAvroReader extends NativeReader<WindowedValue<TableRow>> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryAvroReader.class);

  @Nullable
  final Long startPosition;
  @Nullable
  final Long endPosition;
  final String filename;
  final AvroSource<GenericRecord> avroSource;
  final TableSchema tableSchema;
  final PipelineOptions options;

  public BigQueryAvroReader(
      String filename,
      @Nullable Long startPosition,
      @Nullable Long endPosition,
      TableSchema schema,
      @Nullable PipelineOptions options)
      throws IOException {
    this.filename = filename;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    Schema recordSchema =
        new Parser().parse(AvroUtils.readMetadataFromFile(filename).getSchemaString());
    this.tableSchema = schema;
    this.options = options;
    this.avroSource = AvroSource.from(filename).withSchema(recordSchema);
  }

  @Override
  public BigQueryAvroFileIterator iterator() throws IOException {
    Long endPosition = firstNonNull(this.endPosition, Long.MAX_VALUE);
    Long startPosition = firstNonNull(this.startPosition, 0L);
    BoundedSource.BoundedReader<GenericRecord> reader;
    if (startPosition == 0 && endPosition == Long.MAX_VALUE) {
      // Read entire file (or collection of files).
      reader = avroSource.createReader(options);
    } else {
      // Read a subrange of file.
      reader = avroSource.createForSubrangeOfFile(filename, startPosition, endPosition)
          .createReader(options);
    }
    return new BigQueryAvroFileIterator((AvroSource.AvroReader<GenericRecord>) reader);
  }

  class BigQueryAvroFileIterator extends LegacyReaderIterator<WindowedValue<TableRow>> {
    final AvroSource.AvroReader<GenericRecord> reader;
    boolean hasStarted = false;
    long blockOffset = -1;

    public BigQueryAvroFileIterator(AvroSource.AvroReader<GenericRecord> reader) {
      this.reader = reader;
    }

    @Override
    protected WindowedValue<TableRow> nextImpl() throws IOException {
      GenericRecord next = reader.getCurrent();
      // Coarse-grained reporting of input bytes consumed.
      // After completing reading a block, the block offset changes.
      long currentOffset = reader.getCurrentBlockOffset();
      if (currentOffset != blockOffset) {
        notifyElementRead(reader.getCurrentBlockSize());
        blockOffset = currentOffset;
      }
      return WindowedValue.valueInGlobalWindow(
          AvroUtils.convertGenericRecordToTableRow(next, tableSchema));
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
      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      progress.setFractionConsumed(readerProgress);
      return cloudProgressToReaderProgress(progress);
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
      OffsetBasedSource<GenericRecord> residual = reader.splitAtFraction(splitAtFraction);
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
