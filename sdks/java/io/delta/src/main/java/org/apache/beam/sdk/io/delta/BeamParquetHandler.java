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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader.BatchReadSupport;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.MetadataColumnSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * A Beam specific {@link ParquetHandler} that delegates row group claiming to a {@link
 * DeltaReadTaskTracker}.
 */
public class BeamParquetHandler implements ParquetHandler {
  private final Configuration conf;
  private final ParquetHandler delegate;
  private final RestrictionTracker<OffsetRange, Long> tracker;
  private static final long DEFAULT_START_RG_INDEX = 0L;

  public BeamParquetHandler(
      Configuration conf, ParquetHandler delegate, RestrictionTracker<OffsetRange, Long> tracker) {
    this.conf = conf;
    this.delegate = delegate;
    this.tracker = tracker;
  }

  private boolean claimFailed = false;

  /**
   * A method that is expected to be called after the first file processing is done. It returns
   * whether the last file process resulted in a claim failure. This allows the caller to skip
   * trying to read the remaining files of the task which would result in claim failures for each
   * row group within them.
   *
   * @return true, if the last file process resulted in a claim failure. Returns false otherwise.
   */
  public boolean hasClaimFailed() {
    return claimFailed;
  }

  @Override
  public CloseableIterator<FileReadResult> readParquetFiles(
      CloseableIterator<FileStatus> fileIter,
      StructType physicalSchema,
      Optional<Predicate> predicate)
      throws IOException {
    return readParquetFiles(fileIter, physicalSchema, predicate, DEFAULT_START_RG_INDEX);
  }

  /**
   * Reads Parquet files starting from a given row group index.
   *
   * <p>This takes the {@code RestrictionTracker} referenced by the current {@code ParquetReader}
   * into consideration when reading by performing the following.
   *
   * <p>* Skips blocks of the set of files till the given start row group index or the start point
   * of the {@code RestrictionTracker}, whatever is higher. * Invokes {@code tryClaim} when reading
   * a specific block stops reading if a {@code tryClaim} fails. * Stops reading if the end of the
   * range of the {@code RestrictionTracker} is reached.
   *
   * <p>If {@code tryClaim} fails during reading, subsequent {@code hasClaimFailed} calls will
   * return {@code true}, so the caller can skip reading subsequent files that are in the range
   * being considered for reading.
   */
  public CloseableIterator<FileReadResult> readParquetFiles(
      CloseableIterator<FileStatus> fileIter,
      StructType physicalSchema,
      Optional<Predicate> predicate,
      long startRgIndex)
      throws IOException {

    List<CloseableIterator<FileReadResult>> results = new ArrayList<>();
    boolean hasRowIndexCol = physicalSchema.contains(MetadataColumnSpec.ROW_INDEX);

    long currentRgIndex = startRgIndex;

    try {
      while (fileIter.hasNext()) {
        if (currentRgIndex >= tracker.currentRestriction().getTo()) {
          // Skipping all blocks for the remaining files since they are located after the
          // end index of the tracker. Since currentRgIndex is monotonically increasing,
          // we can break the loop immediately to avoid extremely expensive network I/O.
          break;
        }

        FileStatus fileStatus = fileIter.next();
        Path hadoopPath = new Path(fileStatus.getPath());
        ParquetMetadata metadata =
            ParquetFileReader.readFooter(conf, hadoopPath, ParquetMetadataConverter.NO_FILTER);
        long fileBlocks = metadata.getBlocks().size();

        if (currentRgIndex + fileBlocks <= tracker.currentRestriction().getFrom()) {
          // Skipping all blocks for the current file since they are located before the
          // start index of the tracker.
          currentRgIndex += fileBlocks;
          continue;
        }

        results.add(
            readParquetFileDirect(
                fileStatus,
                hadoopPath,
                metadata,
                physicalSchema,
                hasRowIndexCol,
                currentRgIndex,
                fileBlocks));

        currentRgIndex += fileBlocks;
      }
    } finally {
      fileIter.close();
    }

    return combineResults(results);
  }

  // Reads the correct set of blocks that belong to the given Parquet file that
  // are within range for the current `RestrictionTracker`. If the current file
  // has some blocks that are within the tracker's range and some that are
  // outside,
  // this will only read the blocks that are within the range.
  private CloseableIterator<FileReadResult> readParquetFileDirect(
      FileStatus fileStatus,
      Path hadoopPath,
      ParquetMetadata metadata,
      StructType physicalSchema,
      boolean hasRowIndexCol,
      long startRgIndex,
      long fileBlocks) {

    return new CloseableIterator<FileReadResult>() {
      @javax.annotation.Nullable private ParquetFileReader reader = null;
      @javax.annotation.Nullable private BatchReadSupport readSupport = null;
      @javax.annotation.Nullable private RecordMaterializer<Object> recordConverter = null;
      @javax.annotation.Nullable private MessageColumnIO columnIO = null;

      private long currentRgOffset = 0;
      @javax.annotation.Nullable private RecordReader<Object> currentRecordReader = null;
      private long currentRgTotalRows = 0;
      private long currentRgRowOffset = 0;
      private long currentRgStartingRowIndex = 0;

      @javax.annotation.Nullable private FileReadResult nextResult = null;
      private boolean isDone = false;

      private void initReaderIfRequired() throws IOException {
        if (reader != null) {
          return;
        }
        HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);
        ParquetFileReader localReader = ParquetFileReader.open(inputFile);
        reader = localReader;

        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        Map<String, Set<String>> keyValueMetadata = new HashMap<>();
        if (fileMetaData.getKeyValueMetaData() != null) {
          for (Map.Entry<String, String> entry : fileMetaData.getKeyValueMetaData().entrySet()) {
            keyValueMetadata.put(entry.getKey(), Collections.singleton(entry.getValue()));
          }
        }

        BatchReadSupport localReadSupport = new BatchReadSupport(1024, physicalSchema);
        readSupport = localReadSupport;
        ReadSupport.ReadContext readContext =
            localReadSupport.init(new InitContext(conf, keyValueMetadata, fileSchema));
        RecordMaterializer<Object> localRecordConverter =
            localReadSupport.prepareForRead(
                conf, fileMetaData.getKeyValueMetaData(), fileSchema, readContext);
        recordConverter = localRecordConverter;
        localReader.setRequestedSchema(readContext.getRequestedSchema());

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(fileMetaData.getCreatedBy());
        columnIO = columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema, true);
      }

      @Override
      public boolean hasNext() {
        if (isDone) {
          return false;
        }
        if (nextResult != null) {
          return true;
        }

        try {
          initReaderIfRequired();
          ParquetFileReader localReader = reader;
          BatchReadSupport localReadSupport = readSupport;
          MessageColumnIO localColumnIO = columnIO;
          RecordMaterializer<Object> localRecordConverter = recordConverter;
          if (localReader == null
              || localReadSupport == null
              || localColumnIO == null
              || localRecordConverter == null) {
            throw new IllegalStateException("Reader not initialized");
          }

          while (true) {
            RecordReader<Object> localRecordReader = currentRecordReader;
            if (localRecordReader != null && currentRgRowOffset < currentRgTotalRows) {
              int batchSize = (int) Math.min(1024L, currentRgTotalRows - currentRgRowOffset);
              for (int i = 0; i < batchSize; i++) {
                localRecordReader.read();
                long rowIndex =
                    hasRowIndexCol ? (currentRgStartingRowIndex + currentRgRowOffset + i) : -1L;
                localReadSupport.finalizeCurrentRow(rowIndex);
              }
              currentRgRowOffset += batchSize;
              io.delta.kernel.data.ColumnarBatch batch =
                  localReadSupport.getDataAsColumnarBatch(batchSize);
              nextResult = new FileReadResult(batch, fileStatus.getPath());
              return true;
            }

            currentRecordReader = null;
            if (currentRgOffset >= fileBlocks) {
              isDone = true;
              return false;
            }

            // Checking the range for specific row groups.
            long rgIndex = startRgIndex + currentRgOffset;
            if (rgIndex < tracker.currentRestriction().getFrom()) {
              // Skip till we get to the first block to read.
              localReader.skipNextRowGroup();
              currentRgOffset++;
              continue;
            } else if (rgIndex >= tracker.currentRestriction().getTo()) {
              // Once we are past the end index of the tracker we don't have to read any more
              // blocks.
              isDone = true;
              return false;
            }

            // We only read the row group if it's within the range for the
            // RestrictionTracker.
            if (tracker.tryClaim(rgIndex)) {
              PageReadStore pages = localReader.readNextRowGroup();
              currentRecordReader =
                  localColumnIO.getRecordReader(pages, localRecordConverter, FilterCompat.NOOP);
              currentRgTotalRows = pages.getRowCount();
              currentRgRowOffset = 0;
              currentRgStartingRowIndex = pages.getRowIndexOffset().orElse(0L);
              currentRgOffset++;
            } else {
              // Mark claim failed for the current row group, so we stop processing
              // the remaining row groups in the source.
              claimFailed = true;
              isDone = true;
              return false;
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public FileReadResult next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        FileReadResult res = nextResult;
        if (res == null) {
          throw new NoSuchElementException();
        }
        nextResult = null;
        return res;
      }

      @Override
      public void close() throws IOException {
        if (reader != null) {
          reader.close();
        }
      }
    };
  }

  @Override
  public void writeParquetFileAtomically(
      String filePath, CloseableIterator<FilteredColumnarBatch> data) throws IOException {
    delegate.writeParquetFileAtomically(filePath, data);
  }

  @Override
  public CloseableIterator<DataFileStatus> writeParquetFiles(
      String filePath, CloseableIterator<FilteredColumnarBatch> data, List<Column> statsColumns)
      throws IOException {
    return delegate.writeParquetFiles(filePath, data, statsColumns);
  }

  private static CloseableIterator<FileReadResult> combineResults(
      List<CloseableIterator<FileReadResult>> iterators) {
    return new CloseableIterator<FileReadResult>() {
      private int currentIdx = 0;

      @Override
      public boolean hasNext() {
        while (currentIdx < iterators.size()) {
          if (iterators.get(currentIdx).hasNext()) {
            return true;
          }
          currentIdx++;
        }
        return false;
      }

      @Override
      public FileReadResult next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return iterators.get(currentIdx).next();
      }

      @Override
      public void close() throws IOException {
        for (CloseableIterator<FileReadResult> it : iterators) {
          it.close();
        }
      }
    };
  }
}
