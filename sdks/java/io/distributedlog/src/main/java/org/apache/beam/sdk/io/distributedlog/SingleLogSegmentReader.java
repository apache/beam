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
package org.apache.beam.sdk.io.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.*;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.ExposedByteArrayInputStream;

/**
 * Reader to read records for a single log segment.
 */
class SingleLogSegmentReader<R> {

  static final int NUM_RECORDS_PER_BATCH = 100;

  private final DistributedLogNamespace namespace;
  private final LogSegmentBundle segment;

  // kv coder
  private final Coder<R> coder;

  // Reader State
  private DistributedLogManager logStream;
  private AsyncLogReader reader;
  private R currentR;
  private DLSN lastDLSN;
  private boolean done = false;
  private int numReadRecords = 0;
  private int totalRecords = 0;
  private Iterator<LogRecordWithDLSN> currentRecordBatch;

  SingleLogSegmentReader(DistributedLogNamespace namespace,
                         LogSegmentBundle segment,
                         Coder<R> coder) {
    this.namespace = namespace;
    this.segment = segment;
    this.coder = coder;
  }

  @VisibleForTesting
  DLSN getLastDLSN() {
    return lastDLSN;
  }

  @VisibleForTesting
  int getTotalRecords() {
    return totalRecords;
  }

  @VisibleForTesting
  AsyncLogReader getReader() {
    return reader;
  }

  @VisibleForTesting
  DistributedLogManager getLogStream() {
    return logStream;
  }

  boolean start() throws IOException {
    doStart();
    return advance();
  }

  double getProgress() {
    if (totalRecords == 0) {
      return 1.0;
    }
    return numReadRecords * 1.0 / totalRecords;
  }

  private void doStart() throws IOException {
    logStream = namespace.openLog(segment.getStreamName());
    if (segment.getMetadata().isInProgress()) {
      LogRecordWithDLSN lastRecord;
      try {
        lastRecord = logStream.getLastLogRecord();
      } catch (LogEmptyException | LogNotFoundException e) {
        // no records in the stream
        currentR = null;
        done = true;
        return;
      }
      lastDLSN = lastRecord.getDlsn();
      totalRecords = lastRecord.getPositionWithinLogSegment();
    } else if (segment.getMetadata().getRecordCount() == 0) {
      // the log segment is an empty one
      currentR = null;
      done = true;
      return;
    } else {
      lastDLSN = segment.getMetadata().getLastDLSN();
      totalRecords = segment.getMetadata().getRecordCount();
    }
    DLSN firstDLSN = segment.getMetadata().getFirstDLSN();
    reader = FutureUtils.result(logStream.openAsyncLogReader(firstDLSN));
  }

  boolean advance() throws IOException {
    if (done) {
      currentR = null;
      return false;
    }
    LogRecordWithDLSN record = null;
    while (null == record) {
      if (null == currentRecordBatch) {
        currentRecordBatch =
            FutureUtils.result(reader.readBulk(NUM_RECORDS_PER_BATCH)).iterator();
      }
      if (currentRecordBatch.hasNext()) {
        record = currentRecordBatch.next();
      } else {
        currentRecordBatch = null;
      }
    }
    int diff = record.getDlsn().compareTo(lastDLSN);
    if (diff < 0) {
      currentR = getR(record);
      ++numReadRecords;
      return true;
    } else if (diff > 0) {
      currentR = null;
      done = true;
      return false;
    } else  {
      currentR = getR(record);
      done = true;
      ++numReadRecords;
      return true;
    }
  }

  private R getR(LogRecordWithDLSN record) throws IOException {
    return coder.decode(getRecordStream(record), Coder.Context.OUTER);
  }

  private static InputStream getRecordStream(LogRecordWithDLSN record) {
    return new ExposedByteArrayInputStream(record.getPayload());
  }

  R getCurrentR() throws NoSuchElementException {
    return currentR;
  }

  void close() throws IOException {
    if (null != reader) {
      Utils.close(reader);
    }
    if (null != logStream) {
      logStream.close();
    }
  }

}
