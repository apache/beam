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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.TestSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;

public class TestBoundedCountingSource extends BoundedSource<KV<Integer, Integer>>
    implements TestSource {
  private final int totalNumRecords;
  private final TestCountingSource source;
  private final List<TestReader> createdReaders;
  private int nextValueForValidating;
  private long nextTimestampForValidating;

  public TestBoundedCountingSource(int shardNum, int totalNumRecords) {
    this.totalNumRecords = totalNumRecords;
    this.source =
        new TestCountingSource(totalNumRecords).withShardNumber(shardNum).withFixedNumSplits(1);
    this.createdReaders = new ArrayList<>();
    this.nextValueForValidating = 0;
    this.nextTimestampForValidating = 0;
  }

  @Override
  public List<? extends BoundedSource<KV<Integer, Integer>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    List<TestBoundedCountingSource> splits = new ArrayList<>();
    int numRecordsAssigned = 0;
    int shardNum = 0;
    while (numRecordsAssigned < totalNumRecords) {
      int numRecordsForSplit =
          (int) Math.min(totalNumRecords - numRecordsAssigned, desiredBundleSizeBytes);
      splits.add(new TestBoundedCountingSource(shardNum, numRecordsForSplit));
      numRecordsAssigned += numRecordsForSplit;
      shardNum++;
    }
    return splits;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return totalNumRecords;
  }

  @Override
  public BoundedReader<KV<Integer, Integer>> createReader(PipelineOptions options)
      throws IOException {
    BoundedTestCountingSourceReader reader =
        new BoundedTestCountingSourceReader(source.createReader(options, null), this);
    createdReaders.add(reader);
    return reader;
  }

  @Override
  public List<TestReader> createdReaders() {
    return createdReaders;
  }

  @Override
  public boolean validateNextValue(int value) {
    boolean result = value == nextValueForValidating;
    nextValueForValidating++;
    return result;
  }

  @Override
  public boolean validateNextTimestamp(long timestamp) {
    boolean result = timestamp == nextTimestampForValidating;
    nextTimestampForValidating++;
    return result;
  }

  @Override
  public boolean isConsumptionCompleted() {
    return nextValueForValidating == totalNumRecords;
  }

  @Override
  public boolean allTimestampsReceived() {
    return nextTimestampForValidating == nextValueForValidating;
  }

  public static class BoundedTestCountingSourceReader
      extends BoundedSource.BoundedReader<KV<Integer, Integer>> implements TestReader {

    private final TestCountingSource.CountingSourceReader reader;
    private final TestBoundedCountingSource currentSource;
    private boolean closed;

    private BoundedTestCountingSourceReader(
        TestCountingSource.CountingSourceReader reader, TestBoundedCountingSource currentSource) {
      this.reader = reader;
      this.currentSource = currentSource;
      this.closed = false;
    }

    @Override
    public boolean start() throws IOException {
      return reader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public KV<Integer, Integer> getCurrent() throws NoSuchElementException {
      return reader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      closed = true;
      reader.close();
    }

    @Override
    public BoundedSource<KV<Integer, Integer>> getCurrentSource() {
      return currentSource;
    }

    @Override
    public boolean isClosed() {
      return closed;
    }
  }
}
