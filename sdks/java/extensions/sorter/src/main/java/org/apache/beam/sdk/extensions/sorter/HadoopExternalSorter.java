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
package org.apache.beam.sdk.extensions.sorter;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Does an external sort of the provided values using Hadoop's {@link SequenceFile}. */
class HadoopExternalSorter extends ExternalSorter {

  /** Whether {@link #sort()} was already called. */
  private boolean sortCalled = false;

  /** SequenceFile Writer for writing all input data to a file. */
  private @MonotonicNonNull Writer writer = null;

  /** Sorter used to sort the input file. */
  private SequenceFile.@MonotonicNonNull Sorter sorter = null;

  private @MonotonicNonNull JobConf conf;

  /** Temporary directory for input and intermediate files. */
  private Path tempDir;

  /** The list of input files to be sorted. */
  private Path[] paths;

  /** Returns a {@link Sorter} configured with the given {@link Options}. */
  public static HadoopExternalSorter create(Options options) {
    return new HadoopExternalSorter(options);
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    checkState(!sortCalled, "Records can only be added before sort()");
    BytesWritable key = new BytesWritable(record.getKey());
    BytesWritable value = new BytesWritable(record.getValue());
    getWriter().append(key, value);
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    checkState(!sortCalled, "sort() can only be called once.");
    sortCalled = true;
    getWriter().close();
    return new SortedRecordsIterable();
  }

  private HadoopExternalSorter(Options options) {
    super(options);
    tempDir = new Path(options.getTempLocation(), "tmp" + UUID.randomUUID().toString());
    paths = new Path[] {new Path(tempDir, "test.seq")};
  }

  private JobConf getConf() {
    if (conf == null) {
      // This call is expensive in the constructor (check
      // BufferedExternalSorterTest.testManySortersFewRecords)
      conf = new JobConf();
      // Sets directory for intermediate files created during merge of merge sort
      conf.set("io.seqfile.local.dir", tempDir.toUri().getPath());
    }
    return conf;
  }

  /**
   * Initializes the writer. Does some local file system setup, and is somewhat expensive (~20 ms on
   * local machine). Only executed when necessary.
   */
  private Writer getWriter() throws IOException {
    if (writer == null) {
      writer =
          SequenceFile.createWriter(
              getConf(),
              Writer.valueClass(BytesWritable.class),
              Writer.keyClass(BytesWritable.class),
              Writer.file(paths[0]),
              Writer.compression(CompressionType.NONE));

      FileSystem fs = FileSystem.getLocal(getConf());
      // Directory has to exist for Hadoop to recognize it as deletable on exit
      fs.mkdirs(tempDir);
      fs.deleteOnExit(tempDir);
    }
    return writer;
  }

  /** Sorter used to sort the input file. */
  private SequenceFile.Sorter getSorter() throws IOException {
    if (sorter == null) {
      FileSystem fs = FileSystem.getLocal(getConf());
      sorter =
          new SequenceFile.Sorter(
              fs,
              new BytesWritable.Comparator(),
              BytesWritable.class,
              BytesWritable.class,
              getConf());
      sorter.setMemory(options.getMemoryMB() * 1024 * 1024);
    }
    return sorter;
  }

  /** An {@link Iterable} producing the iterators over sorted data. */
  private class SortedRecordsIterable implements Iterable<KV<byte[], byte[]>> {
    @Override
    public Iterator<KV<byte[], byte[]>> iterator() {
      return new SortedRecordsIterator();
    }
  }

  /** An {@link Iterator} producing the sorted data. */
  private class SortedRecordsIterator implements Iterator<KV<byte[], byte[]>> {
    private RawKeyValueIterator iterator;

    /** Next {@link KV} to return from {@link #next()}. */
    private @Nullable KV<byte[], byte[]> nextKV;

    SortedRecordsIterator() {
      try {
        this.iterator = getSorter().sortAndIterate(paths, tempDir, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      nextKV = readKeyValueOrFail(iterator);
    }

    @Override
    public boolean hasNext() {
      return nextKV != null;
    }

    @Override
    public KV<byte[], byte[]> next() {
      if (nextKV == null) {
        throw new NoSuchElementException();
      }

      KV<byte[], byte[]> r = nextKV;
      nextKV = readKeyValueOrFail(iterator);
      return r;
    }
  }

  private @Nullable KV<byte[], byte[]> readKeyValueOrFail(RawKeyValueIterator iterator) {
    try {
      return readKeyValue(iterator);
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private @Nullable KV<byte[], byte[]> readKeyValue(RawKeyValueIterator iterator)
      throws IOException {
    if (!iterator.next()) {
      return null;
    }

    // Parse key from DataOutputBuffer.
    ByteArrayInputStream keyStream = new ByteArrayInputStream(iterator.getKey().getData());
    BytesWritable key = new BytesWritable();
    key.readFields(new DataInputStream(keyStream));

    // Parse value from ValueBytes.
    ByteArrayOutputStream valOutStream = new ByteArrayOutputStream();
    iterator.getValue().writeUncompressedBytes(new DataOutputStream(valOutStream));
    ByteArrayInputStream valInStream = new ByteArrayInputStream(valOutStream.toByteArray());
    BytesWritable value = new BytesWritable();
    value.readFields(new DataInputStream(valInStream));

    return KV.of(key.copyBytes(), value.copyBytes());
  }
}
