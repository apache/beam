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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
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

/** Does an external sort of the provided values using Hadoop's {@link SequenceFile}. */
class ExternalSorter implements Sorter {
  private Options options;

  /** Whether {@link #sort()} was already called. */
  private boolean sortCalled = false;

  /** SequenceFile Writer for writing all input data to a file. */
  private Writer writer;

  /** Sorter used to sort the input file. */
  private SequenceFile.Sorter sorter;

  /** Temporary directory for input and intermediate files. */
  private Path tempDir;

  /** The list of input files to be sorted. */
  private Path[] paths;

  private boolean initialized = false;

  /** {@link Options} contains configuration of the sorter. */
  public static class Options implements Serializable {
    private String tempLocation = "/tmp";
    private int memoryMB = 100;

    /** Sets the path to a temporary location where the sorter writes intermediate files. */
    public Options setTempLocation(String tempLocation) {
      if (tempLocation.startsWith("gs://")) {
        throw new IllegalArgumentException("Sorter doesn't support GCS temporary location.");
      }

      this.tempLocation = tempLocation;
      return this;
    }

    /** Returns the configured temporary location. */
    public String getTempLocation() {
      return tempLocation;
    }

    /** Sets the size of the memory buffer in megabytes. */
    public Options setMemoryMB(int memoryMB) {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      this.memoryMB = memoryMB;
      return this;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }
  }

  /** Returns a {@link Sorter} configured with the given {@link Options}. */
  public static ExternalSorter create(Options options) {
    return new ExternalSorter(options);
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    checkState(!sortCalled, "Records can only be added before sort()");

    initHadoopSorter();

    BytesWritable key = new BytesWritable(record.getKey());
    BytesWritable value = new BytesWritable(record.getValue());

    writer.append(key, value);
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    checkState(!sortCalled, "sort() can only be called once.");
    sortCalled = true;

    initHadoopSorter();

    writer.close();

    return new SortedRecordsIterable();
  }

  private ExternalSorter(Options options) {
    this.options = options;
  }

  /**
   * Initializes the hadoop sorter. Does some local file system setup, and is somewhat expensive
   * (~20 ms on local machine). Only executed when necessary.
   */
  private void initHadoopSorter() throws IOException {
    if (!initialized) {
      tempDir = new Path(options.getTempLocation(), "tmp" + UUID.randomUUID().toString());
      paths = new Path[] {new Path(tempDir, "test.seq")};

      JobConf conf = new JobConf();
      writer =
          SequenceFile.createWriter(
              conf,
              Writer.valueClass(BytesWritable.class),
              Writer.keyClass(BytesWritable.class),
              Writer.file(paths[0]),
              Writer.compression(CompressionType.NONE));

      FileSystem fs = FileSystem.getLocal(conf);
      sorter =
          new SequenceFile.Sorter(
              fs, new BytesWritable.Comparator(), BytesWritable.class, BytesWritable.class, conf);
      sorter.setMemory(options.getMemoryMB() * 1024 * 1024);

      initialized = true;
    }
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
    private KV<byte[], byte[]> nextKV = null;

    SortedRecordsIterator() {
      try {
        this.iterator = sorter.sortAndIterate(paths, tempDir, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      nextKV = KV.of(null, null); // A dummy value that will be overwritten by next().
      next();
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

      KV<byte[], byte[]> current = nextKV;

      try {
        if (iterator.next()) {
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

          nextKV = KV.of(key.copyBytes(), value.copyBytes());
        } else {
          nextKV = null;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Iterator does not support remove");
    }
  }
}
