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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * External Sorter based on <a
 * href="https://github.com/lemire/externalsortinginjava">lemire/externalsortinginjava</a>.
 */
class NativeFileSorter {

  private static final Logger LOG = LoggerFactory.getLogger(NativeFileSorter.class);

  private static final int MAX_TEMP_FILES = 1024;
  private static final long OBJECT_OVERHEAD = getObjectOverhead();

  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  private static final Comparator<KV<byte[], byte[]>> KV_COMPARATOR =
      (x, y) -> COMPARATOR.compare(x.getKey(), y.getKey());
  private static final ByteArrayCoder CODER = ByteArrayCoder.of();

  private final Path tempDir;
  private final long maxMemory;
  private final File dataFile;
  private final OutputStream dataStream;

  private boolean sortCalled = false;

  /** Create a new file sorter. */
  public NativeFileSorter(Path tempDir, long maxMemory) throws IOException {
    this.tempDir = tempDir;
    this.maxMemory = maxMemory;

    this.dataFile = Files.createTempFile(tempDir, "input", "seq").toFile();
    this.dataStream = new BufferedOutputStream(new FileOutputStream(dataFile));
    dataFile.deleteOnExit();

    LOG.debug("Created input file {}", dataFile);
  }

  /**
   * Adds a given record to the sorter.
   *
   * <p>Records can only be added before calling {@link #sort()}.
   */
  public void add(byte[] key, byte[] value) throws IOException {
    Preconditions.checkState(!sortCalled, "Records can only be added before sort()");
    CODER.encode(key, dataStream);
    CODER.encode(value, dataStream);
  }

  /**
   * Sorts the added elements and returns an {@link Iterable} over the sorted elements.
   *
   * <p>Can be called at most once.
   */
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    Preconditions.checkState(!sortCalled, "sort() can only be called once.");
    sortCalled = true;

    dataStream.close();

    return mergeSortedFiles(sortInBatch());
  }

  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Loads the file by blocks of records, sorts in memory, and writes the result to temporary files
   * that have to be merged later.
   */
  private List<File> sortInBatch() throws IOException {
    final long fileSize = Files.size(dataFile.toPath());
    final long memory = maxMemory > 0 ? maxMemory : estimateAvailableMemory();
    final long blockSize = estimateBestBlockSize(fileSize, memory); // in bytes
    LOG.debug(
        "Sort in batch with fileSize: {}, memory: {}, blockSize: {}", fileSize, memory, blockSize);

    final List<File> files = new ArrayList<>();
    InputStream inputStream = new BufferedInputStream(new FileInputStream(dataFile));
    try {
      final List<KV<byte[], byte[]>> tempList = new ArrayList<>();
      @Nullable KV<byte[], byte[]> kv = KV.of(new byte[0], new byte[0]);
      while (kv != null) {
        long currentBlockSize = 0;
        while (currentBlockSize < blockSize) {
          kv = readKeyValue(inputStream);
          if (kv == null) {
            break;
          }

          // as long as you have enough memory
          tempList.add(kv);
          currentBlockSize += estimateSizeOf(kv);
        }
        files.add(sortAndSave(tempList));
        tempList.clear();
      }
    } finally {
      inputStream.close();
      Preconditions.checkArgument(dataFile.delete());
    }
    return files;
  }

  /** Sort a list and save it to a temporary file. */
  private File sortAndSave(List<KV<byte[], byte[]>> tempList) throws IOException {
    final File tempFile = Files.createTempFile(tempDir, "sort", "seq").toFile();
    tempFile.deleteOnExit();
    LOG.debug("Sort and save {}", tempFile);

    tempList.sort(KV_COMPARATOR);

    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
    try {
      for (KV<byte[], byte[]> kv : tempList) {
        CODER.encode(kv.getKey(), outputStream);
        CODER.encode(kv.getValue(), outputStream);
      }
    } finally {
      outputStream.close();
    }
    return tempFile;
  }

  /** Merges a list of temporary flat files. */
  private Iterable<KV<byte[], byte[]>> mergeSortedFiles(List<File> files) {
    return () -> {
      final List<Iterator<KV<byte[], byte[]>>> iterators = new ArrayList<>();
      for (File file : files) {
        try {
          iterators.add(iterateFile(file));
        } catch (FileNotFoundException e) {
          throw new IllegalStateException(e);
        }
      }

      return Iterators.mergeSorted(iterators, KV_COMPARATOR);
    };
  }

  /** Creates an {@link Iterator} over the key-value pairs in a file. */
  private Iterator<KV<byte[], byte[]>> iterateFile(File file) throws FileNotFoundException {
    final InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
    return new Iterator<KV<byte[], byte[]>>() {
      @Nullable KV<byte[], byte[]> nextKv = readKeyValueOrFail(inputStream);

      @Override
      public boolean hasNext() {
        return nextKv != null;
      }

      @Override
      public KV<byte[], byte[]> next() {
        if (nextKv == null) {
          throw new NoSuchElementException();
        }
        KV<byte[], byte[]> r = nextKv;
        nextKv = readKeyValueOrFail(inputStream);
        return r;
      }
    };
  }

  private @Nullable KV<byte[], byte[]> readKeyValueOrFail(InputStream inputStream) {
    try {
      return readKeyValue(inputStream);
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Reads the next key-value pair from a file. */
  private @Nullable KV<byte[], byte[]> readKeyValue(InputStream inputStream) throws IOException {
    try {
      final byte[] keyBytes = CODER.decode(inputStream);
      final byte[] valueBytes = CODER.decode(inputStream);
      return KV.of(keyBytes, valueBytes);
    } catch (EOFException e) {
      return null;
    }
  }

  ////////////////////////////////////////////////////////////////////////////////

  private int bufferSize(int numFiles) {
    final long memory = maxMemory > 0 ? maxMemory : estimateAvailableMemory();
    return (int) (memory / numFiles / 2);
  }

  /**
   * This method calls the garbage collector and then returns the free memory. This avoids problems
   * with applications where the GC hasn't reclaimed memory and reports no available memory.
   */
  @SuppressFBWarnings("DM_GC")
  private static long estimateAvailableMemory() {
    System.gc();
    // http://stackoverflow.com/questions/12807797/java-get-available-memory
    final Runtime r = Runtime.getRuntime();
    final long allocatedMemory = r.totalMemory() - r.freeMemory();
    return r.maxMemory() - allocatedMemory;
  }

  /**
   * We divide the file into small blocks. If the blocks are too small, we shall create too many
   * temporary files. If they are too big, we shall be using too much memory.
   *
   * @param sizeOfFile how much data (in bytes) can we expect
   * @param maxMemory Maximum memory to use (in bytes)
   */
  private static long estimateBestBlockSize(final long sizeOfFile, final long maxMemory) {
    // we don't want to open up much more than MAX_TEMP_FILES temporary files, better run out of
    // memory first.
    long blockSize = sizeOfFile / MAX_TEMP_FILES + (sizeOfFile % MAX_TEMP_FILES == 0 ? 0 : 1);

    // on the other hand, we don't want to create many temporary files for naught. If blockSize is
    // smaller than half the free memory, grow it.
    if (blockSize < maxMemory / 2) {
      blockSize = maxMemory / 2;
    }
    return blockSize;
  }

  private static long getObjectOverhead() {
    // By default we assume 64 bit JVM
    // (defensive approach since we will get larger estimations in case we are not sure)
    boolean is64BitJvm = true;
    // check the system property "sun.arch.data.model"
    // not very safe, as it might not work for all JVM implementations
    // nevertheless the worst thing that might happen is that the JVM is 32bit
    // but we assume its 64bit, so we will be counting a few extra bytes per string object
    // no harm done here since this is just an approximation.
    String arch = System.getProperty("sun.arch.data.model");
    if (arch != null && arch.contains("32")) {
      // If exists and is 32 bit then we assume a 32bit JVM
      is64BitJvm = false;
    }
    // The sizes below are a bit rough as we don't take into account
    // advanced JVM options such as compressed oops
    // however if our calculation is not accurate it'll be a bit over
    // so there is no danger of an out of memory error because of this.
    long objectHeader = is64BitJvm ? 16 : 8;
    long arrayHeader = is64BitJvm ? 24 : 12;
    long objectRef = is64BitJvm ? 8 : 4;
    return objectHeader + (objectRef + arrayHeader) * 2;
  }

  private static long estimateSizeOf(KV<byte[], byte[]> kv) {
    return kv.getKey().length + kv.getValue().length + OBJECT_OVERHEAD;
  }
}
