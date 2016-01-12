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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.util.RandomAccessData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReaderObserver;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink.SinkWriter;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Tests for {@link IsmReader}.
 */
@RunWith(JUnit4.class)
public class IsmReaderTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testReadEmpty() throws Exception {
    runTestRead(Collections.<KV<byte[], byte[]>>emptyList(), tmpFolder.newFile());
  }

  @Test
  public void testRead() throws Exception {
    Random random = new Random(23498321490L);
    for (int i : Arrays.asList(4, 8, 12)) {
      int minElements = (int) Math.pow(2, i);
      // Generates between 2^i and 2^(i + 1) elements.
      runTestRead(dataGenerator(minElements + random.nextInt(minElements),
          8 /* approximate key size */, 8 /* max value size */), tmpFolder.newFile());
    }
  }

  @Test
  public void testReadRandomOrder() throws Exception {
    Random random = new Random(2348238943L);
    for (int i : Arrays.asList(4, 8, 12)) {
      int minElements = (int) Math.pow(2, i);
      // Generates between 2^i and 2^(i + 1) elements.
      runTestReadRandomOrder(
          dataGenerator(minElements + random.nextInt(minElements),
              8 /* approximate key size */, 4096 /* max value size */), tmpFolder.newFile());
    }
  }

  @Test
  public void testReadMissingKeysBypassingBloomFilter() throws Exception {
    List<KV<byte[], byte[]>> data = new ArrayList<>();
    data.add(KV.of(new byte[]{ 0x04 }, new byte[] { 0x00 }));
    data.add(KV.of(new byte[]{ 0x08 }, new byte[] { 0x01 }));
    String path = initInputFile(data, tmpFolder.newFile());
    IsmReader<byte[], byte[]> reader =
        new IsmReader<byte[], byte[]>(path, ByteArrayCoder.of(), ByteArrayCoder.of()) {
      // We use this override to get around the Bloom filter saying that the key doesn't exist.
      @Override
      boolean bloomFilterMightContain(RandomAccessData keyBytes) {
        return true;
      }
    };

    // Check that we got null with a key before all keys contained in the file.
    assertNull(reader.get(new byte[]{ 0x02 }));
    // Check that we got null with a key between two other keys contained in the file.
    assertNull(reader.get(new byte[]{ 0x06 }));
    // Check that we got null with a key that is after all keys contained in the file.
    assertNull(reader.get(new byte[]{ 0x10 }));
  }

  /** Write input elements to a file and return the file name. */
  static String initInputFile(Iterable<KV<byte[], byte[]>> elements, File tmpFile)
      throws Exception {
    Sink<WindowedValue<KV<byte[], byte[]>>> sink =
        new IsmSink<byte[], byte[]>(tmpFile.getPath(), ByteArrayCoder.of(), ByteArrayCoder.of());

    try (SinkWriter<WindowedValue<KV<byte[], byte[]>>> writer = sink.writer()) {
      for (KV<byte[], byte[]> element : elements) {
        writer.add(WindowedValue.valueInGlobalWindow(element));
      }
    }
    return tmpFile.getPath();
  }

  /**
   * Reads from a file generated from a collection of elements and verifies that the elements read
   * are the same as the elements written.
   */
  static void runTestRead(Iterable<KV<byte[], byte[]>> expectedData, File tmpFile)
      throws Exception {
    String filename = initInputFile(expectedData, tmpFile);
    IsmReader<byte[], byte[]> reader =
        new IsmReader<>(filename, ByteArrayCoder.of(), ByteArrayCoder.of());
    TestReaderObserver observer = new TestReaderObserver(reader);
    reader.addObserver(observer);

    Iterator<KV<byte[], byte[]>> expectedIterator = expectedData.iterator();
    try (NativeReader.LegacyReaderIterator<KV<byte[], byte[]>> iterator = reader.iterator()) {
      while (iterator.hasNext() && expectedIterator.hasNext()) {
        KV<byte[], byte[]> actual = iterator.next();
        KV<byte[], byte[]> expectedNext = expectedIterator.next();
        assertArrayEquals(actual.getKey(), expectedNext.getKey());
        assertArrayEquals(actual.getValue(), expectedNext.getValue());

        // Verify that the observer saw at least as many bytes as the size of the value.
        assertTrue(actual.getValue().length
            <= observer.getActualSizes().get(observer.getActualSizes().size() - 1));
      }
      if (iterator.hasNext()) {
        fail("Read more elements then expected, did not expect: " + iterator.next());
      } else if (expectedIterator.hasNext()) {
        fail("Read less elements then expected, expected: " + expectedIterator.next());
      }

      // Verify that we see a {@link NoSuchElementException} if we attempt to go further.
      try {
        iterator.next();
        fail("Expected a NoSuchElementException to have been thrown.");
      } catch (NoSuchElementException expected) {
      }
    }
  }

  static class EvenFilter implements Predicate<KV<byte[], byte[]>> {
    private static final EvenFilter INSTANCE = new EvenFilter();

    @Override
    public boolean apply(KV<byte[], byte[]> input) {
      return input.getValue()[input.getValue().length - 1] % 2 == 0;
    }
  }

  static void runTestReadRandomOrder(Iterable<KV<byte[], byte[]>> elements, File tmpFile)
      throws Exception {
    Iterable<KV<byte[], byte[]>> oddValues =
        Iterables.filter(elements, Predicates.not(EvenFilter.INSTANCE));
    Iterable<KV<byte[], byte[]>> evenValues =
        Iterables.filter(elements, EvenFilter.INSTANCE);

    String filename = initInputFile(oddValues, tmpFile);
    IsmReader<byte[], byte[]> reader =
        new IsmReader<>(filename, ByteArrayCoder.of(), ByteArrayCoder.of());
    TestReaderObserver observer = new TestReaderObserver(reader);
    reader.addObserver(observer);

    Iterator<KV<byte[], byte[]>> expectedIterator = oddValues.iterator();
    while (expectedIterator.hasNext()) {
      KV<byte[], byte[]> expectedNext = expectedIterator.next();
      KV<byte[], byte[]> actual = reader.get(expectedNext.getKey());
      assertArrayEquals(actual.getValue(), expectedNext.getValue());

      // Verify that the observer saw at least as many bytes as the size of the value.
      assertTrue(actual.getValue().length
          <= observer.getActualSizes().get(observer.getActualSizes().size() - 1));
    }

    Iterator<KV<byte[], byte[]>> missingIterator = evenValues.iterator();
    while (missingIterator.hasNext()) {
      KV<byte[], byte[]> missingNext = missingIterator.next();
      assertNull(reader.get(missingNext.getKey()));
    }
  }

  // Creates an iterable with key bytes in ascending order.
  static Iterable<KV<byte[], byte[]>> dataGenerator(
      final int numberOfKeys, final int approximateKeySize, final int maxValueSize) {
    final int minimumNumberOfKeyBytes = 4;
    return new Iterable<KV<byte[], byte[]>>() {
      @Override
      public Iterator<KV<byte[], byte[]>> iterator() {
        final Random random = new Random(numberOfKeys);
        return new Iterator<KV<byte[], byte[]>>() {
          int current;
          byte[] previousKey = new byte[random.nextInt(approximateKeySize)
                                        + minimumNumberOfKeyBytes];
          {
            // Generate a random key with enough space at the front for 2^32 values.
            random.nextBytes(previousKey);
            previousKey[0] = 0;
            previousKey[1] = 0;
            previousKey[2] = 0;
            previousKey[3] = 0;
          }

          @Override
          public boolean hasNext() {
            return current < numberOfKeys;
          }

          @Override
          public KV<byte[], byte[]> next() {
            current += 1;
            byte[] currentKey = new byte[random.nextInt(approximateKeySize)
                                         + minimumNumberOfKeyBytes];
            int matchingPrefix = Math.min(currentKey.length,
                random.nextInt(approximateKeySize) + minimumNumberOfKeyBytes);
            byte[] randomSuffix = new byte[currentKey.length - matchingPrefix];
            random.nextBytes(randomSuffix);

            System.arraycopy(previousKey, 0,
                currentKey, 0, Math.min(currentKey.length, previousKey.length));
            System.arraycopy(randomSuffix, 0, currentKey, matchingPrefix, randomSuffix.length);

            matchingPrefix -= 1;
            // Find the first byte which is less than 255 at the end of the matching portion.
            while ((currentKey[matchingPrefix] & 0xFF) == 0xFF) {
              currentKey[matchingPrefix] = 0;
              matchingPrefix -= 1;
            }
            // Increment the last byte of the matching prefix to make sure this key is
            // larger than the previous key.
            currentKey[matchingPrefix] = (byte) ((currentKey[matchingPrefix] & 0xFF) + 1);

            byte[] value = new byte[random.nextInt(maxValueSize) + 1];
            random.nextBytes(value);
            previousKey = currentKey;
            return KV.of(currentKey, value);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
