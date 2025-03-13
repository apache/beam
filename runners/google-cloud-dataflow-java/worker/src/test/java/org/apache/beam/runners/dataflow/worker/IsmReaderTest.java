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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.IsmReaderImpl.CachedTailSeekableByteChannel;
import org.apache.beam.runners.dataflow.worker.IsmReaderImpl.IsmShardKey;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils.TestReaderObserver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink.SinkWriter;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderPropertiesTest.NonDeterministicCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link IsmReader}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class IsmReaderTest {
  private static final long BLOOM_FILTER_SIZE_LIMIT = 10_000;
  private static final int TEST_BLOCK_SIZE = 1024;
  private static final IsmRecordCoder<byte[]> CODER =
      IsmRecordCoder.of(
          1, // number or shard key coders for value records
          1, // number of shard key coders for metadata records
          ImmutableList.<Coder<?>>of(MetadataKeyCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
          ByteArrayCoder.of());

  private static final Coder<String> NON_DETERMINISTIC_CODER = new NonDeterministicCoder();
  private static final byte[] EMPTY = new byte[0];

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  private Cache<
          IsmReaderImpl.IsmShardKey,
          WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<byte[]>>>>>
      cache;
  private DataflowExecutionContext executionContext;
  private Closeable stateCloseable;

  @Before
  public void setUp() {
    cache =
        CacheBuilder.newBuilder()
            .weigher(Weighers.fixedWeightKeys(1))
            .maximumWeight(10_000)
            .build();

    executionContext =
        BatchModeExecutionContext.forTesting(
            PipelineOptionsFactory.as(DataflowPipelineOptions.class), "testStage");

    DataflowExecutionState state =
        executionContext
            .getExecutionStateRegistry()
            .getState(
                NameContextsForTests.nameContextForTest(), "test", null, NoopProfileScope.NOOP);
    stateCloseable = executionContext.getExecutionStateTracker().enterState(state);
  }

  @After
  public void tearDown() throws IOException {
    stateCloseable.close();
  }

  @Test
  public void testReadEmpty() throws Exception {
    writeElementsToFileAndReadInOrder(Collections.<IsmRecord<byte[]>>emptyList());
  }

  @Test
  public void testUsingNonDeterministicShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmReaderImpl<>(
        FileSystems.matchSingleFileSpec(tmpFolder.newFile().getPath()).resourceId(),
        IsmRecordCoder.of(
            1, // number or shard key coders for value records
            0, // number of shard key coders for metadata records
            ImmutableList.<Coder<?>>of(NON_DETERMINISTIC_CODER, ByteArrayCoder.of()),
            ByteArrayCoder.of()),
        cache);
  }

  @Test
  public void testUsingNonDeterministicNonShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmReaderImpl<>(
        FileSystems.matchSingleFileSpec(tmpFolder.newFile().getPath()).resourceId(),
        IsmRecordCoder.of(
            1, // number or shard key coders for value records
            0, // number of shard key coders for metadata records
            ImmutableList.<Coder<?>>of(ByteArrayCoder.of(), NON_DETERMINISTIC_CODER),
            ByteArrayCoder.of()),
        cache);
  }

  @Test
  public void testIsEmpty() throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<IsmRecord<byte[]>> data = new ArrayList<>();
    writeElementsToFile(data, tmpFile);

    IsmReader<byte[]> reader =
        new IsmReaderImpl<byte[]>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);
    assertFalse(reader.isInitialized());
    assertTrue(reader.isEmpty());
    assertTrue(reader.isInitialized());
  }

  @Test
  public void testRead() throws Exception {
    Random random = new Random(23498321490L);
    for (int i : Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) {
      int minElements = (int) Math.pow(2, i);
      int valueSize = 128;
      // Generates between 2^i and 2^(i + 1) elements.
      writeElementsToFileAndReadInOrder(
          dataGenerator(
              8 /* number of primary keys */,
              minElements + random.nextInt(minElements) /* number of secondary keys */,
              8 /* max key size */,
              valueSize));
    }
  }

  @Test
  public void testReadThatProducesIndexEntries() throws Exception {
    Random random = new Random(23498323891L);
    int minElements = (int) Math.pow(2, 6);
    int valueSize = 128;
    // Since we are generating more than 2 blocks worth of data, we are guaranteed that
    // at least one index entry is generated per shard.
    checkState(minElements * valueSize > 2 * TEST_BLOCK_SIZE);
    writeElementsToFileAndReadInOrder(
        dataGenerator(
            8 /* number of primary keys */,
            minElements + random.nextInt(minElements) /* number of secondary keys */,
            8 /* max key size */,
            valueSize /* max value size */));
  }

  @Test
  public void testReadRandomOrder() throws Exception {
    Random random = new Random(2348238943L);
    for (int i : Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8)) {
      int minElements = (int) Math.pow(2, i);
      int valueSize = 128;
      // Generates between 2^i and 2^(i + 1) elements.
      writeElementsToFileAndReadInRandomOrder(
          dataGenerator(
              7 /* number of primary keys */,
              minElements + random.nextInt(minElements) /* number of secondary keys */,
              8 /* max key size */,
              valueSize /* max value size */));
    }
  }

  @Test
  public void testGetLastWithPrefix() throws Exception {
    Random random = new Random(2348238943L);
    for (int i : Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) {
      int minElements = (int) Math.pow(2, i);
      int valueSize = 128;
      // Generates between 2^i and 2^(i + 1) elements.
      writeElementsToFileAndFindLastElementPerPrimaryKey(
          dataGenerator(
              7,
              minElements + random.nextInt(minElements),
              8 /* max key size */,
              valueSize /* max value size */));
    }
  }

  @Test
  public void testReadMissingKeys() throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<IsmRecord<byte[]>> data = new ArrayList<>();
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x04}), EMPTY));
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x08}), EMPTY));
    writeElementsToFile(data, tmpFile);

    IsmReader<byte[]> reader =
        new IsmReaderImpl<byte[]>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);

    // Check that we got false with a key before all keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x02})).start());
    // Check that we got false with a key between two other keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x06})).start());
    // Check that we got false with a key that is after all keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x10})).start());
  }

  @Test
  public void testReadMissingKeysBypassingBloomFilter() throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<IsmRecord<byte[]>> data = new ArrayList<>();
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x04}), EMPTY));
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x08}), EMPTY));
    writeElementsToFile(data, tmpFile);

    IsmReader<byte[]> reader =
        new IsmReaderImpl<byte[]>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache) {
          // We use this override to get around the Bloom filter saying that the key doesn't exist.
          @Override
          boolean bloomFilterMightContain(RandomAccessData keyBytes) {
            return true;
          }
        };

    // Check that we got false with a key before all keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x02})).start());
    // Check that we got false with a key between two other keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x06})).start());
    // Check that we got false with a key that is after all keys contained in the file.
    assertFalse(reader.overKeyComponents(ImmutableList.of(EMPTY, new byte[] {0x10})).start());
  }

  @Test
  public void testReadKeyThatEncodesToEmptyByteArray() throws Exception {
    File tmpFile = tmpFolder.newFile();
    IsmRecordCoder<Void> coder =
        IsmRecordCoder.of(1, 0, ImmutableList.<Coder<?>>of(VoidCoder.of()), VoidCoder.of());
    IsmSink<Void> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFile.getPath(), false), coder, BLOOM_FILTER_SIZE_LIMIT);
    IsmRecord<Void> element = IsmRecord.of(Arrays.asList((Void) null), (Void) null);
    try (SinkWriter<WindowedValue<IsmRecord<Void>>> writer = sink.writer()) {
      writer.add(new ValueInEmptyWindows<>(element));
    }

    Cache<
            IsmShardKey,
            WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<Void>>>>>
        cache =
            CacheBuilder.newBuilder()
                .weigher(Weighers.fixedWeightKeys(1))
                .maximumWeight(10_000)
                .build();
    IsmReader<Void> reader =
        new IsmReaderImpl<>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), coder, cache);
    IsmReader<Void>.IsmPrefixReaderIterator iterator = reader.iterator();
    assertTrue(iterator.start());
    assertEquals(
        coder.structuralValue(element), coder.structuralValue(iterator.getCurrent().getValue()));
  }

  @Test
  public void testInitializationForSmallFilesIsCached() throws Exception {
    File tmpFile = tmpFolder.newFile();
    IsmShardKey expectedShardKey =
        new IsmShardKey(tmpFile.getAbsolutePath(), new RandomAccessData(0), 0, 13);
    List<IsmRecord<byte[]>> data = new ArrayList<>();
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x04}), new byte[] {0x04}));
    data.add(IsmRecord.<byte[]>of(ImmutableList.of(EMPTY, new byte[] {0x08}), new byte[] {0x08}));
    writeElementsToFile(data, tmpFile);

    IsmReader<byte[]> reader =
        new IsmReaderImpl<byte[]>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);
    // Validate that reader and cache are in initial state
    assertFalse(reader.isInitialized());
    assertEquals(0, cache.size());
    // Force initialization
    reader.overKeyComponents(ImmutableList.of());
    // Validate reader is initialized and expected entry is cached
    assertTrue(reader.isInitialized());
    WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<byte[]>>>> block =
        cache.getIfPresent(expectedShardKey);
    assertNotNull(block);
    assertArrayEquals(
        new byte[] {0x04}, block.getValue().firstEntry().getValue().getValue().getValue());
    assertArrayEquals(
        new byte[] {0x08}, block.getValue().lastEntry().getValue().getValue().getValue());
  }

  @Test
  public void testInitializationForLargeFilesIsNotCached() throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<IsmRecord<byte[]>> data = new ArrayList<>();
    // Use enough data records which are smaller than the cache limit to exceed the 1 MB
    // footer read buffer used by IsmReader to optimize small file handling.
    for (int i = 0; i < IsmReaderImpl.MAX_SHARD_INDEX_AND_FOOTER_SIZE / TEST_BLOCK_SIZE + 1; ++i) {
      data.add(
          IsmRecord.<byte[]>of(
              ImmutableList.of(EMPTY, Ints.toByteArray(i)), new byte[TEST_BLOCK_SIZE]));
    }
    writeElementsToFile(data, tmpFile);

    IsmReader<byte[]> reader =
        new IsmReaderImpl<byte[]>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);
    assertFalse(reader.isInitialized());
    assertEquals(0, cache.size());
    reader.overKeyComponents(ImmutableList.of());
    assertTrue(reader.isInitialized());
    assertEquals(0, cache.size());
  }

  /** Write input elements to the specified file. */
  static void writeElementsToFile(Iterable<IsmRecord<byte[]>> elements, File tmpFile)
      throws Exception {
    IsmSink<byte[]> sink =
        new IsmSink<byte[]>(
            FileSystems.matchNewResource(tmpFile.getPath(), false),
            CODER,
            BLOOM_FILTER_SIZE_LIMIT) {
          @Override
          long getBlockSize() {
            return TEST_BLOCK_SIZE;
          }
        };

    try (SinkWriter<WindowedValue<IsmRecord<byte[]>>> writer = sink.writer()) {
      for (IsmRecord<byte[]> element : elements) {
        writer.add(new ValueInEmptyWindows<>(element));
      }
    }
  }

  /**
   * Writes elements to an Ism file using an IsmSink. Then reads them back with an IsmReader,
   * verifying the values read match those that were written.
   */
  private void writeElementsToFileAndReadInOrder(Iterable<IsmRecord<byte[]>> elements)
      throws Exception {
    File tmpFile = tmpFolder.newFile();
    writeElementsToFile(elements, tmpFile);
    IsmReader<byte[]> reader =
        new IsmReaderImpl<>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);
    assertFalse(reader.isInitialized());

    TestReaderObserver observer = new TestReaderObserver(reader);
    reader.addObserver(observer);

    Iterator<IsmRecord<byte[]>> elementsIterator = elements.iterator();
    try (NativeReader.NativeReaderIterator<WindowedValue<IsmRecord<byte[]>>> iterator =
        reader.iterator()) {

      boolean more = iterator.start();
      assertTrue(reader.isInitialized());
      for (; more; more = iterator.advance()) {

        if (!elementsIterator.hasNext()) {
          break;
        }
        IsmRecord<byte[]> expected = elementsIterator.next();
        IsmRecord<byte[]> actual = iterator.getCurrent().getValue();
        assertIsmEquals(actual, expected);

        final int expectedLength;
        if (IsmFormat.isMetadataKey(expected.getKeyComponents())) {
          expectedLength = expected.getMetadata().length;
        } else {
          expectedLength = expected.getValue().length;
        }
        // Verify that the observer saw at least as many bytes as the size of the value.
        assertTrue(
            expectedLength <= observer.getActualSizes().get(observer.getActualSizes().size() - 1));
      }
      if (iterator.advance()) {
        fail("Read more elements then expected, did not expect: " + iterator.getCurrent());
      } else if (elementsIterator.hasNext()) {
        fail("Read less elements then expected, expected: " + elementsIterator.next());
      }

      // Verify that we see a {@link NoSuchElementException} if we attempt to go further.
      try {
        iterator.getCurrent();
        fail("Expected a NoSuchElementException to have been thrown.");
      } catch (NoSuchElementException expected) {
      }
    }
  }

  private static void assertIsmEquals(IsmRecord<byte[]> actual, IsmRecord<byte[]> expected) {
    assertEquals(expected.getKeyComponents().size(), actual.getKeyComponents().size());
    for (int i = 0; i < expected.getKeyComponents().size(); ++i) {
      if (actual.getKeyComponent(i) != expected.getKeyComponent(i)) {
        assertArrayEquals((byte[]) actual.getKeyComponent(i), (byte[]) expected.getKeyComponent(i));
      }
    }

    if (IsmFormat.isMetadataKey(expected.getKeyComponents())) {
      assertArrayEquals(actual.getMetadata(), expected.getMetadata());
    } else {
      assertArrayEquals(actual.getValue(), expected.getValue());
    }
  }

  /**
   * A predicate which filters elements on whether the second key's last byte is odd or even. Allows
   * for a stable partitioning of generated data.
   */
  private static class EvenFilter implements Predicate<IsmRecord<byte[]>> {
    private static final EvenFilter INSTANCE = new EvenFilter();

    @Override
    public boolean apply(IsmRecord<byte[]> input) {
      byte[] secondKey = (byte[]) input.getKeyComponent(1);
      return secondKey[secondKey.length - 1] % 2 == 0;
    }
  }

  /**
   * Writes elements to an Ism file using an IsmSink. Then reads them back with an IsmReader using a
   * random order.
   */
  private void writeElementsToFileAndReadInRandomOrder(Iterable<IsmRecord<byte[]>> elements)
      throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<IsmRecord<byte[]>> oddSecondaryKeys =
        new ArrayList<>(
            ImmutableList.copyOf(Iterables.filter(elements, Predicates.not(EvenFilter.INSTANCE))));
    List<IsmRecord<byte[]>> evenSecondaryKeys =
        new ArrayList<>(ImmutableList.copyOf(Iterables.filter(elements, EvenFilter.INSTANCE)));

    writeElementsToFile(oddSecondaryKeys, tmpFile);
    IsmReader<byte[]> reader =
        new IsmReaderImpl<>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);

    // Test using next() for a within shard Ism prefix reader iterator
    Collections.shuffle(oddSecondaryKeys);
    for (IsmRecord<byte[]> expectedNext : oddSecondaryKeys) {
      IsmReader<byte[]>.IsmPrefixReaderIterator iterator =
          reader.overKeyComponents(expectedNext.getKeyComponents());
      assertTrue(iterator.start());
      assertIsmEquals(iterator.getCurrent().getValue(), expectedNext);
    }

    Collections.shuffle(oddSecondaryKeys);
    // Test using get() for a shard aware Ism prefix reader
    IsmReader<byte[]>.IsmPrefixReaderIterator readerIterator =
        reader.overKeyComponents(ImmutableList.of());
    for (IsmRecord<byte[]> expectedNext : oddSecondaryKeys) {
      assertIsmEquals(readerIterator.get(expectedNext.getKeyComponents()).getValue(), expectedNext);
    }

    // Test using next() for a within shard Ism prefix reader iterator
    Collections.shuffle(evenSecondaryKeys);
    for (IsmRecord<byte[]> missingNext : evenSecondaryKeys) {
      assertFalse(reader.overKeyComponents(missingNext.getKeyComponents()).start());
    }

    Collections.shuffle(evenSecondaryKeys);
    // Test using get() for a shard aware Ism prefix reader
    readerIterator = reader.overKeyComponents(ImmutableList.of());
    for (IsmRecord<byte[]> missingNext : evenSecondaryKeys) {
      assertNull(readerIterator.get(missingNext.getKeyComponents()));
    }
  }

  private void writeElementsToFileAndFindLastElementPerPrimaryKey(
      Iterable<IsmRecord<byte[]>> elements) throws Exception {
    File tmpFile = tmpFolder.newFile();
    Iterable<IsmRecord<byte[]>> oddValues =
        Iterables.filter(elements, Predicates.not(EvenFilter.INSTANCE));
    Iterable<IsmRecord<byte[]>> evenValues = Iterables.filter(elements, EvenFilter.INSTANCE);
    writeElementsToFile(oddValues, tmpFile);
    IsmReader<byte[]> reader =
        new IsmReaderImpl<>(
            FileSystems.matchSingleFileSpec(tmpFile.getAbsolutePath()).resourceId(), CODER, cache);

    SortedMap<byte[], NavigableSet<IsmRecord<byte[]>>> sortedBySecondKey =
        new TreeMap<>(UnsignedBytes.lexicographicalComparator());
    for (IsmRecord<byte[]> element : oddValues) {
      byte[] encodedPrimaryKey =
          CoderUtils.encodeToByteArray(CODER.getKeyComponentCoder(0), element.getKeyComponent(0));
      if (!sortedBySecondKey.containsKey(encodedPrimaryKey)) {
        sortedBySecondKey.put(
            encodedPrimaryKey, new TreeSet<>(new IsmRecordKeyComparator<>(CODER)));
      }
      sortedBySecondKey.get(encodedPrimaryKey).add(element);
    }

    // The returned value should have the element as a prefix of itself.
    for (IsmRecord<byte[]> element : oddValues) {
      byte[] encodedPrimaryKey =
          CoderUtils.encodeToByteArray(CODER.getKeyComponentCoder(0), element.getKeyComponent(0));
      assertIsmEquals(
          reader
              .overKeyComponents(ImmutableList.of(element.getKeyComponent(0)))
              .getLast()
              .getValue(),
          sortedBySecondKey.get(encodedPrimaryKey).last());
    }

    // The returned value should always have the element as a prefix of itself or not exist.
    for (IsmRecord<byte[]> element : evenValues) {
      byte[] encodedPrimaryKey =
          CoderUtils.encodeToByteArray(CODER.getKeyComponentCoder(0), element.getKeyComponent(0));
      IsmReader<byte[]>.IsmPrefixReaderIterator readerIterator =
          reader.overKeyComponents(ImmutableList.of(element.getKeyComponent(0)));
      WindowedValue<IsmRecord<byte[]>> lastWindowedValue = readerIterator.getLast();
      if (lastWindowedValue != null) {
        assertIsmEquals(
            lastWindowedValue.getValue(), sortedBySecondKey.get(encodedPrimaryKey).last());
      }
    }
  }

  static class IsmRecordKeyComparator<V> implements Comparator<IsmRecord<V>> {
    private final IsmRecordCoder<V> coder;

    IsmRecordKeyComparator(IsmRecordCoder<V> coder) {
      this.coder = coder;
    }

    @Override
    public int compare(IsmRecord<V> first, IsmRecord<V> second) {
      RandomAccessData firstKeyBytes = new RandomAccessData();
      coder.encodeAndHash(first.getKeyComponents(), firstKeyBytes);
      RandomAccessData secondKeyBytes = new RandomAccessData();
      coder.encodeAndHash(second.getKeyComponents(), secondKeyBytes);
      return RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
          firstKeyBytes, secondKeyBytes);
    }
  }

  /**
   * Specifies the minimum key size so that we can produce a random byte array with enough of a
   * prefix to be able to create successively larger secondary keys.
   */
  private static final int MIN_KEY_SIZE = 4;

  /** Specifies the percentage of keys that are metadata records when using the data generator. */
  private static final double PERCENT_METADATA_RECORDS = 0.01;

  /** Creates a map from Ism shard to a sorted set of IsmRecords. */
  private Map<Integer, SortedSet<IsmRecord<byte[]>>> dataGeneratorPerShard(
      final int numberOfPrimaryKeys,
      final int minNumberOfSecondaryKeys,
      final int maxKeySize,
      final int maxValueSize) {
    checkState(maxKeySize >= MIN_KEY_SIZE);

    final Random random = new Random(minNumberOfSecondaryKeys);

    Map<Integer, SortedSet<IsmRecord<byte[]>>> shardToRecordMap = new HashMap<>();
    while (shardToRecordMap.keySet().size() < numberOfPrimaryKeys) {
      // Generate the next primary key
      byte[] primaryKey = new byte[random.nextInt(maxKeySize - MIN_KEY_SIZE) + MIN_KEY_SIZE];
      random.nextBytes(primaryKey);
      int shardId = CODER.hash(ImmutableList.of(primaryKey));
      // Add a sorted set for the shard id if this shard id has never been generated before.
      if (!shardToRecordMap.containsKey(shardId)) {
        shardToRecordMap.put(
            shardId, new TreeSet<IsmRecord<byte[]>>(new IsmRecordKeyComparator<byte[]>(CODER)));
      }

      // Generate the requested number of secondary keys using the newly generated primary key.
      byte[] secondaryKey = new byte[maxKeySize];
      for (int j = 0; j < minNumberOfSecondaryKeys; ++j) {
        secondaryKey = generateNextSecondaryKey(random, maxKeySize, secondaryKey);

        // Generate the value bytes.
        byte[] value = new byte[random.nextInt(maxValueSize)];
        random.nextBytes(value);

        // 1% of keys are metadata records
        if (random.nextFloat() < PERCENT_METADATA_RECORDS) {
          IsmRecord<byte[]> ismRecord =
              IsmRecord.meta(ImmutableList.of(IsmFormat.getMetadataKey(), secondaryKey), value);
          int metadataShardId = CODER.hash(ismRecord.getKeyComponents());
          // Add a sorted set for the shard id if this shard id has never been generated before.
          if (!shardToRecordMap.containsKey(metadataShardId)) {
            shardToRecordMap.put(
                metadataShardId,
                new TreeSet<IsmRecord<byte[]>>(new IsmRecordKeyComparator<byte[]>(CODER)));
          }
          shardToRecordMap.get(metadataShardId).add(ismRecord);
        } else {
          IsmRecord<byte[]> ismRecord =
              IsmRecord.<byte[]>of(ImmutableList.of(primaryKey, secondaryKey), value);
          shardToRecordMap.get(shardId).add(ismRecord);
        }
      }
    }
    return shardToRecordMap;
  }

  private byte[] generateNextSecondaryKey(
      Random random, int maxKeySize, byte[] previousSecondaryKey) {
    byte[] currentSecondaryKey = new byte[random.nextInt(maxKeySize - MIN_KEY_SIZE) + MIN_KEY_SIZE];
    int matchingPrefix =
        Math.min(
            currentSecondaryKey.length, random.nextInt(maxKeySize - MIN_KEY_SIZE) + MIN_KEY_SIZE);
    byte[] randomSuffix = new byte[currentSecondaryKey.length - matchingPrefix];
    random.nextBytes(randomSuffix);

    System.arraycopy(
        previousSecondaryKey,
        0,
        currentSecondaryKey,
        0,
        Math.min(currentSecondaryKey.length, previousSecondaryKey.length));
    System.arraycopy(randomSuffix, 0, currentSecondaryKey, matchingPrefix, randomSuffix.length);

    matchingPrefix -= 1;
    // Find the first byte which is less than 255 at the end of the matching portion.
    while ((currentSecondaryKey[matchingPrefix] & 0xFF) == 0xFF) {
      currentSecondaryKey[matchingPrefix] = 0;
      matchingPrefix -= 1;
    }
    // Increment the last byte of the matching prefix to make sure this key is
    // larger than the previous key.
    currentSecondaryKey[matchingPrefix] = (byte) ((currentSecondaryKey[matchingPrefix] & 0xFF) + 1);
    return currentSecondaryKey;
  }

  /** Creates an iterable of IsmRecords grouped by shard id, and in ascending order per shard. */
  private Iterable<IsmRecord<byte[]>> dataGenerator(
      final int numberOfPrimaryKeys,
      final int numberOfSecondaryKeys,
      final int approximateKeySize,
      final int maxValueSize) {

    FluentIterable<IsmRecord<byte[]>> records =
        FluentIterable.from(
                dataGeneratorPerShard(
                        numberOfPrimaryKeys, numberOfSecondaryKeys,
                        approximateKeySize, maxValueSize)
                    .entrySet())
            .transformAndConcat(Map.Entry::getValue);
    return records;
  }

  @Test
  public void testCachedTailSeekableByteChannelThrowsOnTruncate() throws Exception {
    try (SeekableByteChannel channel = new CachedTailSeekableByteChannel(0, new byte[0])) {
      expectedException.expect(NonWritableChannelException.class);
      channel.truncate(0);
    }
  }

  @Test
  public void testCachedTailSeekableByteChannelThrowsOnWrite() throws Exception {
    try (SeekableByteChannel channel = new CachedTailSeekableByteChannel(0, new byte[0])) {
      expectedException.expect(NonWritableChannelException.class);
      channel.write(ByteBuffer.wrap(new byte[0]));
    }
  }

  @Test
  public void testCachedTailSeekableByteChannelRead() throws Exception {
    final int offset = 10;
    try (SeekableByteChannel channel =
        new CachedTailSeekableByteChannel(offset, new byte[] {0, 1, 2})) {
      ByteBuffer buffer = ByteBuffer.allocate(1);

      channel.position(offset);
      assertEquals(1, channel.read(buffer));
      assertEquals(0, buffer.get(0));
      assertEquals(offset + 1, channel.position());
      buffer.clear();

      assertEquals(1, channel.read(buffer));
      assertEquals(1, buffer.get(0));
      assertEquals(offset + 2, channel.position());
      buffer.clear();

      assertEquals(1, channel.read(buffer));
      assertEquals(2, buffer.get(0));
      assertEquals(offset + 3, channel.position());
      buffer.clear();

      // Reposition the stream and do a read
      channel.position(offset + 1);
      assertEquals(1, channel.read(buffer));
      assertEquals(1, buffer.get(0));
      assertEquals(offset + 2, channel.position());
      buffer.clear();

      assertEquals(1, channel.read(buffer));
      assertEquals(2, buffer.get(0));
      assertEquals(offset + 3, channel.position());
      buffer.clear();

      // This read is expected to return EOF
      assertEquals(-1, channel.read(buffer));
      buffer.clear();

      // Reposition the stream to EOF and do a read, expected to return EOF
      channel.position(offset + 3);
      assertEquals(-1, channel.read(buffer));
      buffer.clear();
    }
  }

  @Test
  public void testCachedTailSeekableByteChannelSeekBeforeBounds() throws Exception {
    try (SeekableByteChannel channel = new CachedTailSeekableByteChannel(1, new byte[0])) {
      // Seek to only valid position
      channel.position(1);
      expectedException.expect(IllegalArgumentException.class);
      channel.position(0);
    }
  }

  @Test
  public void testCachedTailSeekableByteChannelSeekBeyondBounds() throws Exception {
    try (SeekableByteChannel channel = new CachedTailSeekableByteChannel(1, new byte[0])) {
      // Seek to only valid position
      channel.position(1);
      expectedException.expect(IllegalArgumentException.class);
      channel.position(2);
    }
  }
}
