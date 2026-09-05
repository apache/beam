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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.util.SerializableConfiguration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.reflect.ClassTag;

/**
 * Drives {@link BeamPartitionReader} directly over hand built partitions to prove the reader cache
 * protocol. The session only supplies the broadcasts, no query runs.
 */
@Category(StreamingTest.class)
@RunWith(JUnit4.class)
public class BeamReaderCacheProtocolTest {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final AtomicInteger TAGS = new AtomicInteger();

  private static final Coder<WindowedValue<Integer>> CODER =
      WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

  private static final long MAX_RECORDS = 3L;

  private static final long MAX_BATCH_MILLIS = 30_000L;

  private static final long READER_IDLE_MILLIS = 600_000L;

  private static Broadcast<SerializablePipelineOptions> options;
  private static Broadcast<SerializableConfiguration> hadoopConf;
  private static Configuration conf;

  private String tag;
  private IntListSource source;
  private String location;

  @BeforeClass
  public static void broadcastOnce() {
    SparkSession session = SESSION.getSession();
    conf = ((org.apache.spark.sql.classic.SparkSession) session).sessionState().newHadoopConf();
    options =
        session
            .sparkContext()
            .broadcast(
                new SerializablePipelineOptions(PipelineOptionsFactory.create()),
                ClassTag.apply(SerializablePipelineOptions.class));
    hadoopConf =
        session
            .sparkContext()
            .broadcast(
                new SerializableConfiguration(conf),
                ClassTag.apply(SerializableConfiguration.class));
  }

  @Before
  public void setUp() throws IOException {
    tag = "protocol-" + TAGS.incrementAndGet();
    source = new IntListSource(tag, 100);
    location = temp.newFolder("source").getAbsolutePath();
  }

  @After
  public void tearDown() {
    BeamReaderCache.invalidateAll();
    IntListSource.forget(tag);
  }

  /**
   * A batch starting where the cached reader stopped reuses it and finalizes the mark taken there.
   */
  @Test
  public void testContinuationReusesReaderAndFinalizesOnce() throws Exception {
    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    assertEquals("nothing is finalized before the next batch opens", noPositions(), finalized());

    BeamPartitionReader<Integer> second = open(1, 2);
    assertEquals("the epoch 1 mark is finalized on open", positions(3), finalized());
    assertEquals(Arrays.asList(3, 4, 5), drain(second));
    assertEquals(positions(3), finalized());
    assertEquals("one reader serves both batches", 1, IntListSource.created(tag));
  }

  /** A retried batch restarts from the durable mark at its start and finalizes nothing. */
  @Test
  public void testRetryRecreatesReaderWithoutFinalizing() throws Exception {
    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    assertEquals(noPositions(), finalized());
    assertEquals(2, IntListSource.created(tag));
  }

  /** After the cache is lost the durable mark at the start epoch restores the position. */
  @Test
  public void testExecutorHopRestoresFromDurableMark() throws Exception {
    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    assertEquals(Arrays.asList(3, 4, 5), readBatch(1, 2));
    BeamReaderCache.invalidateAll();

    assertEquals(Arrays.asList(6, 7, 8), readBatch(2, 3));
    assertEquals(
        "only the epoch 1 mark was live when its batch was committed", positions(3), finalized());
    assertEquals(2, IntListSource.created(tag));
  }

  /**
   * A start epoch above zero without a durable mark is an invariant violation, not a fresh start.
   */
  @Test
  public void testMissingMarkThrows() {
    assertThrows(IllegalStateException.class, () -> new BeamPartitionReader<>(partition(5, 6)));
    assertThrows(
        IllegalStateException.class,
        () -> new BeamPartitionReaderFactory().createReader(partition(5, 6)));
    assertEquals(0, IntListSource.created(tag));
  }

  /**
   * A reader closed before it started writes its start mark forward at E and stays reusable from E,
   * a later batch from S is a fresh start.
   */
  @Test
  public void testNeverStartedReaderWritesStartMarkForward() throws Exception {
    BeamPartitionReader<Integer> idle = open(0, 1);
    idle.close();
    assertTrue(new File(location, "marks/0/1").exists());
    assertEquals(1, IntListSource.created(tag));

    assertEquals(Arrays.asList(0, 1, 2), readBatch(1, 2));
    assertEquals("the idle reader is reused", 1, IntListSource.created(tag));

    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    assertEquals("a batch from epoch 0 is a fresh start", 2, IntListSource.created(tag));
    assertEquals(noPositions(), finalized());
  }

  /** A failed mark write fails the batch instead of advancing the reader silently. */
  @Test
  public void testMarkWriteFailureFailsBatch() throws Exception {
    String file = temp.newFile("not-a-directory").getAbsolutePath();
    BeamPartitionReader<Integer> reader = new BeamPartitionReader<>(partition(file, 0, 1));
    List<Integer> values = new ArrayList<>();
    assertThrows(IOException.class, () -> drainInto(reader, values));
    assertEquals(Arrays.asList(0, 1, 2), values);
  }

  /** A retry after a failed mark write recreates the reader from the durable mark at S. */
  @Test
  public void testRetryAfterFailedMarkWriteRecreatesReader() throws Exception {
    String file = temp.newFile("not-a-directory").getAbsolutePath();
    assertEquals(
        Arrays.asList(0, 1, 2), drainUntilFailure(partition(file, 0, 1), IOException.class));
    assertEquals(
        Arrays.asList(0, 1, 2), drainUntilFailure(partition(file, 0, 1), IOException.class));
    assertEquals(noPositions(), finalized());
    assertEquals(2, IntListSource.created(tag));
  }

  /** A retry after a failed mark encode above epoch 0 restores from the durable mark at S. */
  @Test
  public void testRetryAfterFailedMarkEncodeRestoresFromDurableMark() throws Exception {
    assertEquals(Arrays.asList(0, 1, 2), readBatch(0, 1));
    IntListSource.failEncoding(tag, true);
    assertEquals(
        Arrays.asList(3, 4, 5), drainUntilFailure(partition(1, 2), IllegalStateException.class));
    IntListSource.failEncoding(tag, false);

    assertEquals(Arrays.asList(3, 4, 5), readBatch(1, 2));
    assertEquals(
        "the epoch 1 mark was finalized by the first open only", positions(3), finalized());
    assertEquals(2, IntListSource.created(tag));
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private BeamInputPartition<Integer> partition(long start, long end) {
    return partition(location, start, end);
  }

  private BeamInputPartition<Integer> partition(String checkpointLocation, long start, long end) {
    return new BeamInputPartition<>(
        source,
        CODER,
        options,
        hadoopConf,
        checkpointLocation,
        0,
        start,
        end,
        MAX_RECORDS,
        MAX_BATCH_MILLIS,
        READER_IDLE_MILLIS,
        new String[0]);
  }

  private BeamPartitionReader<Integer> open(long start, long end) throws IOException {
    return new BeamPartitionReader<>(partition(start, end));
  }

  private List<Integer> readBatch(long start, long end) throws IOException {
    return drain(open(start, end));
  }

  private static List<Integer> drain(BeamPartitionReader<Integer> reader) throws IOException {
    List<Integer> values = new ArrayList<>();
    drainInto(reader, values);
    return values;
  }

  private static void drainInto(BeamPartitionReader<Integer> reader, List<Integer> values)
      throws IOException {
    while (reader.next()) {
      InternalRow row = reader.get();
      values.add(CoderUtils.decodeFromByteArray(CODER, row.getBinary(0)).getValue());
    }
    reader.close();
  }

  /** Opens and drains a batch expected to fail, returns what it delivered before failing. */
  private static List<Integer> drainUntilFailure(
      BeamInputPartition<Integer> partition, Class<? extends Exception> failure)
      throws IOException {
    BeamPartitionReader<Integer> reader = new BeamPartitionReader<>(partition);
    List<Integer> values = new ArrayList<>();
    assertThrows(failure, () -> drainInto(reader, values));
    return values;
  }

  private List<Integer> finalized() {
    return IntListSource.finalized(tag);
  }

  private static List<Integer> positions(Integer... positions) {
    return Arrays.asList(positions);
  }

  private static List<Integer> noPositions() {
    return Collections.emptyList();
  }

  // ---------------------------------------------------------------------------------------------
  // a list source whose marks count their finalizations
  // ---------------------------------------------------------------------------------------------

  /**
   * Single split source over the integers {@code 0..count-1}. Marks are not Java serializable and
   * record the position they finalize under the source's tag, readers are counted per tag.
   */
  static class IntListSource extends UnboundedSource<Integer, IntListSource.Mark> {
    private static final long serialVersionUID = 1L;

    static final Coder<Mark> MARK_CODER = new MarkCoder();

    private static final ConcurrentMap<String, List<Integer>> FINALIZED = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> CREATED = new ConcurrentHashMap<>();
    private static final Set<String> FAILING_ENCODE = ConcurrentHashMap.newKeySet();

    private final String tag;
    private final int count;

    IntListSource(String tag, int count) {
      this.tag = tag;
      this.count = count;
    }

    static List<Integer> finalized(String tag) {
      List<Integer> positions = FINALIZED.get(tag);
      if (positions == null) {
        return Collections.emptyList();
      }
      synchronized (positions) {
        return new ArrayList<>(positions);
      }
    }

    static int created(String tag) {
      AtomicInteger created = CREATED.get(tag);
      return created == null ? 0 : created.get();
    }

    static void failEncoding(String tag, boolean fail) {
      if (fail) {
        FAILING_ENCODE.add(tag);
      } else {
        FAILING_ENCODE.remove(tag);
      }
    }

    static void forget(String tag) {
      FINALIZED.remove(tag);
      CREATED.remove(tag);
      FAILING_ENCODE.remove(tag);
    }

    @Override
    public List<IntListSource> split(int desiredNumSplits, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Integer> createReader(PipelineOptions options, @Nullable Mark mark) {
      CREATED.computeIfAbsent(tag, t -> new AtomicInteger()).incrementAndGet();
      return new IntListReader(this, mark == null ? 0 : mark.next);
    }

    @Override
    public Coder<Mark> getCheckpointMarkCoder() {
      return MARK_CODER;
    }

    @Override
    public Coder<Integer> getOutputCoder() {
      return VarIntCoder.of();
    }

    /** Position of the next element, deliberately not {@link java.io.Serializable}. */
    static final class Mark implements UnboundedSource.CheckpointMark {
      private final String tag;
      private final int next;

      Mark(String tag, int next) {
        this.tag = tag;
        this.next = next;
      }

      @Override
      public void finalizeCheckpoint() {
        FINALIZED
            .computeIfAbsent(tag, t -> Collections.synchronizedList(new ArrayList<>()))
            .add(next);
      }
    }

    private static final class MarkCoder extends CustomCoder<Mark> {
      private static final long serialVersionUID = 1L;

      @Override
      public void encode(Mark mark, OutputStream out) throws IOException {
        if (FAILING_ENCODE.contains(mark.tag)) {
          throw new CoderException("injected mark encode failure for " + mark.tag);
        }
        StringUtf8Coder.of().encode(mark.tag, out);
        VarIntCoder.of().encode(mark.next, out);
      }

      @Override
      public Mark decode(InputStream in) throws IOException {
        return new Mark(StringUtf8Coder.of().decode(in), VarIntCoder.of().decode(in));
      }
    }

    private static final class IntListReader extends UnboundedReader<Integer> {
      private final IntListSource source;
      private int next;
      private int current = -1;

      IntListReader(IntListSource source, int next) {
        this.source = source;
        this.next = next;
      }

      @Override
      public boolean start() {
        return advance();
      }

      @Override
      public boolean advance() {
        if (next < source.count) {
          current = next++;
          return true;
        }
        return false;
      }

      @Override
      public Integer getCurrent() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return new Instant(1_700_000_000_000L + current * 1_000L);
      }

      @Override
      public Instant getWatermark() {
        return current < 0 ? BoundedWindow.TIMESTAMP_MIN_VALUE : getCurrentTimestamp();
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new Mark(source.tag, next);
      }

      @Override
      public UnboundedSource<Integer, ?> getCurrentSource() {
        return source;
      }

      @Override
      public void close() {}
    }
  }
}
