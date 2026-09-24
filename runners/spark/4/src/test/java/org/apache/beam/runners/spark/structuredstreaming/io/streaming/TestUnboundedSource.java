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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * In memory unbounded source for tests. Splits into one sub source per shard. Elements are strings
 * formatted as tag, shard, index. Watermark freezes after exhaustion. Marks record finalized
 * positions under tag and shard.
 */
public final class TestUnboundedSource extends UnboundedSource<String, TestUnboundedSource.Mark> {
  private static final long serialVersionUID = 1L;

  /** A modern timestamp with no rebase or DST subtleties. */
  public static final long BASE_MILLIS = 1_700_000_000_000L;

  public static final long INTERVAL_MILLIS = 1_000L;

  public static final Coder<Mark> MARK_CODER = new MarkCoder();

  private static final ConcurrentMap<String, List<Integer>> FINALIZED = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, AtomicInteger> CREATED = new ConcurrentHashMap<>();

  private final String tag;
  private final int shard;
  private final int shards;
  private final int perShard;

  public TestUnboundedSource(String tag, int shards, int count) {
    this(tag, -1, shards, count / shards);
  }

  private TestUnboundedSource(String tag, int shard, int shards, int perShard) {
    this.tag = tag;
    this.shard = shard;
    this.shards = shards;
    this.perShard = perShard;
  }

  public static Set<String> elements(String tag, int shards, int count) {
    Set<String> elements = new HashSet<>();
    for (int shard = 0; shard < shards; shard++) {
      for (int index = 0; index < count / shards; index++) {
        elements.add(element(tag, shard, index));
      }
    }
    return elements;
  }

  public static String element(String tag, int shard, int index) {
    return tag + "-" + shard + "-" + index;
  }

  public static int shardOf(String element) {
    String head = element.substring(0, element.lastIndexOf('-'));
    return Integer.parseInt(head.substring(head.lastIndexOf('-') + 1));
  }

  public static int indexOf(String element) {
    return Integer.parseInt(element.substring(element.lastIndexOf('-') + 1));
  }

  public static List<Integer> finalized(String tag, int shard) {
    List<Integer> positions = FINALIZED.get(key(tag, shard));
    if (positions == null) {
      return Collections.emptyList();
    }
    synchronized (positions) {
      return new ArrayList<>(positions);
    }
  }

  public static int created(String tag) {
    AtomicInteger created = CREATED.get(tag);
    return created == null ? 0 : created.get();
  }

  public static void forget(String tag) {
    FINALIZED.keySet().removeIf(key -> key.startsWith(tag + "/"));
    CREATED.remove(tag);
  }

  private static String key(String tag, int shard) {
    return tag + "/" + shard;
  }

  @Override
  public List<TestUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    if (shard >= 0) {
      return Collections.singletonList(this);
    }
    List<TestUnboundedSource> splits = new ArrayList<>();
    for (int i = 0; i < shards; i++) {
      splits.add(new TestUnboundedSource(tag, i, shards, perShard));
    }
    return splits;
  }

  @Override
  public UnboundedReader<String> createReader(PipelineOptions options, @Nullable Mark mark) {
    if (shard < 0) {
      throw new IllegalStateException("split before reading");
    }
    CREATED.computeIfAbsent(tag, t -> new AtomicInteger()).incrementAndGet();
    return new Reader(this, mark == null ? 0 : mark.next);
  }

  @Override
  public Coder<Mark> getCheckpointMarkCoder() {
    return MARK_CODER;
  }

  @Override
  public Coder<String> getOutputCoder() {
    return StringUtf8Coder.of();
  }

  /** Position of the next element of a shard, not Java serializable. */
  public static final class Mark implements UnboundedSource.CheckpointMark {
    private final String tag;
    private final int shard;
    final int next;

    public Mark(String tag, int shard, int next) {
      this.tag = tag;
      this.shard = shard;
      this.next = next;
    }

    @Override
    public void finalizeCheckpoint() {
      FINALIZED
          .computeIfAbsent(key(tag, shard), k -> Collections.synchronizedList(new ArrayList<>()))
          .add(next);
    }
  }

  private static final class MarkCoder extends CustomCoder<Mark> {
    private static final long serialVersionUID = 1L;

    @Override
    public void encode(Mark mark, OutputStream out) throws IOException {
      StringUtf8Coder.of().encode(mark.tag, out);
      VarIntCoder.of().encode(mark.shard, out);
      VarIntCoder.of().encode(mark.next, out);
    }

    @Override
    public Mark decode(InputStream in) throws IOException {
      return new Mark(
          StringUtf8Coder.of().decode(in),
          VarIntCoder.of().decode(in),
          VarIntCoder.of().decode(in));
    }
  }

  private static final class Reader extends UnboundedReader<String> {
    private final TestUnboundedSource source;
    private int next;
    private int current = -1;

    Reader(TestUnboundedSource source, int next) {
      this.source = source;
      this.next = next;
    }

    @Override
    public boolean start() {
      return advance();
    }

    @Override
    public boolean advance() {
      if (next < source.perShard) {
        current = next++;
        return true;
      }
      return false;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (current < 0) {
        throw new NoSuchElementException();
      }
      return element(source.tag, source.shard, current);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (current < 0) {
        throw new NoSuchElementException();
      }
      return new Instant(
          BASE_MILLIS + (source.shard * source.perShard + current) * INTERVAL_MILLIS);
    }

    @Override
    public Instant getWatermark() {
      return current < 0 ? BoundedWindow.TIMESTAMP_MIN_VALUE : getCurrentTimestamp();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new Mark(source.tag, source.shard, next);
    }

    @Override
    public UnboundedSource<String, ?> getCurrentSource() {
      return source;
    }

    @Override
    public void close() {}
  }
}
