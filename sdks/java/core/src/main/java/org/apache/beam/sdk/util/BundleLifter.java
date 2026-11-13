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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that routes elements to one of two TupleTags depending on how many bytes of them
 * arrive together within a bundle.
 *
 * <p>Elements are buffered per key. Once the buffered elements of a key reach the threshold, they
 * are emitted to the large tag, and all later elements of that key in the same bundle are emitted
 * to the large tag directly without buffering. Elements of keys that never reach the threshold are
 * emitted to the small tag at the end of the bundle.
 *
 * <p>Without a key function, all elements of a bundle share a single key, so the threshold applies
 * to the whole bundle. With {@link #withKeyFn}, the key is the element's window, pane and the value
 * returned by the key function; elements for which it returns {@code null} share their own key.
 *
 * <p>To bound memory, at most {@link #withMaxBufferedBytes maxBufferedBytes} (twice the threshold
 * by default) is buffered in total. When that is exceeded, the keys with the fewest buffered bytes
 * are emitted to the small tag until at most half of it remains buffered, and for the rest of the
 * bundle elements of keys that are neither buffered nor lifted are emitted to the small tag
 * immediately.
 *
 * <p>Every element keeps its own timestamp and window.
 *
 * <p>For internal use only; no backwards-compatibility guarantees.
 *
 * @param <T> The type of elements in the input PCollection.
 */
@Internal
@SuppressFBWarnings(
    value = "CT_CONSTRUCTOR_THROW",
    justification =
        "Pre-existing finding, not triaged yet."
            + " Making the class final or moving the throwing code"
            + " into a static factory method may fix it.")
public class BundleLifter<T> extends PTransform<PCollection<T>, PCollectionTuple> {

  final TupleTag<T> smallBatchTag;
  final TupleTag<T> largeBatchTag;
  final long threshold;
  final long maxBufferedBytes;
  final SerializableFunction<T, Integer> elementSizer;
  final @Nullable SerializableFunction<T, ? extends @Nullable Object> keyFn;

  /** The default memory cap: twice the threshold. */
  static long defaultMaxBufferedBytes(long threshold) {
    return threshold > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : 2 * threshold;
  }

  /**
   * A DoFn that buffers elements per key within a bundle and outputs them to the large tag once a
   * key reaches the threshold, or to the small tag otherwise.
   *
   * @param <T> The type of elements being processed.
   */
  static class BundleLiftDoFn<T> extends DoFn<T, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(BundleLiftDoFn.class);

    /** The key shared by all elements without a key, or whose key function returns null. */
    private static final Object UNKEYED = new Object();

    final TupleTag<T> smallBatchTag;
    final TupleTag<T> largeBatchTag;
    final long threshold;
    final long maxBufferedBytes;
    final SerializableFunction<T, Integer> elementSizer;
    final @Nullable SerializableFunction<T, ? extends @Nullable Object> keyFn;

    // Per-bundle state, allocated once in setup and cleared at the start of each bundle.
    private transient @MonotonicNonNull Map<Object, KeyBuffer<T>> buffers;
    private transient @MonotonicNonNull Set<Object> liftedKeys;
    // Reset in startBundle; not transient so SpotBugs doesn't flag them as unrestored state.
    private long bufferedBytes;
    // Set once the memory cap has been exceeded in this bundle: keys that are neither buffered nor
    // lifted are no longer tracked and go to the small output immediately.
    private boolean keysFinalized;

    BundleLiftDoFn(
        TupleTag<T> smallBatchTag,
        TupleTag<T> largeBatchTag,
        long threshold,
        SerializableFunction<T, Integer> elementSizer) {
      this(smallBatchTag, largeBatchTag, threshold, elementSizer, null);
    }

    BundleLiftDoFn(
        TupleTag<T> smallBatchTag,
        TupleTag<T> largeBatchTag,
        long threshold,
        SerializableFunction<T, Integer> elementSizer,
        @Nullable SerializableFunction<T, ? extends @Nullable Object> keyFn) {
      this(
          smallBatchTag,
          largeBatchTag,
          threshold,
          defaultMaxBufferedBytes(threshold),
          elementSizer,
          keyFn);
    }

    BundleLiftDoFn(
        TupleTag<T> smallBatchTag,
        TupleTag<T> largeBatchTag,
        long threshold,
        long maxBufferedBytes,
        SerializableFunction<T, Integer> elementSizer,
        @Nullable SerializableFunction<T, ? extends @Nullable Object> keyFn) {
      this.smallBatchTag = smallBatchTag;
      this.largeBatchTag = largeBatchTag;
      this.threshold = threshold;
      this.maxBufferedBytes = maxBufferedBytes;
      this.elementSizer = elementSizer;
      this.keyFn = keyFn;
    }

    @Setup
    public void setup() {
      buffers = new LinkedHashMap<>();
      liftedKeys = new HashSet<>();
    }

    @StartBundle
    public void startBundle() {
      checkStateNotNull(buffers, "Buffers should be set by setup.").clear();
      checkStateNotNull(liftedKeys, "Lifted keys should be set by setup.").clear();
      bufferedBytes = 0L;
      keysFinalized = false;
    }

    @ProcessElement
    public void processElement(
        @Element T element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo pane,
        MultiOutputReceiver receiver) {
      Map<Object, KeyBuffer<T>> buffers =
          checkStateNotNull(this.buffers, "Buffers should be set by setup.");
      Set<Object> liftedKeys =
          checkStateNotNull(this.liftedKeys, "Lifted keys should be set by setup.");

      Object key;
      if (keyFn == null) {
        key = UNKEYED;
      } else {
        @Nullable Object userKey = keyFn.apply(element);
        key = new WindowedKey(window, pane, userKey == null ? UNKEYED : userKey);
      }

      if (liftedKeys.contains(key)) {
        // This key already reached the threshold in this bundle; stream it through.
        receiver.get(largeBatchTag).output(element);
        return;
      }

      KeyBuffer<T> buffer = buffers.get(key);
      if (buffer == null) {
        if (keysFinalized) {
          // The memory cap was exceeded earlier in this bundle; don't track new keys.
          receiver.get(smallBatchTag).output(element);
          return;
        }
        buffer = new KeyBuffer<>();
        buffers.put(key, buffer);
      }
      long elementBytes = elementSizer.apply(element);
      buffer.add(new BufferedElement<>(element, timestamp, window, pane), elementBytes);
      bufferedBytes += elementBytes;

      if (buffer.sizeBytes >= threshold) {
        buffers.remove(key);
        bufferedBytes -= buffer.sizeBytes;
        liftedKeys.add(key);
        LOG.debug(
            "Key reached {} estimated bytes; emitting {} buffered elements to tag: '{}'",
            buffer.sizeBytes,
            buffer.elements.size(),
            largeBatchTag.getId());
        emit(buffer, receiver.get(largeBatchTag));
        return;
      }

      if (bufferedBytes > maxBufferedBytes) {
        keysFinalized = true;
        evictSmallestKeys(buffers, receiver.get(smallBatchTag));
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      Map<Object, KeyBuffer<T>> buffers =
          checkStateNotNull(this.buffers, "Buffers should be set by setup.");
      for (KeyBuffer<T> buffer : buffers.values()) {
        LOG.debug(
            "Emitting {} elements of {} estimated bytes to tag: '{}'",
            buffer.elements.size(),
            buffer.sizeBytes,
            smallBatchTag.getId());
        for (BufferedElement<T> e : buffer.elements) {
          context.output(smallBatchTag, e.value, e.timestamp, e.window);
        }
      }
      buffers.clear();
      checkStateNotNull(this.liftedKeys).clear();
      bufferedBytes = 0L;
      keysFinalized = false;
    }

    /**
     * Buffered elements are re-emitted from {@link ProcessElement} with their own timestamps, which
     * may be earlier than the current element's. This is safe because they belong to the same
     * bundle, so the input watermark is still held at or before them.
     */
    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }

    /**
     * Emits the smallest keys to the small output until at most half of {@code maxBufferedBytes}
     * remains buffered, so each call frees a large chunk and the sort's cost stays amortized.
     */
    private void evictSmallestKeys(Map<Object, KeyBuffer<T>> buffers, OutputReceiver<T> small) {
      long target = maxBufferedBytes / 2;
      List<Map.Entry<Object, KeyBuffer<T>>> bySize = new ArrayList<>(buffers.entrySet());
      bySize.sort(Comparator.comparingLong(entry -> entry.getValue().sizeBytes));
      for (Map.Entry<Object, KeyBuffer<T>> entry : bySize) {
        if (bufferedBytes <= target) {
          break;
        }
        KeyBuffer<T> buffer = entry.getValue();
        buffers.remove(entry.getKey());
        bufferedBytes -= buffer.sizeBytes;
        emit(buffer, small);
      }
    }

    private static <T> void emit(KeyBuffer<T> buffer, OutputReceiver<T> receiver) {
      for (BufferedElement<T> e : buffer.elements) {
        receiver.outputWindowedValue(
            e.value, e.timestamp, Collections.singletonList(e.window), e.pane);
      }
    }
  }

  /** The buffered elements of one key, in arrival order. */
  private static class KeyBuffer<T> {
    private final List<BufferedElement<T>> elements = new ArrayList<>();
    private long sizeBytes;

    void add(BufferedElement<T> element, long elementBytes) {
      elements.add(element);
      sizeBytes += elementBytes;
    }
  }

  /** A buffered element with the metadata needed to re-emit it. */
  private static class BufferedElement<T> {
    private final T value;
    private final Instant timestamp;
    private final BoundedWindow window;
    private final PaneInfo pane;

    BufferedElement(T value, Instant timestamp, BoundedWindow window, PaneInfo pane) {
      this.value = value;
      this.timestamp = timestamp;
      this.window = window;
      this.pane = pane;
    }
  }

  /** A user key scoped to a window and pane. */
  private static class WindowedKey {
    private final BoundedWindow window;
    private final PaneInfo pane;
    private final Object key;

    WindowedKey(BoundedWindow window, PaneInfo pane, Object key) {
      this.window = window;
      this.pane = pane;
      this.key = key;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WindowedKey)) {
        return false;
      }
      WindowedKey other = (WindowedKey) o;
      return window.equals(other.window) && pane.equals(other.pane) && key.equals(other.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(window, pane, key);
    }
  }

  /** Sizes elements by their encoded size under the given coder. */
  private static class CoderSizer<T> implements SerializableFunction<T, Integer> {
    private final Coder<T> coder;

    CoderSizer(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public Integer apply(T element) {
      try {
        long bytes;
        if (coder.isRegisterByteSizeObserverCheap(element)) {
          SizeObserver observer = new SizeObserver();
          coder.registerByteSizeObserver(element, observer);
          observer.advance();
          bytes = observer.bytes;
        } else {
          bytes = CoderUtils.encodeToByteArray(coder, element).length;
        }
        return (int) Math.min(bytes, Integer.MAX_VALUE);
      } catch (Exception e) {
        throw new RuntimeException("Failed to estimate the encoded size of an element", e);
      }
    }
  }

  private static class SizeObserver extends ElementByteSizeObserver {
    private long bytes;

    @Override
    protected void reportElementSize(long elementByteSize) {
      bytes = elementByteSize;
    }
  }

  private BundleLifter(
      TupleTag<T> smallBatchTag,
      TupleTag<T> largeBatchTag,
      long threshold,
      long maxBufferedBytes,
      SerializableFunction<T, Integer> elementSizer,
      @Nullable SerializableFunction<T, ? extends @Nullable Object> keyFn) {
    if (smallBatchTag == null || largeBatchTag == null) {
      throw new IllegalArgumentException("smallBatchTag and largeBatchTag must not be null");
    }
    if (smallBatchTag.getId().equals(largeBatchTag.getId())) {
      throw new IllegalArgumentException("smallBatchTag and largeBatchTag must be different");
    }
    if (threshold <= 0) {
      throw new IllegalArgumentException("Threshold must be a positive integer");
    }
    if (maxBufferedBytes < threshold) {
      throw new IllegalArgumentException(
          "maxBufferedBytes must be at least the threshold, so a key can reach the threshold");
    }

    this.smallBatchTag = smallBatchTag;
    this.largeBatchTag = largeBatchTag;
    this.threshold = threshold;
    this.maxBufferedBytes = maxBufferedBytes;
    this.elementSizer = elementSizer;
    this.keyFn = keyFn;
  }

  /** Routes by element count: each element counts as one toward the threshold. */
  public static <T> BundleLifter<T> of(
      TupleTag<T> smallBatchTag, TupleTag<T> largeBatchTag, long threshold) {
    return of(smallBatchTag, largeBatchTag, threshold, x -> 1);
  }

  /** Routes by the sizes returned by {@code elementSizer}. */
  public static <T> BundleLifter<T> of(
      TupleTag<T> smallBatchTag,
      TupleTag<T> largeBatchTag,
      long threshold,
      SerializableFunction<T, Integer> elementSizer) {
    return new BundleLifter<>(
        smallBatchTag,
        largeBatchTag,
        threshold,
        defaultMaxBufferedBytes(threshold),
        elementSizer,
        null);
  }

  /** Routes by each element's encoded size under {@code coder}. */
  public static <T> BundleLifter<T> of(
      TupleTag<T> smallBatchTag, TupleTag<T> largeBatchTag, long threshold, Coder<T> coder) {
    return of(smallBatchTag, largeBatchTag, threshold, new CoderSizer<>(coder));
  }

  /**
   * Returns a copy that evaluates the threshold separately for each window, pane and key returned
   * by {@code keyFn}. Elements for which {@code keyFn} returns {@code null} share their own key.
   */
  public BundleLifter<T> withKeyFn(SerializableFunction<T, ? extends @Nullable Object> keyFn) {
    return new BundleLifter<>(
        smallBatchTag, largeBatchTag, threshold, maxBufferedBytes, elementSizer, keyFn);
  }

  /**
   * Returns a copy that buffers at most {@code maxBufferedBytes} per bundle across all keys (twice
   * the threshold by default). Must be at least the threshold.
   */
  public BundleLifter<T> withMaxBufferedBytes(long maxBufferedBytes) {
    return new BundleLifter<>(
        smallBatchTag, largeBatchTag, threshold, maxBufferedBytes, elementSizer, keyFn);
  }

  @Override
  public PCollectionTuple expand(PCollection<T> input) {
    final TupleTag<Void> mainOutputTag = new TupleTag<Void>() {};

    return input.apply(
        "BundleLiftDoFn",
        ParDo.of(
                new BundleLiftDoFn<>(
                    smallBatchTag, largeBatchTag, threshold, maxBufferedBytes, elementSizer, keyFn))
            .withOutputTags(mainOutputTag, TupleTagList.of(smallBatchTag).and(largeBatchTag)));
  }
}
