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
package org.apache.beam.sdk.transforms.windowing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.WindowedContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides information about the pane an element belongs to. Every pane is implicitly associated
 * with a window. Panes are observable only via the {@link DoFn.ProcessContext#pane} method of the
 * context passed to a {@link DoFn.ProcessElement} method.
 *
 * <p>Note: This does not uniquely identify a pane, and should not be used for comparisons.
 */
public final class PaneInfo {
  /**
   * Enumerates the possibilities for the timing of this pane firing related to the input and output
   * watermarks for its computation.
   *
   * <p>A window may fire multiple panes, and the timing of those panes generally follows the
   * regular expression {@code EARLY* ON_TIME? LATE*}. Generally a pane is considered:
   *
   * <ol>
   *   <li>{@code EARLY} if the system cannot be sure it has seen all data which may contribute to
   *       the pane's window.
   *   <li>{@code ON_TIME} if the system predicts it has seen all the data which may contribute to
   *       the pane's window.
   *   <li>{@code LATE} if the system has encountered new data after predicting no more could
   *       arrive. It is possible an {@code ON_TIME} pane has already been emitted, in which case
   *       any following panes are considered {@code LATE}.
   * </ol>
   *
   * <p>Only an {@link AfterWatermark#pastEndOfWindow} trigger may produce an {@code ON_TIME} pane.
   * With merging {@link WindowFn}'s, windows may be merged to produce new windows that satisfy
   * their own instance of the above regular expression. The only guarantee is that once a window
   * produces a final pane, it will not be merged into any new windows.
   *
   * <p>The predictions above are made using the mechanism of watermarks.
   *
   * <p>We can state some properties of {@code LATE} and {@code ON_TIME} panes, but first need some
   * definitions:
   *
   * <ol>
   *   <li>We'll call a pipeline 'simple' if it does not use {@link
   *       WindowedContext#outputWithTimestamp} in any {@link DoFn}, and it uses the same {@link
   *       org.apache.beam.sdk.transforms.windowing.Window#withAllowedLateness} argument value on
   *       all windows (or uses the default of {@link org.joda.time.Duration#ZERO}).
   *   <li>We'll call an element 'locally late', from the point of view of a computation on a
   *       worker, if the element's timestamp is before the input watermark for that computation on
   *       that worker. The element is otherwise 'locally on-time'.
   *   <li>We'll say 'the pane's timestamp' to mean the timestamp of the element produced to
   *       represent the pane's contents.
   * </ol>
   *
   * <p>Then in simple pipelines:
   *
   * <ol>
   *   <li>(Soundness) An {@code ON_TIME} pane can never cause a later computation to generate a
   *       {@code LATE} pane. (If it did, it would imply a later computation's input watermark
   *       progressed ahead of an earlier stage's output watermark, which by design is not
   *       possible.)
   *   <li>(Liveness) An {@code ON_TIME} pane is emitted as soon as possible after the input
   *       watermark passes the end of the pane's window.
   *   <li>(Consistency) A pane with only locally on-time elements will always be {@code ON_TIME}.
   *       And a {@code LATE} pane cannot contain locally on-time elements.
   * </ol>
   *
   * <p>However, note that:
   *
   * <ol>
   *   <li>An {@code ON_TIME} pane may contain locally late elements. It may even contain only
   *       locally late elements. Provided a locally late element finds its way into an {@code
   *       ON_TIME} pane its lateness becomes unobservable.
   *   <li>A {@code LATE} pane does not necessarily cause any following computation panes to be
   *       marked as {@code LATE}.
   * </ol>
   */
  public enum Timing {
    /** Pane was fired before the input watermark had progressed after the end of the window. */
    EARLY,
    /**
     * Pane was fired by a {@link AfterWatermark#pastEndOfWindow} trigger because the input
     * watermark progressed after the end of the window. However the output watermark has not yet
     * progressed after the end of the window. Thus it is still possible to assign a timestamp to
     * the element representing this pane which cannot be considered locally late by any following
     * computation.
     */
    ON_TIME,
    /** Pane was fired after the output watermark had progressed past the end of the window. */
    LATE,
    /**
     * This element was not produced in a triggered pane and its relation to input and output
     * watermarks is unknown.
     */
    UNKNOWN

    // NOTE: Do not add fields or re-order them. The ordinal is used as part of
    // the encoding.
  }

  private static byte encodedByte(boolean isFirst, boolean isLast, Timing timing) {
    byte result = 0b00;
    if (isFirst) {
      result |= (byte) 0b01;
    }
    if (isLast) {
      result |= (byte) 0b10;
    }
    result |= (byte) (timing.ordinal() << 2);
    return result;
  }

  private static final ImmutableMap<Byte, PaneInfo> BYTE_TO_PANE_INFO;

  static {
    ImmutableMap.Builder<Byte, PaneInfo> decodingBuilder = ImmutableMap.builder();
    for (Timing timing : Timing.values()) {
      long onTimeIndex = timing == Timing.EARLY ? -1 : 0;
      register(decodingBuilder, new PaneInfo(true, true, timing, 0, onTimeIndex));
      register(decodingBuilder, new PaneInfo(true, false, timing, 0, onTimeIndex));
      register(decodingBuilder, new PaneInfo(false, true, timing, -1, onTimeIndex));
      register(decodingBuilder, new PaneInfo(false, false, timing, -1, onTimeIndex));
    }
    BYTE_TO_PANE_INFO = decodingBuilder.build();
  }

  private static void register(ImmutableMap.Builder<Byte, PaneInfo> builder, PaneInfo info) {
    builder.put(info.encodedByte, info);
  }

  private final byte encodedByte;

  private final boolean isFirst;
  private final boolean isLast;
  private final Timing timing;
  private final long index;
  private final long nonSpeculativeIndex;

  /**
   * {@code PaneInfo} to use for elements on (and before) initial window assignment (including
   * elements read from sources) before they have passed through a {@link GroupByKey} and are
   * associated with a particular trigger firing.
   */
  public static final PaneInfo NO_FIRING = PaneInfo.createPane(true, true, Timing.UNKNOWN, 0, 0);

  /** {@code PaneInfo} to use when there will be exactly one firing and it is on time. */
  public static final PaneInfo ON_TIME_AND_ONLY_FIRING =
      PaneInfo.createPane(true, true, Timing.ON_TIME, 0, 0);

  private PaneInfo(boolean isFirst, boolean isLast, Timing timing, long index, long onTimeIndex) {
    this.encodedByte = encodedByte(isFirst, isLast, timing);
    this.isFirst = isFirst;
    this.isLast = isLast;
    this.timing = timing;
    this.index = index;
    this.nonSpeculativeIndex = onTimeIndex;
  }

  public static PaneInfo createPane(boolean isFirst, boolean isLast, Timing timing) {
    checkArgument(isFirst, "Indices must be provided for non-first pane info.");
    return createPane(isFirst, isLast, timing, 0, timing == Timing.EARLY ? -1 : 0);
  }

  /** Factory method to create a {@link PaneInfo} with the specified parameters. */
  public static PaneInfo createPane(
      boolean isFirst, boolean isLast, Timing timing, long index, long onTimeIndex) {
    if (isFirst || timing == Timing.UNKNOWN) {
      return checkNotNull(BYTE_TO_PANE_INFO.get(encodedByte(isFirst, isLast, timing)));
    } else {
      return new PaneInfo(isFirst, isLast, timing, index, onTimeIndex);
    }
  }

  public static PaneInfo decodePane(byte encodedPane) {
    return checkNotNull(BYTE_TO_PANE_INFO.get(encodedPane));
  }

  /**
   * Return true if there is no timing information for the current {@link PaneInfo}. This typically
   * indicates that the current element has not been assigned to windows or passed through an
   * operation that executes triggers yet.
   */
  public boolean isUnknown() {
    return Timing.UNKNOWN.equals(timing);
  }

  /** Return true if this is the first pane produced for the associated window. */
  public boolean isFirst() {
    return isFirst;
  }

  /** Return true if this is the last pane that will be produced in the associated window. */
  public boolean isLast() {
    return isLast;
  }

  /** Return the timing of this pane. */
  public Timing getTiming() {
    return timing;
  }

  /**
   * The zero-based index of this trigger firing that produced this pane.
   *
   * <p>This will return 0 for the first time the timer fires, 1 for the next time, etc.
   *
   * <p>A given (key, window, pane-index) is guaranteed to be unique in the output of a group-by-key
   * operation.
   */
  public long getIndex() {
    return index;
  }

  /**
   * The zero-based index of this trigger firing among non-speculative panes.
   *
   * <p>This will return 0 for the first non-{@link Timing#EARLY} timer firing, 1 for the next one,
   * etc.
   *
   * <p>Always -1 for speculative data.
   */
  public long getNonSpeculativeIndex() {
    return nonSpeculativeIndex;
  }

  int getEncodedByte() {
    return encodedByte;
  }

  @Override
  public int hashCode() {
    return Objects.hash(encodedByte, index, nonSpeculativeIndex);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      // Simple PaneInfos are interned.
      return true;
    } else if (obj instanceof PaneInfo) {
      PaneInfo that = (PaneInfo) obj;
      return this.encodedByte == that.encodedByte
          && this.index == that.index
          && this.nonSpeculativeIndex == that.nonSpeculativeIndex;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    if (this.equals(PaneInfo.NO_FIRING)) {
      return "PaneInfo.NO_FIRING";
    }

    return MoreObjects.toStringHelper(getClass())
        .omitNullValues()
        .add("isFirst", isFirst ? true : null)
        .add("isLast", isLast ? true : null)
        .add("timing", timing)
        .add("index", index)
        .add("onTimeIndex", nonSpeculativeIndex != -1 ? nonSpeculativeIndex : null)
        .toString();
  }

  /** A Coder for encoding PaneInfo instances. */
  public static class PaneInfoCoder extends AtomicCoder<PaneInfo> {
    private enum Encoding {
      FIRST,
      ONE_INDEX,
      TWO_INDICES;

      // NOTE: Do not reorder fields. The ordinal is used as part of
      // the encoding.

      public final byte tag;

      Encoding() {
        assert ordinal() < 16;
        tag = (byte) (ordinal() << 4);
      }

      public static Encoding fromTag(byte b) {
        return Encoding.values()[b >> 4];
      }
    }

    private Encoding chooseEncoding(PaneInfo value) {
      if ((value.index == 0 && value.nonSpeculativeIndex == 0) || value.timing == Timing.UNKNOWN) {
        return Encoding.FIRST;
      } else if (value.index == value.nonSpeculativeIndex || value.timing == Timing.EARLY) {
        return Encoding.ONE_INDEX;
      } else {
        return Encoding.TWO_INDICES;
      }
    }

    public static final PaneInfoCoder INSTANCE = new PaneInfoCoder();

    public static PaneInfoCoder of() {
      return INSTANCE;
    }

    private PaneInfoCoder() {}

    @Override
    public void encode(PaneInfo value, final OutputStream outStream)
        throws CoderException, IOException {
      Encoding encoding = chooseEncoding(value);
      switch (chooseEncoding(value)) {
        case FIRST:
          outStream.write(value.encodedByte);
          break;
        case ONE_INDEX:
          outStream.write(value.encodedByte | encoding.tag);
          VarInt.encode(value.index, outStream);
          break;
        case TWO_INDICES:
          outStream.write(value.encodedByte | encoding.tag);
          VarInt.encode(value.index, outStream);
          VarInt.encode(value.nonSpeculativeIndex, outStream);
          break;
        default:
          throw new CoderException("Unknown encoding " + encoding);
      }
    }

    @Override
    public PaneInfo decode(final InputStream inStream) throws CoderException, IOException {
      byte keyAndTag = (byte) inStream.read();
      PaneInfo base = BYTE_TO_PANE_INFO.get((byte) (keyAndTag & 0x0F));
      long index, onTimeIndex;
      switch (Encoding.fromTag(keyAndTag)) {
        case FIRST:
          return base;
        case ONE_INDEX:
          index = VarInt.decodeLong(inStream);
          onTimeIndex = base.timing == Timing.EARLY ? -1 : index;
          break;
        case TWO_INDICES:
          index = VarInt.decodeLong(inStream);
          onTimeIndex = VarInt.decodeLong(inStream);
          break;
        default:
          throw new CoderException("Unknown encoding " + (keyAndTag & 0xF0));
      }
      return new PaneInfo(base.isFirst, base.isLast, base.timing, index, onTimeIndex);
    }

    @Override
    public void verifyDeterministic() {}
  }
}
