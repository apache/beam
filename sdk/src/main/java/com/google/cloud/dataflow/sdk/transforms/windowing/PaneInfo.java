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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Provides information about the pane this value belongs to. Every pane is implicitly associated
 * with a window.
 *
 * <p> Note: This does not uniquely identify a pane, and should not be used for comparisons.
 */
public final class PaneInfo {

  /**
   * Enumerates the possibilities for how the timing of this pane firing related to the watermark.
   */
  public enum Timing {
    /** Pane was fired before the watermark passed the end of the window. */
    EARLY,
    /** First pane fired after the watermark passed the end of the window. */
    ON_TIME,
    /** Panes fired after the {@code ON_TIME} firing. */
    LATE,
    /**
     * This element was not produced in a triggered pane and its relation to the watermark is
     * unknown.
     */
    UNKNOWN;

    // NOTE: Do not add fields or re-order them. The ordinal is used as part of
    // the encoding.
  }

  private static byte encodedByte(boolean isFirst, boolean isLast, Timing timing) {
    byte result = 0x0;
    if (isFirst) {
      result |= 1;
    }
    if (isLast) {
      result |= 2;
    }
    result |= timing.ordinal() << 2;
    return result;
  }

  private static final ImmutableMap<Byte, PaneInfo> BYTE_TO_PANE_INFO;
  static {
    ImmutableMap.Builder<Byte, PaneInfo> decodingBuilder = ImmutableMap.builder();
    for (Timing timing : Timing.values()) {
      register(decodingBuilder, new PaneInfo(true, true, timing));
      register(decodingBuilder, new PaneInfo(true, false, timing));
      register(decodingBuilder, new PaneInfo(false, true, timing));
      register(decodingBuilder, new PaneInfo(false, false, timing));
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

  /**
   * Until an element has been assigned to a window and had triggers processed, it doesn't belong
   * to any pane. This is the default value assigned to elements read from sources, and those that
   * have been assigned a window but not passed through execution of any trigger.
   */
  public static final PaneInfo DEFAULT = PaneInfo.createPane(false, false, Timing.UNKNOWN);

  /**
   * PaneInfo to use when there will be exactly one firing and it is on time.
   */
  public static final PaneInfo ON_TIME_AND_ONLY_FIRING =
      PaneInfo.createPane(true, true, Timing.ON_TIME);

  private PaneInfo(boolean isFirst, boolean isLast, Timing timing) {
    this.encodedByte = encodedByte(isFirst, isLast, timing);
    this.isFirst = isFirst;
    this.isLast = isLast;
    this.timing = timing;
  }

  /**
   * Returns true if this pane corresponds to the {@link #DEFAULT} pane.
   */
  public boolean isDefault() {
    return DEFAULT.equals(this);
  }

  /**
   * Factory method to create a {@link PaneInfo} with the specified parameters.
   */
  public static PaneInfo createPane(
      boolean isFirst, boolean isLast, Timing timing) {
    return Preconditions.checkNotNull(BYTE_TO_PANE_INFO.get(encodedByte(isFirst, isLast, timing)));
  }

  public static PaneInfo decodePane(byte encodedPane) {
    return Preconditions.checkNotNull(BYTE_TO_PANE_INFO.get(encodedPane));
  }

  /**
   * Return true if there is no timing information for the current {@link PaneInfo}.
   * This typically indicates that the current element has not been assigned to
   * windows or passed through an operation that executes triggers yet.
   */
  public boolean isUnknown() {
    return Timing.UNKNOWN.equals(timing);
  }

  /**
   * Return true if this is the first pane produced for the associated window.
   */
  public boolean isFirst() {
    return isFirst;
  }

  /**
   * Return true if this is the last pane that will be produced in the associated window.
   */
  public boolean isLast() {
    return isLast;
  }

  /**
   * Return true if this is the last pane that will be produced in the associated window.
   */
  public Timing getTiming() {
    return timing;
  }

  int getEncodedByte() {
    return encodedByte;
  }

  @Override
  public int hashCode() {
    // Just hash the encoded byte, because we know that it uniquely identifies the pane.
    return Objects.hash(encodedByte);
  }

  @Override
  public boolean equals(Object obj) {
    // Because we intern the PaneInfo objects, equals is the same as pointer equality.
    return this == obj;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .omitNullValues()
        .add("isFirst", isFirst ? true : null)
        .add("isLast", isLast ? true : null)
        .add("timing", timing)
        .toString();
  }

  /**
   * A Coder for encoding PaneInfo instances.
   */
  public static class PaneInfoCoder extends AtomicCoder<PaneInfo> {
    private static final long serialVersionUID = 0;

    public static final PaneInfoCoder INSTANCE = new PaneInfoCoder();

    @Override
    public void encode(PaneInfo value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      outStream.write(value.encodedByte);
    }

    @Override
    public PaneInfo decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      byte key = (byte) inStream.read();
      return BYTE_TO_PANE_INFO.get(key);
    }
  }
}
