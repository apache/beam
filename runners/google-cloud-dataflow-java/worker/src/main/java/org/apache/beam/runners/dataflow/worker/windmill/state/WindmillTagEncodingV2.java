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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.GlobalNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream.StreamHandle;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.joda.time.Instant;

/**
 * Encodes and decodes StateTags and TimerTags from and to windmill bytes. This encoding scheme
 * enforces a specific lexicographical order on state tags. The ordering enables building range
 * filters using the tags.
 *
 * <h2>1. High-Level Tag Formats</h2>
 *
 * <p>State tags and Timer tags differ in structure but share common component encodings.
 *
 * <h3>1.1 State Tag Encoding</h3>
 *
 * <p>Used for generic state variables (e.g., ValueState, BagState, etc).
 *
 * <pre>
 * Format:
 * | Encoded Namespace | Encoded Address |
 * </pre>
 *
 * <ul>
 *   <li><b>Encoded Namespace:</b> Encodes the state namespace (see Section 2.1).
 *   <li><b>Encoded Address:</b> Encodes the state variable address (see Section 2.3).
 * </ul>
 *
 * <h3>1.2 Timer/Timer Hold Tag Encoding</h3>
 *
 * <p>Specialized tags, used for timers and automatic watermark holds associated with the timers.
 *
 * <pre>
 * Format:
 * | Encoded Namespace | Tag Type | Timer Family Id | Timer Id |
 *
 * +-------------------+-----------------------------------------------------------+
 * | Field             | Format                                                    |
 * +-------------------+-----------------------------------------------------------+
 * | Encoded Namespace | Encoded namespace (see Section 2.1).                      |
 * +-------------------+-----------------------------------------------------------+
 * | Tag Type          | {@code 0x03} (Single byte): System Timer/Watermark Hold   |
 * |                   | {@code 0x04} (Single byte): User Timer/Watermark Hold     |
 * +-------------------+-----------------------------------------------------------+
 * | Timer Family ID   | TimerFamilyId encoded via length prefixed                 |
 * |                   | {@code StringUtf8Coder}.                                  |
 * +-------------------+-----------------------------------------------------------+
 * | Timer ID          | TimerId encoded via length prefixed                       |
 * |                   | {@code StringUtf8Coder}.                                  |
 * +-------------------+-----------------------------------------------------------+
 * </pre>
 *
 * <h2>2. Component Encodings</h2>
 *
 * <h3>2.1 Namespace Encoding</h3>
 *
 * <p>Namespaces are prefixed with a byte ID to control sorting order.
 *
 * <pre>
 * +---------------------------+-------------------------------------------------------------+
 * | Namespace Type            | Format                                                      |
 * +---------------------------+-------------------------------------------------------------+
 * | GlobalNamespace           | | {@code 0x01} |                                            |
 * |                           | (Single byte)                                               |
 * +---------------------------+-------------------------------------------------------------+
 * | WindowNamespace           | | {@code 0x10} | Encoded Window | {@code 0x01} |            |
 * |                           | (See Section 2.2)                                           |
 * +---------------------------+-------------------------------------------------------------+
 * | WindowAndTriggerNamespace | | {@code 0x10} | Encoded Window | {@code 0x02} | TriggerIndex |
 * |                           | (See Section 2.2 for Encoded Window)                        |
 * |                           | TriggerIndex is encoded by {@code BigEndianIntegerCoder}    |
 * +---------------------------+-------------------------------------------------------------+
 * </pre>
 *
 * <h3>2.2 Window Encoding</h3>
 *
 * <h4>2.2.1 IntervalWindow</h4>
 *
 * <p>IntervalWindows use a custom encoding that is different from the IntervalWindowCoder.
 *
 * <pre>
 * Format:
 * | 0x64 | End Time | Start Time |
 * </pre>
 *
 * <ul>
 *   <li><b>Prefix:</b> {@code 0x64}. Single byte identifying Interval windows.
 *   <li><b>End Time:</b> {@code intervalWindow.end()} encoded via {@code InstantCoder}.
 *   <li><b>Start Time:</b> {@code intervalWindow.start()} encoded via {@code InstantCoder}.
 * </ul>
 *
 * <p><b>Note:</b> {@code InstantCoder} preserves the sort order. The encoded IntervalWindow is to
 * be sorted based on {@code [End Time, Start Time]} directly without needing to decode.
 *
 * <h4>2.2.2 Other Windows</h4>
 *
 * <p>All non-IntervalWindows use the standard window coders.
 *
 * <pre>
 * Format:
 * | 0x02 | Window |
 * </pre>
 *
 * <ul>
 *   <li><b>Prefix:</b> {@code 0x02}. Single byte identifying non-Interval windows.
 *   <li><b>Window:</b> The window serialized using its {@code windowCoder}.
 * </ul>
 *
 * <h3>2.3 Address Encoding</h3>
 *
 * <p>Combines the state type and the state identifier.
 *
 * <pre>
 * Format:
 * | State Type | Address |
 *
 * +------------+-----------------------------------------------------------------+
 * | Field      | Format                                                          |
 * +------------+-----------------------------------------------------------------+
 * | State Type | {@code 0x01} (Single byte): System State                        |
 * |            | {@code 0x02} (Single byte): User State                          |
 * +------------+-----------------------------------------------------------------+
 * | Address    | The state address (string) is encoded via length prefixed       |
 * |            | {@code StringUtf8Coder}.                                        |
 * +------------+-----------------------------------------------------------------+
 * </pre>
 *
 * <h2>3. Tag Ordering</h2>
 *
 * <p>The encoding prefixes are chosen to enforce the following lexicographical sort order (lowest
 * to highest):
 *
 * <ol>
 *   <li><b>Tags in Global Namespace</b> (Prefix {@code 0x01})
 *   <li><b>Tags in Non-Interval Windows</b> (Prefix {@code 0x1002})
 *   <li><b>Tags in Interval Windows</b> (Prefix {@code 0x1064})
 *       <ul>
 *         <li>Sorted internally by {@code [EndTime, StartTime]}.
 *       </ul>
 * </ol>
 */
@Internal
@ThreadSafe
public class WindmillTagEncodingV2 extends WindmillTagEncoding {

  private static final WindmillTagEncodingV2 INSTANCE = new WindmillTagEncodingV2();
  private static final int WINDOW_NAMESPACE_BYTE = 0x01;
  private static final int WINDOW_AND_TRIGGER_NAMESPACE_BYTE = 0x02;
  private static final int NON_GLOBAL_NAMESPACE_BYTE = 0x10;
  private static final int GLOBAL_NAMESPACE_BYTE = 0x01;
  private static final int SYSTEM_STATE_TAG_BYTE = 0x01;
  private static final int USER_STATE_TAG_BYTE = 0x02;
  private static final int SYSTEM_TIMER_BYTE = 0x03;
  private static final int USER_TIMER_BYTE = 0x04;
  private static final int INTERVAL_WINDOW_BYTE = 0x64;
  private static final int OTHER_WINDOW_BYTE = 0x02;

  // Private constructor to prevent instantiations from outside.
  private WindmillTagEncodingV2() {}

  /** {@inheritDoc} */
  @Override
  public InternedByteString stateTag(StateNamespace namespace, StateTag<?> address) {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      encodeNameSpace(namespace, stream);
      encodeAddress(address, stream);
      return InternedByteString.of(stream.toByteStringAndReset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ByteString timerHoldTag(
      WindmillNamespacePrefix prefix, TimerData timerData, ByteString timerTag) {
    // Same encoding for timer tag and timer hold tag.
    // They are put in different places and won't collide.
    return timerTag;
  }

  /** {@inheritDoc} */
  @Override
  public ByteString timerTag(WindmillNamespacePrefix prefix, TimerData timerData) {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      encodeNameSpace(timerData.getNamespace(), stream);
      if (WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX.equals(prefix)) {
        stream.write(SYSTEM_TIMER_BYTE);
      } else if (WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.equals(prefix)) {
        stream.write(USER_TIMER_BYTE);
      } else {
        throw new IllegalStateException("Unexpected WindmillNamespacePrefix" + prefix);
      }
      StringUtf8Coder.of().encode(timerData.getTimerFamilyId(), stream);
      StringUtf8Coder.of().encode(timerData.getTimerId(), stream);
      return stream.toByteStringAndReset();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public TimerData windmillTimerToTimerData(
      WindmillNamespacePrefix prefix,
      Timer timer,
      Coder<? extends BoundedWindow> windowCoder,
      boolean draining) {

    InputStream stream = timer.getTag().newInput();

    try {
      StateNamespace stateNamespace = decodeNameSpace(stream, windowCoder);
      int nextByte = stream.read();
      if (nextByte == SYSTEM_TIMER_BYTE) {
        checkState(WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX.equals(prefix));
      } else if (nextByte == USER_TIMER_BYTE) {
        checkState(WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.equals(prefix));
      } else {
        throw new IllegalStateException("Unexpected timer tag byte: " + nextByte);
      }

      String timerFamilyId = StringUtf8Coder.of().decode(stream);
      String timerId = StringUtf8Coder.of().decode(stream);

      Instant timestamp = WindmillTimeUtils.windmillToHarnessTimestamp(timer.getTimestamp());
      Instant outputTimestamp = timestamp;
      if (timer.hasMetadataTimestamp()) {
        // We use BoundedWindow.TIMESTAMP_MAX_VALUE+1 to indicate "no output timestamp" so make sure
        // to change the upper bound.
        outputTimestamp =
            WindmillTimeUtils.windmillToHarnessTimestamp(timer.getMetadataTimestamp());
        if (outputTimestamp.equals(OUTPUT_TIMESTAMP_MAX_WINDMILL_VALUE)) {
          outputTimestamp = OUTPUT_TIMESTAMP_MAX_VALUE;
        }
      }

      return TimerData.of(
          timerId,
          timerFamilyId,
          stateNamespace,
          timestamp,
          outputTimestamp,
          timerTypeToTimeDomain(timer.getType()));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // todo add draining (https://github.com/apache/beam/issues/36884)
  }

  /** @return the singleton WindmillStateTagUtil */
  public static WindmillTagEncodingV2 instance() {
    return INSTANCE;
  }

  private void encodeAddress(StateTag<?> tag, ByteStringOutputStream stream) throws IOException {
    if (StateTags.isSystemTagInternal(tag)) {
      stream.write(SYSTEM_STATE_TAG_BYTE);
    } else {
      stream.write(USER_STATE_TAG_BYTE);
    }
    StringUtf8Coder.of().encode(tag.getId(), stream);
  }

  private void encodeNameSpace(StateNamespace namespace, ByteStringOutputStream stream)
      throws IOException {
    if (namespace instanceof GlobalNamespace) {
      stream.write(GLOBAL_NAMESPACE_BYTE);
    } else if (namespace instanceof WindowNamespace) {
      stream.write(NON_GLOBAL_NAMESPACE_BYTE);
      encodeWindowNamespace((WindowNamespace<? extends BoundedWindow>) namespace, stream);
    } else if (namespace instanceof WindowAndTriggerNamespace) {
      stream.write(NON_GLOBAL_NAMESPACE_BYTE);
      encodeWindowAndTriggerNamespace(
          (WindowAndTriggerNamespace<? extends BoundedWindow>) namespace, stream);
    } else {
      throw new IllegalStateException("Unsupported namespace type: " + namespace.getClass());
    }
  }

  private StateNamespace decodeNameSpace(
      InputStream stream, Coder<? extends BoundedWindow> windowCoder) throws IOException {
    int firstByte = stream.read();
    switch (firstByte) {
      case GLOBAL_NAMESPACE_BYTE:
        return StateNamespaces.global();
      case NON_GLOBAL_NAMESPACE_BYTE:
        return decodeNonGlobalNamespace(stream, windowCoder);
      default:
        throw new IllegalStateException("Invalid first namespace byte: " + firstByte);
    }
  }

  private <W extends BoundedWindow> StateNamespace decodeNonGlobalNamespace(
      InputStream stream, Coder<W> windowCoder) throws IOException {
    W window = decodeWindow(stream, windowCoder);
    int namespaceByte = stream.read();
    switch (namespaceByte) {
      case WINDOW_NAMESPACE_BYTE:
        return StateNamespaces.window(windowCoder, window);
      case WINDOW_AND_TRIGGER_NAMESPACE_BYTE:
        Integer triggerIndex = BigEndianIntegerCoder.of().decode(stream);
        return StateNamespaces.windowAndTrigger(windowCoder, window, triggerIndex);
      default:
        throw new IllegalStateException("Invalid trigger namespace byte: " + namespaceByte);
    }
  }

  private <W extends BoundedWindow> W decodeWindow(InputStream stream, Coder<W> windowCoder)
      throws IOException {
    int firstByte = stream.read();
    W window;
    switch (firstByte) {
      case INTERVAL_WINDOW_BYTE:
        window = (W) decodeIntervalWindow(stream);
        break;
      case OTHER_WINDOW_BYTE:
        window = windowCoder.decode(stream);
        break;
      default:
        throw new IllegalStateException("Unexpected window first byte: " + firstByte);
    }
    return window;
  }

  private IntervalWindow decodeIntervalWindow(InputStream stream) throws IOException {
    Instant end = InstantCoder.of().decode(stream);
    Instant start = InstantCoder.of().decode(stream);
    return new IntervalWindow(start, end);
  }

  private <W extends BoundedWindow> void encodeWindowNamespace(
      WindowNamespace<W> windowNamespace, ByteStringOutputStream stream) throws IOException {
    encodeWindow(windowNamespace.getWindow(), windowNamespace.getWindowCoder(), stream);
    stream.write(WINDOW_NAMESPACE_BYTE);
  }

  private <W extends BoundedWindow> void encodeWindowAndTriggerNamespace(
      WindowAndTriggerNamespace<W> windowAndTriggerNamespace, ByteStringOutputStream stream)
      throws IOException {
    encodeWindow(
        windowAndTriggerNamespace.getWindow(), windowAndTriggerNamespace.getWindowCoder(), stream);
    stream.write(WINDOW_AND_TRIGGER_NAMESPACE_BYTE);
    BigEndianIntegerCoder.of().encode(windowAndTriggerNamespace.getTriggerIndex(), stream);
  }

  private <W extends BoundedWindow> void encodeWindow(
      W window, Coder<W> windowCoder, ByteStringOutputStream stream) throws IOException {
    if (windowCoder instanceof IntervalWindowCoder) {
      stream.write(INTERVAL_WINDOW_BYTE);
      InstantCoder.of().encode(((IntervalWindow) window).end(), stream);
      InstantCoder.of().encode(((IntervalWindow) window).start(), stream);
    } else {
      stream.write(OTHER_WINDOW_BYTE);
      windowCoder.encode(window, stream);
    }
  }
}
