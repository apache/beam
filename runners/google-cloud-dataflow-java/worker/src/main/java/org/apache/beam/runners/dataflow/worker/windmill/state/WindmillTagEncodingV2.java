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

/** Encodes and decodes StateTags and TimerTags from and to windmill bytes */
@Internal
@ThreadSafe
public class WindmillTagEncodingV2 extends WindmillTagEncoding {

  private static final WindmillTagEncodingV2 INSTANCE = new WindmillTagEncodingV2();

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
        stream.write(0x03); // System namespace prefix
      } else if (WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.equals(prefix)) {
        stream.write(0x04); // User namespace prefix
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
      if (nextByte == 0x03) { // System namespace prefix
        checkState(WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX.equals(prefix));
      } else if (nextByte == 0x04) { // User namespace prefix
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
      stream.write(0x01); // System tag
    } else {
      stream.write(0x02); // User tag
    }
    StringUtf8Coder.of().encode(tag.getId(), stream);
  }

  private void encodeNameSpace(StateNamespace namespace, ByteStringOutputStream stream)
      throws IOException {
    if (namespace instanceof GlobalNamespace) {
      // Single byte 0x01 for GlobalNamespace.
      stream.write(0x01);
    } else if (namespace instanceof WindowNamespace) {
      // Single byte 0x10 for Non-Global namespace.
      stream.write(0x10);
      encodeWindowNamespace((WindowNamespace<? extends BoundedWindow>) namespace, stream);
    } else if (namespace instanceof WindowAndTriggerNamespace) {
      // Single byte 0x10 for Non-Global namespace.
      stream.write(0x10);
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
      case 0x01: // GlobalNamespace
        return StateNamespaces.global();
      case 0x10: // Non-Global namespace
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
      case 0x1: // Window namespace
        return StateNamespaces.window(windowCoder, window);
      case 0x2: // Window and trigger namespace
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
      case 0x64: // IntervalWindow
        window = (W) decodeIntervalWindow(stream);
        break;
      case 0x02: // Other Windows
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
    stream.write(0x01); // Window namespace
  }

  private <W extends BoundedWindow> void encodeWindowAndTriggerNamespace(
      WindowAndTriggerNamespace<W> windowAndTriggerNamespace, ByteStringOutputStream stream)
      throws IOException {
    encodeWindow(
        windowAndTriggerNamespace.getWindow(), windowAndTriggerNamespace.getWindowCoder(), stream);
    stream.write(0x02); // Window and trigger namespace
    BigEndianIntegerCoder.of().encode(windowAndTriggerNamespace.getTriggerIndex(), stream);
  }

  private <W extends BoundedWindow> void encodeWindow(
      W window, Coder<W> windowCoder, ByteStringOutputStream stream) throws IOException {
    if (windowCoder instanceof IntervalWindowCoder) {
      stream.write(0x64); // IntervalWindow
      InstantCoder.of().encode(((IntervalWindow) window).end(), stream);
      InstantCoder.of().encode(((IntervalWindow) window).start(), stream);
    } else {
      stream.write(0x02); // Other Windows
      windowCoder.encode(window, stream);
    }
  }
}
