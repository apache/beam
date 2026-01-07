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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream.StreamHandle;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.joda.time.Instant;

@Internal
@ThreadSafe
public class WindmillTagEncodingV1 extends WindmillTagEncoding {

  private static final String TIMER_HOLD_PREFIX = "/h";
  private static final WindmillTagEncodingV1 INSTANCE = new WindmillTagEncodingV1();

  // Private constructor to prevent instantiations from outside.
  private WindmillTagEncodingV1() {}

  /** {@inheritDoc} */
  @Override
  public InternedByteString stateTag(StateNamespace namespace, StateTag<?> address) {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      // Use ByteStringOutputStream rather than concatenation and String.format. We build these keys
      // a lot, and this leads to better performance results. See associated benchmarks.
      ByteStringOutputStream stream = streamHandle.stream();
      // stringKey starts and ends with a slash.  We separate it from the
      // StateTag ID by a '+' (which is guaranteed not to be in the stringKey) because the
      // ID comes from the user.
      namespace.appendTo(stream);
      stream.append('+');
      address.appendTo(stream);
      return InternedByteString.of(stream.toByteStringAndReset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ByteString timerHoldTag(
      WindmillNamespacePrefix prefix, TimerData timerData, ByteString timerTag) {
    String tagString;
    if ("".equals(timerData.getTimerFamilyId())) {
      tagString =
          prefix.byteString().toStringUtf8()
              + // this never ends with a slash
              TIMER_HOLD_PREFIX
              + // this never ends with a slash
              timerData.getNamespace().stringKey()
              + // this must begin and end with a slash
              '+'
              + timerData.getTimerId() // this is arbitrary; currently unescaped
      ;
    } else {
      tagString =
          prefix.byteString().toStringUtf8()
              + // this never ends with a slash
              TIMER_HOLD_PREFIX
              + // this never ends with a slash
              timerData.getNamespace().stringKey()
              + // this must begin and end with a slash
              '+'
              + timerData.getTimerId()
              + // this is arbitrary; currently unescaped
              '+'
              + timerData.getTimerFamilyId() // use to differentiate same timerId in different
      // timerMap
      ;
    }
    return ByteString.copyFromUtf8(tagString);
  }

  /** {@inheritDoc} */
  @Override
  public ByteString timerTag(WindmillNamespacePrefix prefix, TimerData timerData) {
    String tagString;
    if (useNewTimerTagEncoding(timerData)) {
      tagString =
          prefix.byteString().toStringUtf8()
              + // this never ends with a slash
              timerData.getNamespace().stringKey()
              + // this must begin and end with a slash
              '+'
              + timerData.getTimerId()
              + // this is arbitrary; currently unescaped
              '+'
              + timerData.getTimerFamilyId();
    } else {
      // Timers without timerFamily would have timerFamily would be an empty string
      tagString =
          prefix.byteString().toStringUtf8()
              + // this never ends with a slash
              timerData.getNamespace().stringKey()
              + // this must begin and end with a slash
              '+'
              + timerData.getTimerId() // this is arbitrary; currently unescaped
      ;
    }
    return ByteString.copyFromUtf8(tagString);
  }

  /** {@inheritDoc} */
  @Override
  public TimerData windmillTimerToTimerData(
      WindmillNamespacePrefix prefix,
      Timer timer,
      Coder<? extends BoundedWindow> windowCoder,
      boolean draining) {

    // The tag is a path-structure string but cheaper to parse than a proper URI. It follows
    // this pattern, where no component but the ID can contain a slash
    //
    //     prefix namespace '+' id '+' familyId
    //
    //     prefix ::= '/' prefix_char
    //     namespace ::= '/' | '/' window '/'
    //     id ::= autogenerated_id | arbitrary_string
    //     autogenerated_id ::= timedomain_ordinal ':' millis
    //
    // Notes:
    //
    //  - the slashes and whaatnot in prefix and namespace are owned by that bit of code
    //  - the prefix_char is always ASCII 'u' or 's' for "user" or "system"
    //  - the namespace is generally a base64 encoding of the window passed through its coder, but:
    //    - the GlobalWindow is currently encoded in zero bytes, so it becomes "//"
    //    - the Global StateNamespace is different, and becomes "/"
    //  - the id is totally arbitrary; currently unescaped though that could change

    ByteString tag = timer.getTag();
    checkArgument(
        tag.startsWith(prefix.byteString()),
        "Expected timer tag %s to start with prefix %s",
        tag,
        prefix.byteString());

    Instant timestamp = WindmillTimeUtils.windmillToHarnessTimestamp(timer.getTimestamp());

    // Parse the namespace.
    int namespaceStart = prefix.byteString().size(); // drop the prefix, leave the begin slash
    int namespaceEnd = namespaceStart;
    while (namespaceEnd < tag.size() && tag.byteAt(namespaceEnd) != '+') {
      namespaceEnd++;
    }
    String namespaceString = tag.substring(namespaceStart, namespaceEnd).toStringUtf8();

    // Parse the timer id.
    int timerIdStart = namespaceEnd + 1;
    int timerIdEnd = timerIdStart;
    while (timerIdEnd < tag.size() && tag.byteAt(timerIdEnd) != '+') {
      timerIdEnd++;
    }
    String timerId = tag.substring(timerIdStart, timerIdEnd).toStringUtf8();

    // Parse the timer family.
    int timerFamilyStart = timerIdEnd + 1;
    int timerFamilyEnd = timerFamilyStart;
    while (timerFamilyEnd < tag.size() && tag.byteAt(timerFamilyEnd) != '+') {
      timerFamilyEnd++;
    }
    // For backwards compatibility, handle the case were the timer family isn't present.
    String timerFamily =
        (timerFamilyStart < tag.size())
            ? tag.substring(timerFamilyStart, timerFamilyEnd).toStringUtf8()
            : "";

    // For backwards compatibility, parse the output timestamp from the tag. Not using '+' as a
    // terminator because the
    // output timestamp is the last segment in the tag and the timestamp encoding itself may contain
    // '+'.
    int outputTimestampStart = timerFamilyEnd + 1;
    int outputTimestampEnd = tag.size();

    // For backwards compatibility, handle the case were the output timestamp isn't present.
    Instant outputTimestamp = timestamp;
    if ((outputTimestampStart < tag.size())) {
      try {
        outputTimestamp =
            new Instant(
                VarInt.decodeLong(
                    tag.substring(outputTimestampStart, outputTimestampEnd).newInput()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (timer.hasMetadataTimestamp()) {
      // We use BoundedWindow.TIMESTAMP_MAX_VALUE+1 to indicate "no output timestamp" so make sure
      // to change the upper
      // bound.
      outputTimestamp = WindmillTimeUtils.windmillToHarnessTimestamp(timer.getMetadataTimestamp());
      if (outputTimestamp.equals(OUTPUT_TIMESTAMP_MAX_WINDMILL_VALUE)) {
        outputTimestamp = OUTPUT_TIMESTAMP_MAX_VALUE;
      }
    }

    StateNamespace namespace = StateNamespaces.fromString(namespaceString, windowCoder);
    return TimerData.of(
        timerId,
        timerFamily,
        namespace,
        timestamp,
        outputTimestamp,
        timerTypeToTimeDomain(timer.getType()));
    // todo add draining (https://github.com/apache/beam/issues/36884)

  }

  private static boolean useNewTimerTagEncoding(TimerData timerData) {
    return !timerData.getTimerFamilyId().isEmpty();
  }

  /** @return the singleton WindmillStateTagUtil */
  public static WindmillTagEncodingV1 instance() {
    return INSTANCE;
  }
}
