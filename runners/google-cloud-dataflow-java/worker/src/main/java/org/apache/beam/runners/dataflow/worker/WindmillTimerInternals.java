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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ExposedByteArrayInputStream;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table.Cell;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Windmill {@link TimerInternals}.
 *
 * <p>Includes parsing / assembly of timer tags and some extra methods.
 */
class WindmillTimerInternals implements TimerInternals {

  private static final String TIMER_HOLD_PREFIX = "/h";
  // Map from timer id to its TimerData. If it is to be deleted, we still need
  // its time domain here. Note that TimerData is unique per ID and namespace,
  // though technically in Windmill this is only enforced per ID and namespace
  // and TimeDomain. This TimerInternals is scoped to a step and key, shared
  // across namespaces.
  private Table<String, StateNamespace, TimerData> timers = HashBasedTable.create();

  // Map from timer id to whether it is to be deleted or set
  private Table<String, StateNamespace, Boolean> timerStillPresent = HashBasedTable.create();

  private Instant inputDataWatermark;
  private Instant processingTime;
  private @Nullable Instant outputDataWatermark;
  private @Nullable Instant synchronizedProcessingTime;
  private String stateFamily;
  private WindmillNamespacePrefix prefix;

  public WindmillTimerInternals(
      String stateFamily, // unique identifies a step
      WindmillNamespacePrefix prefix, // partitions user and system namespaces into "/u" and "/s"
      Instant inputDataWatermark,
      Instant processingTime,
      @Nullable Instant outputDataWatermark,
      @Nullable Instant synchronizedProcessingTime) {
    this.inputDataWatermark = checkNotNull(inputDataWatermark);
    this.processingTime = checkNotNull(processingTime);
    this.outputDataWatermark = outputDataWatermark;
    this.synchronizedProcessingTime = synchronizedProcessingTime;
    this.stateFamily = stateFamily;
    this.prefix = prefix;
  }

  public WindmillTimerInternals withPrefix(WindmillNamespacePrefix prefix) {
    return new WindmillTimerInternals(
        stateFamily,
        prefix,
        inputDataWatermark,
        processingTime,
        outputDataWatermark,
        synchronizedProcessingTime);
  }

  @Override
  public void setTimer(TimerData timerKey) {
    timers.put(
        getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId()),
        timerKey.getNamespace(),
        timerKey);
    timerStillPresent.put(
        getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId()),
        timerKey.getNamespace(),
        true);
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    timers.put(
        getTimerDataKey(timerId, timerFamilyId),
        namespace,
        TimerData.of(timerId, timerFamilyId, namespace, timestamp, outputTimestamp, timeDomain));
    timerStillPresent.put(getTimerDataKey(timerId, timerFamilyId), namespace, true);
  }

  private String getTimerDataKey(String timerId, String timerFamilyId) {
    // Identifies timer uniquely with timerFamilyId
    return timerId + '+' + timerFamilyId;
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    timers.put(
        getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId()),
        timerKey.getNamespace(),
        timerKey);
    timerStillPresent.put(
        getTimerDataKey(timerKey.getTimerId(), timerKey.getTimerFamilyId()),
        timerKey.getNamespace(),
        false);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException("Canceling a timer by ID is not yet supported.");
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Deletion of timers by ID is not supported.");
  }

  @Override
  public Instant currentProcessingTime() {
    Instant now = Instant.now();
    return processingTime.isAfter(now) ? processingTime : now;
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that this value may be arbitrarily behind the global input watermark. Windmill simply
   * reports the last known input watermark value at the time the GetWork response was constructed.
   * However, if an element in a GetWork request has a timestamp at or ahead of the local input
   * watermark then Windmill will not allow the local input watermark to advance until that element
   * has been committed.
   */
  @Override
  public Instant currentInputWatermarkTime() {
    return inputDataWatermark;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that Windmill will provisionally hold the output watermark to the timestamp of the
   * earliest element in a computation's GetWork response. (Elements with timestamps already behind
   * the output watermark at the point the GetWork response is constructed will have no influence on
   * the output watermark). The provisional hold will last until this work item is committed. It is
   * the responsibility of the harness to impose any persistent holds it needs.
   */
  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return outputDataWatermark;
  }

  public void persistTo(Windmill.WorkItemCommitRequest.Builder outputBuilder) {
    for (Cell<String, StateNamespace, Boolean> cell : timerStillPresent.cellSet()) {
      // Regardless of whether it is set or not, it must have some TimerData stored so we
      // can know its time domain
      TimerData timerData = timers.get(cell.getRowKey(), cell.getColumnKey());

      Timer.Builder timer =
          buildWindmillTimerFromTimerData(
              stateFamily, prefix, timerData, outputBuilder.addOutputTimersBuilder());

      if (cell.getValue()) {
        // Setting the timer. If it is a user timer, set a hold.

        if (needsWatermarkHold(timerData)) {
          // Setting a timer, clear any prior hold and set to the new value
          outputBuilder
              .addWatermarkHoldsBuilder()
              .setTag(timerHoldTag(prefix, timerData))
              .setStateFamily(stateFamily)
              .setReset(true)
              .addTimestamps(
                  WindmillTimeUtils.harnessToWindmillTimestamp(timerData.getOutputTimestamp()));
        }
      } else {
        // Deleting a timer. If it is a user timer, clear the hold
        timer.clearTimestamp();
        if (needsWatermarkHold(timerData)) {
          // We are deleting timer; clear the hold
          outputBuilder
              .addWatermarkHoldsBuilder()
              .setTag(timerHoldTag(prefix, timerData))
              .setStateFamily(stateFamily)
              .setReset(true);
        }
      }
    }

    // Wipe the unpersisted state
    timers.clear();
  }

  public boolean hasTimerBefore(Instant time) {
    for (Cell<String, StateNamespace, Boolean> cell : timerStillPresent.cellSet()) {
      TimerData timerData = timers.get(cell.getRowKey(), cell.getColumnKey());
      if (cell.getValue()) {
        if (timerData.getTimestamp().isBefore(time)) {
          return true;
        }
      }
    }
    return false;
  }

  public List<TimerData> getCurrentTimers() {
    List<TimerData> timerDataList = new ArrayList<>();
    for (Cell<String, StateNamespace, Boolean> cell : timerStillPresent.cellSet()) {
      TimerData timerData = timers.get(cell.getRowKey(), cell.getColumnKey());
      if (cell.getValue()) {
        timerDataList.add(timerData);
      }
    }
    return timerDataList;
  }

  private boolean needsWatermarkHold(TimerData timerData) {
    // If it is a user timer or a system timer with outputTimestamp different than timestamp
    return WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.equals(prefix)
        || !timerData.getTimestamp().isEqual(timerData.getOutputTimestamp());
  }

  public static boolean isSystemTimer(Windmill.Timer timer) {
    return timer.getTag().startsWith(WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX.byteString());
  }

  public static boolean isUserTimer(Windmill.Timer timer) {
    return timer.getTag().startsWith(WindmillNamespacePrefix.USER_NAMESPACE_PREFIX.byteString());
  }

  /**
   * Uses the given {@link Timer} builder to build a windmill {@link Timer} from {@link TimerData}.
   *
   * @return the input builder for chaining
   */
  static Timer.Builder buildWindmillTimerFromTimerData(
      @Nullable String stateFamily,
      WindmillNamespacePrefix prefix,
      TimerData timerData,
      Timer.Builder builder) {

    builder.setTag(timerTag(prefix, timerData)).setType(timerType(timerData.getDomain()));

    if (stateFamily != null) {
      builder.setStateFamily(stateFamily);
    }

    builder.setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timerData.getTimestamp()));

    return builder;
  }

  static Timer timerDataToWindmillTimer(
      @Nullable String stateFamily, WindmillNamespacePrefix prefix, TimerData timerData) {
    return buildWindmillTimerFromTimerData(stateFamily, prefix, timerData, Timer.newBuilder())
        .build();
  }

  public static TimerData windmillTimerToTimerData(
      WindmillNamespacePrefix prefix, Timer timer, Coder<? extends BoundedWindow> windowCoder) {

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

    // Parse the output timestamp.
    int outputTimestampStart = timerFamilyEnd + 1;
    int outputTimestampEnd = outputTimestampStart;
    while (outputTimestampEnd < tag.size() && tag.byteAt(outputTimestampEnd) != '+') {
      outputTimestampEnd++;
    }

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
    }

    StateNamespace namespace = StateNamespaces.fromString(namespaceString, windowCoder);
    return TimerData.of(
        timerId,
        timerFamily,
        namespace,
        timestamp,
        outputTimestamp,
        timerTypeToTimeDomain(timer.getType()));
  }

  private static boolean useNewTimerTagEncoding(TimerData timerData) {
    return !timerData.getTimerFamilyId().isEmpty()
        || !timerData.getOutputTimestamp().equals(timerData.getTimestamp());
  }

  /**
   * Produce a tag that is guaranteed to be unique for the given prefix, namespace, domain and
   * timestamp.
   *
   * <p>This is necessary because Windmill will deduplicate based only on this tag.
   */
  public static ByteString timerTag(WindmillNamespacePrefix prefix, TimerData timerData) {
    String tagString;
    ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
    try {
      if (useNewTimerTagEncoding(timerData)) {
        tagString =
            new StringBuilder()
                .append(prefix.byteString().toStringUtf8()) // this never ends with a slash
                .append(
                    timerData.getNamespace().stringKey()) // this must begin and end with a slash
                .append('+')
                .append(timerData.getTimerId()) // this is arbitrary; currently unescaped
                .append('+')
                .append(timerData.getTimerFamilyId())
                .toString();
        out.write(tagString.getBytes(StandardCharsets.UTF_8));
        // Only encode the extra 9 bytes if the output timestamp is different than the timestamp;
        if (!timerData.getOutputTimestamp().equals(timerData.getTimestamp())) {
          out.write('+');
          VarInt.encode(timerData.getOutputTimestamp().getMillis(), out);
        }
      } else {
        // Timers without timerFamily would have timerFamily would be an empty string
        tagString =
            new StringBuilder()
                .append(prefix.byteString().toStringUtf8()) // this never ends with a slash
                .append(
                    timerData.getNamespace().stringKey()) // this must begin and end with a slash
                .append('+')
                .append(timerData.getTimerId()) // this is arbitrary; currently unescaped
                .toString();
        out.write(tagString.getBytes(StandardCharsets.UTF_8));
      }
      return ByteString.readFrom(new ExposedByteArrayInputStream(out.toByteArray()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Produce a state tag that is guaranteed to be unique for the given timer, to add a watermark
   * hold that is only freed after the timer fires.
   */
  public static ByteString timerHoldTag(WindmillNamespacePrefix prefix, TimerData timerData) {
    String tagString;
    if ("".equals(timerData.getTimerFamilyId())) {
      tagString =
          new StringBuilder()
              .append(prefix.byteString().toStringUtf8()) // this never ends with a slash
              .append(TIMER_HOLD_PREFIX) // this never ends with a slash
              .append(timerData.getNamespace().stringKey()) // this must begin and end with a slash
              .append('+')
              .append(timerData.getTimerId()) // this is arbitrary; currently unescaped
              .toString();
    } else {
      tagString =
          new StringBuilder()
              .append(prefix.byteString().toStringUtf8()) // this never ends with a slash
              .append(TIMER_HOLD_PREFIX) // this never ends with a slash
              .append(timerData.getNamespace().stringKey()) // this must begin and end with a slash
              .append('+')
              .append(timerData.getTimerId()) // this is arbitrary; currently unescaped
              .append('+')
              .append(
                  timerData.getTimerFamilyId()) // use to differentiate same timerId in different
              // timerMap
              .toString();
    }
    return ByteString.copyFromUtf8(tagString);
  }

  @VisibleForTesting
  static Timer.Type timerType(TimeDomain domain) {
    switch (domain) {
      case EVENT_TIME:
        return Timer.Type.WATERMARK;
      case PROCESSING_TIME:
        return Timer.Type.REALTIME;
      case SYNCHRONIZED_PROCESSING_TIME:
        return Timer.Type.DEPENDENT_REALTIME;
      default:
        throw new IllegalArgumentException("Unrecgonized TimeDomain: " + domain);
    }
  }

  @VisibleForTesting
  static TimeDomain timerTypeToTimeDomain(Windmill.Timer.Type type) {
    switch (type) {
      case REALTIME:
        return TimeDomain.PROCESSING_TIME;
      case DEPENDENT_REALTIME:
        return TimeDomain.SYNCHRONIZED_PROCESSING_TIME;
      case WATERMARK:
        return TimeDomain.EVENT_TIME;
      default:
        throw new IllegalArgumentException("Unsupported timer type " + type);
    }
  }
}
