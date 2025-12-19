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

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Internal
@ThreadSafe
/*
 * Windmill StateTag, TimerTag encoding interface
 */
public abstract class WindmillTagEncoding {

  protected static final Instant OUTPUT_TIMESTAMP_MAX_WINDMILL_VALUE =
      GlobalWindow.INSTANCE.maxTimestamp().plus(Duration.millis(1));

  protected static final Instant OUTPUT_TIMESTAMP_MAX_VALUE =
      BoundedWindow.TIMESTAMP_MAX_VALUE.plus(Duration.millis(1));

  /** Encodes state tag */
  public abstract InternedByteString stateTag(StateNamespace namespace, StateTag<?> address);

  /**
   * Produce a state tag that is guaranteed to be unique for the given timer, to add a watermark
   * hold that is only freed after the timer fires.
   */
  public abstract ByteString timerHoldTag(
      WindmillNamespacePrefix prefix, TimerData timerData, ByteString timerTag);

  /**
   * Produce a tag that is guaranteed to be unique for the given prefix, namespace, domain and
   * timestamp.
   *
   * <p>This is necessary because Windmill will deduplicate based only on this tag.
   */
  public abstract ByteString timerTag(WindmillNamespacePrefix prefix, TimerData timerData);

  /** Converts Windmill Timer to beam TimerData */
  public abstract TimerData windmillTimerToTimerData(
      WindmillNamespacePrefix prefix,
      Timer timer,
      Coder<? extends BoundedWindow> windowCoder,
      boolean draining);

  /**
   * Uses the given {@link Timer} builder to build a windmill {@link Timer} from {@link TimerData}.
   *
   * @return the input builder for chaining
   */
  public Timer.Builder buildWindmillTimerFromTimerData(
      @Nullable String stateFamily,
      WindmillNamespacePrefix prefix,
      TimerData timerData,
      Timer.Builder builder) {

    builder.setTag(timerTag(prefix, timerData)).setType(timerType(timerData.getDomain()));

    if (stateFamily != null) {
      builder.setStateFamily(stateFamily);
    }

    builder.setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timerData.getTimestamp()));

    // Store the output timestamp in the metadata timestamp.
    Instant outputTimestamp = timerData.getOutputTimestamp();
    if (outputTimestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // We can't encode any value larger than BoundedWindow.TIMESTAMP_MAX_VALUE, so use the end of
      // the global window
      // here instead.
      outputTimestamp = OUTPUT_TIMESTAMP_MAX_WINDMILL_VALUE;
    }
    builder.setMetadataTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(outputTimestamp));
    return builder;
  }

  protected static Timer.Type timerType(TimeDomain domain) {
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

  protected static TimeDomain timerTypeToTimeDomain(Timer.Type type) {
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
