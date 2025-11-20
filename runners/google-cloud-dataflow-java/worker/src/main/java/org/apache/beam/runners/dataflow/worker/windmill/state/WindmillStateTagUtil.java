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

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream;
import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream.StreamHandle;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

@Internal
@ThreadSafe
public class WindmillStateTagUtil {

  private static final String TIMER_HOLD_PREFIX = "/h";
  private static final WindmillStateTagUtil INSTANCE = new WindmillStateTagUtil();

  // Private constructor to prevent instantiations from outside.
  private WindmillStateTagUtil() {}

  /**
   * Encodes the given namespace and address as {@code &lt;namespace&gt;+&lt;address&gt;}. The
   * returned InternedByteStrings are weakly interned to reduce memory usage and reduce GC pressure.
   */
  @VisibleForTesting
  InternedByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
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

  /**
   * Produce a state tag that is guaranteed to be unique for the given timer, to add a watermark
   * hold that is only freed after the timer fires.
   */
  public ByteString timerHoldTag(WindmillNamespacePrefix prefix, TimerData timerData) {
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

  /** @return the singleton WindmillStateTagUtil */
  public static WindmillStateTagUtil instance() {
    return INSTANCE;
  }
}
