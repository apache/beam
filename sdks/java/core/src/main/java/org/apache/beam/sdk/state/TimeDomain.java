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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;

/**
 * {@link TimeDomain} specifies whether an operation is based on timestamps of elements or current
 * "real-world" time as reported while processing.
 */
@Experimental(Kind.TIMERS)
public enum TimeDomain {
  /**
   * The {@link #EVENT_TIME} domain corresponds to the timestamps on the elements. Time advances on
   * the system watermark advances.
   */
  EVENT_TIME,

  /**
   * The {@link #PROCESSING_TIME} domain corresponds to the current (system) time. This is advanced
   * during execution of the pipeline.
   */
  PROCESSING_TIME,

  /**
   * <b>For internal use only; no backwards compatibility guarantees.</b>
   *
   * <p>Same as the {@link #PROCESSING_TIME} domain, except it won't fire a timer set for time
   * <i>t</i> until all timers from earlier stages set for a time earlier than <i>t</i> have fired.
   */
  @Internal
  SYNCHRONIZED_PROCESSING_TIME
}
