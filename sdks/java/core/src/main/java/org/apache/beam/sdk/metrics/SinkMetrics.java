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
package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** Standard Sink Metrics. */
@Experimental(Kind.METRICS)
public class SinkMetrics {

  private static final String SINK_NAMESPACE = "sink";

  private static final String ELEMENTS_WRITTEN = "elements_written";
  private static final String BYTES_WRITTEN = "bytes_written";

  private static final Counter ELEMENTS_WRITTEN_COUNTER =
      Metrics.counter(SINK_NAMESPACE, ELEMENTS_WRITTEN);
  private static final Counter BYTES_WRITTEN_COUNTER =
      Metrics.counter(SINK_NAMESPACE, BYTES_WRITTEN);

  /** Counter of elements written to a sink. */
  public static Counter elementsWritten() {
    return ELEMENTS_WRITTEN_COUNTER;
  }

  /** Counter of bytes written to a sink. */
  public static Counter bytesWritten() {
    return BYTES_WRITTEN_COUNTER;
  }
}
