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

/** Standard collection of metrics used to record source and sinks information for
 * lineage tracking.*/
public class Lineage {

  private static final String LINEAGE_NAMESPACE = "lineage";
  private static final String SOURCE_METRIC_NAME = "sources";
  private static final String SINK_METRIC_NAME = "sinks";

  private static final StringSet SOURCES =
      Metrics.stringSet(LINEAGE_NAMESPACE, SOURCE_METRIC_NAME);
  private static final StringSet SINKS =
      Metrics.stringSet(LINEAGE_NAMESPACE, SINK_METRIC_NAME);

  /** {@link StringSet} representing sources and optionally side inputs. */
  public static StringSet getSources() {
    return SOURCES;
  }

  /** {@link StringSet} representing sinks. */
  public static StringSet getSinks() {
    return SINKS;
  }
}
