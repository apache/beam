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

/**
 * The results of a single current metric.
 */
@Experimental(Kind.METRICS)
public interface MetricResult<T> {
  /** Return the name of the metric. */
  MetricName getName();
  /** Return the step context to which this metric result applies. */
  String getStep();

  /**
   * Return the value of this metric across all successfully completed parts of the pipeline.
   *
   * <p>Not all runners will support committed metrics. If they are not supported, the runner will
   * throw an {@link UnsupportedOperationException}.
   */
  T getCommitted();

  /**
   * Return the value of this metric across all attempts of executing all parts of the pipeline.
   */
  T getAttempted();
}
