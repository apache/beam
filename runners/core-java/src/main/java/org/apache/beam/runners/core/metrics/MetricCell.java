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
package org.apache.beam.runners.core.metrics;

import java.io.Serializable;

/**
 * A {@link MetricCell} is used for accumulating in-memory changes to a metric. It represents a
 * specific metric name in a single context.
 *
 * @param <DataT> The type of metric data stored (and extracted) from this cell.
 */
public interface MetricCell<DataT> extends Serializable {
  /**
   * Return the {@link DirtyState} tracking whether this metric cell contains uncommitted changes.
   */
  DirtyState getDirty();

  /** Return the cumulative value of this metric. */
  DataT getCumulative();

  /** Reset this metric. */
  void reset();
}
