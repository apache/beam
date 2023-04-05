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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import com.google.auto.value.AutoValue;

/** This is a utility class to represent rowCount, rate and window. */
@AutoValue
public abstract class NodeStats {

  /**
   * Returns an instance with all values set to INFINITY. This will be only used when the node is
   * not a BeamRelNode and we don't have an estimation implementation for it in the metadata
   * handler. In this case we return INFINITE and it will be propagated up in the estimates.
   */
  public static final NodeStats UNKNOWN =
      create(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

  public abstract double getRowCount();

  public abstract double getRate();

  /**
   * This method returns the number of tuples in each window. It is different than the windowing
   * notion of Beam.
   */
  public abstract double getWindow();

  public static NodeStats create(double rowCount, double rate, double window) {
    if (window < 0 || rate < 0 || rowCount < 0) {
      throw new IllegalArgumentException("All the estimates in NodeStats should be positive");
    }
    return new AutoValue_NodeStats(rowCount, rate, window);
  }

  /** It creates an instance with rate=0 and window=rowCount for bounded sources. */
  public static NodeStats create(double rowCount) {
    return create(rowCount, 0d, rowCount);
  }

  /** If any of the values for rowCount, rate or window is infinite, it returns true. */
  public boolean isUnknown() {
    return Double.isInfinite(getRowCount())
        || Double.isInfinite(getRate())
        || Double.isInfinite(getWindow());
  }

  public NodeStats multiply(double factor) {
    return create(getRowCount() * factor, getRate() * factor, getWindow() * factor);
  }

  public NodeStats plus(NodeStats that) {
    if (this.isUnknown() || that.isUnknown()) {
      return UNKNOWN;
    }
    return create(
        this.getRowCount() + that.getRowCount(),
        this.getRate() + that.getRate(),
        this.getWindow() + that.getWindow());
  }

  public NodeStats minus(NodeStats that) {
    if (this.isUnknown() || that.isUnknown()) {
      return UNKNOWN;
    }
    return create(
        Math.max(this.getRowCount() - that.getRowCount(), 0),
        Math.max(this.getRate() - that.getRate(), 0),
        Math.max(this.getWindow() - that.getWindow(), 0));
  }
}
