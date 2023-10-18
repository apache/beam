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
package org.apache.beam.sdk.options;

/** Options that are used to control the Memory Monitor. */
@Description("Options that are used to control the Memory Monitor.")
public interface MemoryMonitorOptions extends PipelineOptions {

  /**
   * The GC thrashing threshold percentage. A given period of time is considered "thrashing" if this
   * percentage of CPU time is spent in garbage collection. Harness will force fail tasks after
   * sustained periods of thrashing.
   *
   * <p>If {@literal 100} is given as the value, MemoryMonitor will be disabled.
   */
  @Description(
      "The GC thrashing threshold percentage. A given period of time is considered \"thrashing\" if this "
          + "percentage of CPU time is spent in garbage collection. Dataflow will force fail tasks after "
          + "sustained periods of thrashing.")
  @Default.Double(50.0)
  Double getGCThrashingPercentagePerPeriod();

  void setGCThrashingPercentagePerPeriod(Double value);
}
