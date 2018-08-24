/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.examples.timeseries.transforms;

import java.util.Date;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/**
 * In order to Backfill any key at least one value from that key needs to be observed by the system.
 * This class will take the TSConfiguration.firstTime value and apply that as the timestamp to use
 * for the first time this key will be added.
 */
public class BootstrapKeys extends DoFn<TimeSeriesData.TSDataPoint, TimeSeriesData.TSDataPoint> {

  private long startTime;

  public BootstrapKeys(long startTime) {
    this.startTime = startTime;
  }

  @DoFn.ProcessElement
  public void process(DoFn.ProcessContext c) {
    c.outputWithTimestamp(c, new Instant(new Date(startTime).getTime()));
  }
}
