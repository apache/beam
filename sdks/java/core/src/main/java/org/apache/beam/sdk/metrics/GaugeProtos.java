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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.IntGaugeData;
import org.joda.time.Instant;

/** Convert gauges between protobuf and SDK representations. */
public class GaugeProtos {
  public static GaugeResult fromProto(IntGaugeData gaugeData) {
    return GaugeResult.create(gaugeData.getValue(), new Instant(gaugeData.getTimestampMs()));
  }

  public static IntGaugeData toProto(GaugeResult gauge) {
    return IntGaugeData.newBuilder()
        .setValue(gauge.getValue())
        .setTimestampMs(gauge.getTimestamp().getMillis())
        .build();
  }
}
