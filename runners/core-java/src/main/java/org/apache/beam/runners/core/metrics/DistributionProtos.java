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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.IntDistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;

/** Convert distributions between protobuf and SDK representations. */
public class DistributionProtos {
  public static DistributionResult fromProto(IntDistributionData distributionData) {
    return DistributionResult.create(
        distributionData.getSum(),
        distributionData.getCount(),
        distributionData.getMin(),
        distributionData.getMax());
  }

  public static IntDistributionData toProto(DistributionResult distributionResult) {
    return IntDistributionData.newBuilder()
        .setMin(distributionResult.getMin())
        .setMax(distributionResult.getMax())
        .setCount(distributionResult.getCount())
        .setSum(distributionResult.getSum())
        .build();
  }
}
