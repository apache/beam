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

package org.apache.beam.runners.core.construction;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.transforms.display.DisplayData;

/** Utilities for going to/from DisplayData protos. */
public class DisplayDataTranslation {
  public static RunnerApi.DisplayData toProto(DisplayData displayData) {
    // TODO https://issues.apache.org/jira/browse/BEAM-2645
    return RunnerApi.DisplayData.newBuilder()
        .addItems(
            RunnerApi.DisplayData.Item.newBuilder()
                .setId(RunnerApi.DisplayData.Identifier.newBuilder().setKey("stubImplementation"))
                .setLabel("Stub implementation")
                .setType(RunnerApi.DisplayData.Type.BOOLEAN)
                .setValue(Any.pack(BoolValue.newBuilder().setValue(true).build())))
        .build();
  }
}
