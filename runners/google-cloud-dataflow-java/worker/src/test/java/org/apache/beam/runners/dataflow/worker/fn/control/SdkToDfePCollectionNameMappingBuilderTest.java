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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class SdkToDfePCollectionNameMappingBuilderTest {

  @Test
  public void testBuildsProperMapping() {
    List<ProcessBundleDescriptor> descriptors =
        ImmutableList.of(
            ProcessBundleDescriptor.newBuilder()
                .putTransforms("first", PTransform.newBuilder().build())
                .putTransforms(
                    "second",
                    PTransform.newBuilder()
                        .putOutputs("firstSystem", "firstSDK")
                        .putOutputs("secondSystem", "secondSDK")
                        .build())
                .build());

    Map<String, String> systemMapping =
        ImmutableMap.of("firstSystem", "firstDFE", "secondSystem", "secondDFE");
    Map<String, String> result =
        new SdkToDfePCollectionNameMappingBuilder().build(descriptors, systemMapping);

    Map<String, String> expected =
        ImmutableMap.of("firstSDK", "firstDFE", "secondSDK", "secondDFE");
    assertTrue(expected.equals(result));
  }
}
