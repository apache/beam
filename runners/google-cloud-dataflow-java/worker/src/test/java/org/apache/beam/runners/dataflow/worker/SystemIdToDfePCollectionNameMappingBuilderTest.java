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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.WorkItem;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SystemIdToDfePCollectionNameMappingBuilderTest}. */
@RunWith(JUnit4.class)
public class SystemIdToDfePCollectionNameMappingBuilderTest {

  @Test
  public void testBuildsValidMapping() {
    WorkItem source = new WorkItem();
    MapTask mt = new MapTask();
    ParallelInstruction instructionWithNoOutputs = new ParallelInstruction();
    ParallelInstruction instructionWithSingleOutput = new ParallelInstruction();
    instructionWithSingleOutput.setOriginalName("i1");
    instructionWithSingleOutput.setOutputs(
        ImmutableList.of(
            new InstructionOutput().setName("SingleOut").setSystemName("i1.SystemSingleOut")));
    ParallelInstruction instructionWithTwoOutputs = new ParallelInstruction();
    instructionWithTwoOutputs.setOriginalName("i22");
    instructionWithTwoOutputs.setOutputs(
        ImmutableList.of(
            new InstructionOutput().setName("TwoOuts1").setSystemName("i22.SystemTwoOuts1"),
            new InstructionOutput().setName("TwoOuts2").setSystemName("i22.SystemTwoOuts2")));
    List<ParallelInstruction> instructionList =
        ImmutableList.of(
            instructionWithNoOutputs, instructionWithSingleOutput, instructionWithTwoOutputs);
    mt.setInstructions(instructionList);
    source.setMapTask(mt);

    Map<String, String> result = new SystemIdToDfePCollectionNameMappingBuilder().build(source);

    Map<String, String> expected =
        ImmutableMap.of(
            "SystemSingleOut",
            "SingleOut",
            "SystemTwoOuts1",
            "TwoOuts1",
            "SystemTwoOuts2",
            "TwoOuts2");
    assertEquals(expected, result);
  }
}
