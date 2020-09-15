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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.dataflow.worker.SdkHarnessRegistry.SdkWorkerHarness;
import org.apache.beam.runners.dataflow.worker.fn.data.BeamFnDataGrpcService;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SdkHarnessRegistry}. */
@RunWith(JUnit4.class)
public class SdkHarnessRegistryTest {

  @Test
  public void testEmptyWorkerRegistration() throws Exception {
    SdkHarnessRegistry sdkHarnessRegistry = SdkHarnessRegistries.emptySdkHarnessRegistry();
    SdkWorkerHarness worker = sdkHarnessRegistry.getAvailableWorkerAndAssignWork();
    assertNull("WorkerId should be null for null worker", worker.getWorkerId());
    assertNull(
        "DataApiServiceDescriptor should be null",
        sdkHarnessRegistry.beamFnDataApiServiceDescriptor());
    assertNull(
        "StateApiServiceDescriptor should be null",
        sdkHarnessRegistry.beamFnStateApiServiceDescriptor());
  }

  @Test
  public void testWorkerRegistrationAndOrdering() throws Exception {
    SdkHarnessRegistry sdkHarnessRegistry =
        SdkHarnessRegistries.createFnApiSdkHarnessRegistry(
            ApiServiceDescriptor.getDefaultInstance(),
            Mockito.mock(GrpcStateService.class),
            Mockito.mock(BeamFnDataGrpcService.class));
    Set<String> workerIds = new HashSet<>();
    IntStream.range(0, 3)
        .forEach(
            i -> {
              String workerId = "worker_" + i;
              sdkHarnessRegistry.registerWorkerClient(
                  FnApiControlClient.forRequestObserver(workerId, null, null));
              workerIds.add(workerId);
            });
    assertEquals(
        "All workers should be provided",
        workerIds,
        IntStream.range(0, 3)
            .mapToObj(i -> sdkHarnessRegistry.getAvailableWorkerAndAssignWork().getWorkerId())
            .collect(Collectors.toSet()));

    IntStream.range(3, 5)
        .forEach(
            i -> {
              String workerId = "worker_" + i;
              sdkHarnessRegistry.registerWorkerClient(
                  FnApiControlClient.forRequestObserver(workerId, null, null));
              workerIds.add(workerId);
            });

    assertEquals(
        "All workers should be provided",
        new HashSet<>(Arrays.asList("worker_3", "worker_4")),
        IntStream.range(3, 5)
            .mapToObj(i -> sdkHarnessRegistry.getAvailableWorkerAndAssignWork().getWorkerId())
            .collect(Collectors.toSet()));

    Set<String> workersNotUsed = new HashSet<>(workerIds);
    workersNotUsed.removeAll(
        IntStream.range(0, 3)
            .mapToObj(i -> sdkHarnessRegistry.getAvailableWorkerAndAssignWork().getWorkerId())
            .collect(Collectors.toSet()));

    assertEquals(
        "All workers should be provided",
        workersNotUsed,
        IntStream.range(3, 5)
            .mapToObj(i -> sdkHarnessRegistry.getAvailableWorkerAndAssignWork().getWorkerId())
            .collect(Collectors.toSet()));

    List<SdkWorkerHarness> workers =
        IntStream.range(0, 5)
            .mapToObj(i -> sdkHarnessRegistry.getAvailableWorkerAndAssignWork())
            .collect(Collectors.toList());

    assertEquals(5, workers.size());

    sdkHarnessRegistry.completeWork(workers.get(0));

    assertEquals(
        "There should only be 1 free worker",
        workers.get(0),
        sdkHarnessRegistry.getAvailableWorkerAndAssignWork());
  }
}
