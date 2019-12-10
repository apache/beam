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
package org.apache.beam.runners.fnexecution.environment;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.RemoteEnvironmentOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessEnvironmentFactory}. */
@RunWith(JUnit4.class)
public class ProcessEnvironmentFactoryTest {

  private static final ApiServiceDescriptor SERVICE_DESCRIPTOR =
      ApiServiceDescriptor.newBuilder().setUrl("service-url").build();
  private static final String COMMAND = "my-command";
  private static final Environment ENVIRONMENT =
      Environments.createProcessEnvironment("", "", COMMAND, Collections.emptyMap());

  private static final InspectibleIdGenerator ID_GENERATOR = new InspectibleIdGenerator();

  @Mock private ProcessManager processManager;

  @Mock private GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  @Mock private GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  @Mock private GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  @Mock private GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;

  @Mock private InstructionRequestHandler client;
  private ProcessEnvironmentFactory factory;

  @Before
  public void initMocks() throws IOException {
    MockitoAnnotations.initMocks(this);

    when(processManager.startProcess(anyString(), anyString(), anyList(), anyMap()))
        .thenReturn(Mockito.mock(ProcessManager.RunningProcess.class));
    when(controlServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(loggingServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(retrievalServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(provisioningServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    factory =
        ProcessEnvironmentFactory.create(
            processManager,
            controlServiceServer,
            loggingServiceServer,
            retrievalServiceServer,
            provisioningServiceServer,
            (workerId, timeout) -> client,
            ID_GENERATOR,
            PipelineOptionsFactory.as(RemoteEnvironmentOptions.class));
  }

  @Test
  public void createsCorrectEnvironment() throws Exception {
    RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT);
    assertThat(handle.getInstructionRequestHandler(), is(client));
    assertThat(handle.getEnvironment(), equalTo(ENVIRONMENT));
    Mockito.verify(processManager)
        .startProcess(eq(ID_GENERATOR.currentId), anyString(), anyList(), anyMap());
  }

  @Test
  public void destroysCorrectContainer() throws Exception {
    RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT);
    handle.close();
    verify(processManager).stopProcess(ID_GENERATOR.currentId);
  }

  @Test
  public void createsMultipleEnvironments() throws Exception {
    Environment fooEnv =
        Environments.createProcessEnvironment("", "", "foo", Collections.emptyMap());
    RemoteEnvironment fooHandle = factory.createEnvironment(fooEnv);
    assertThat(fooHandle.getEnvironment(), is(equalTo(fooEnv)));

    Environment barEnv =
        Environments.createProcessEnvironment("", "", "bar", Collections.emptyMap());
    RemoteEnvironment barHandle = factory.createEnvironment(barEnv);
    assertThat(barHandle.getEnvironment(), is(equalTo(barEnv)));
  }

  private static class InspectibleIdGenerator implements IdGenerator {

    private IdGenerator generator = IdGenerators.incrementingLongs();
    String currentId;

    @Override
    public String getId() {
      currentId = generator.getId();
      return currentId;
    }
  }
}
