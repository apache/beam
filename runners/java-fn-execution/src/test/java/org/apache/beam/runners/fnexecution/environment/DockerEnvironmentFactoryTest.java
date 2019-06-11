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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DockerEnvironmentFactory}. */
@RunWith(JUnit4.class)
public class DockerEnvironmentFactoryTest {

  private static final ApiServiceDescriptor SERVICE_DESCRIPTOR =
      ApiServiceDescriptor.newBuilder().setUrl("service-url").build();
  private static final String IMAGE_NAME = "my-image";
  private static final Environment ENVIRONMENT = Environments.createDockerEnvironment(IMAGE_NAME);
  private static final String CONTAINER_ID =
      "e4485f0f2b813b63470feacba5fe9cb89699878c095df4124abd320fd5401385";

  private static final IdGenerator ID_GENERATOR = IdGenerators.incrementingLongs();

  @Mock private DockerCommand docker;

  @Mock private GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  @Mock private GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  @Mock private GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  @Mock private GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;

  @Mock private InstructionRequestHandler client;
  private DockerEnvironmentFactory factory;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);

    when(controlServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(loggingServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(retrievalServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(provisioningServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    factory =
        DockerEnvironmentFactory.forServicesWithDocker(
            docker,
            controlServiceServer,
            loggingServiceServer,
            retrievalServiceServer,
            provisioningServiceServer,
            (workerId, timeout) -> client,
            ID_GENERATOR,
            false,
            null);
  }

  @Test
  public void createsCorrectEnvironment() throws Exception {
    when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
        .thenReturn(CONTAINER_ID);
    when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(true);

    RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT);
    assertThat(handle.getInstructionRequestHandler(), is(client));
    assertThat(handle.getEnvironment(), equalTo(ENVIRONMENT));
  }

  @Test
  public void destroysCorrectContainer() throws Exception {
    when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
        .thenReturn(CONTAINER_ID);
    when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(true);

    RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT);
    handle.close();
    verify(docker).killContainer(CONTAINER_ID);
  }

  @Test
  public void createsMultipleEnvironments() throws Exception {
    when(docker.isContainerRunning(anyString())).thenReturn(true);
    Environment fooEnv = Environments.createDockerEnvironment("foo");
    RemoteEnvironment fooHandle = factory.createEnvironment(fooEnv);
    assertThat(fooHandle.getEnvironment(), is(equalTo(fooEnv)));

    Environment barEnv = Environments.createDockerEnvironment("bar");
    RemoteEnvironment barHandle = factory.createEnvironment(barEnv);
    assertThat(barHandle.getEnvironment(), is(equalTo(barEnv)));
  }
}
