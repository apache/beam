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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.ManualDockerEnvironmentOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.RemoteEnvironmentOptions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DockerEnvironmentFactory}. */
public class DockerEnvironmentFactoryTest {

  private static final ApiServiceDescriptor SERVICE_DESCRIPTOR =
      ApiServiceDescriptor.newBuilder().setUrl("service-url").build();
  private static final String IMAGE_NAME = "my-image";
  private static final Environment ENVIRONMENT = Environments.createDockerEnvironment(IMAGE_NAME);
  private static final String CONTAINER_ID =
      "e4485f0f2b813b63470feacba5fe9cb89699878c095df4124abd320fd5401385";

  private static final IdGenerator ID_GENERATOR = IdGenerators.incrementingLongs();

  @Mock DockerCommand docker;

  @Mock GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  @Mock GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  @Mock GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  @Mock GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;

  @Mock InstructionRequestHandler client;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);

    when(controlServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(loggingServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(retrievalServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
    when(provisioningServiceServer.getApiServiceDescriptor()).thenReturn(SERVICE_DESCRIPTOR);
  }

  @RunWith(Parameterized.class)
  public static class ParameterizedTest extends DockerEnvironmentFactoryTest {
    @Parameter(0)
    public boolean throwsException;

    @Parameter(1)
    public boolean retainDockerContainer;

    @Parameter(2)
    public int removeContainerTimes;

    @Rule public ExpectedException expectedException = ExpectedException.none();

    private final ControlClientPool.Source normalClientSource = (workerId, timeout) -> client;
    private final ControlClientPool.Source exceptionClientSource =
        (workerId, timeout) -> {
          throw new Exception();
        };

    @Parameterized.Parameters(
        name =
            "{index}: Test with throwsException={0}, retainDockerContainer={1} should remove container {2} time(s)")
    public static Collection<Object[]> data() {
      Object[][] data =
          new Object[][] {{false, false, 1}, {false, true, 0}, {true, false, 1}, {true, true, 0}};
      return Arrays.asList(data);
    }

    @Test
    public void cleansUpContainerCorrectly() throws Exception {
      when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
          .thenReturn(CONTAINER_ID);
      when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(true);
      ManualDockerEnvironmentOptions pipelineOptions =
          PipelineOptionsFactory.as(ManualDockerEnvironmentOptions.class);
      pipelineOptions.setRetainDockerContainers(retainDockerContainer);
      DockerEnvironmentFactory factory =
          DockerEnvironmentFactory.forServicesWithDocker(
              docker,
              provisioningServiceServer,
              throwsException ? exceptionClientSource : normalClientSource,
              ID_GENERATOR,
              pipelineOptions);
      if (throwsException) {
        expectedException.expect(Exception.class);
      }

      RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT, "workerId");

      ArgumentCaptor<List<String>> dockerArgsCaptor = ArgumentCaptor.forClass(List.class);
      verify(docker).runImage(any(), dockerArgsCaptor.capture(), anyList());

      // Ensure we do not remove the container prematurely which would also remove the logs
      assertThat(dockerArgsCaptor.getValue(), not(hasItem("--rm")));

      handle.close();

      verify(docker).killContainer(CONTAINER_ID);
      verify(docker, times(removeContainerTimes)).removeContainer(CONTAINER_ID);
    }
  }

  public static class NonParameterizedTest extends DockerEnvironmentFactoryTest {
    @Test
    public void createsCorrectEnvironment() throws Exception {
      when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
          .thenReturn(CONTAINER_ID);
      when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(true);
      DockerEnvironmentFactory factory = getFactory((workerId, timeout) -> client);

      RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT, "workerId");

      assertThat(handle.getInstructionRequestHandler(), is(client));
      assertThat(handle.getEnvironment(), equalTo(ENVIRONMENT));
    }

    @Test(expected = RuntimeException.class)
    public void logsDockerOutputOnStartupFailed() throws Exception {
      when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
          .thenReturn(CONTAINER_ID);
      when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(false);
      DockerEnvironmentFactory factory = getFactory((workerId, timeout) -> client);

      factory.createEnvironment(ENVIRONMENT, "workerId");

      verify(docker).getContainerLogs(CONTAINER_ID);
    }

    @Test
    public void logsDockerOutputOnClose() throws Exception {
      when(docker.runImage(Mockito.eq(IMAGE_NAME), Mockito.any(), Mockito.any()))
          .thenReturn(CONTAINER_ID);
      when(docker.isContainerRunning(Mockito.eq(CONTAINER_ID))).thenReturn(true);
      DockerEnvironmentFactory factory = getFactory((workerId, timeout) -> client);

      RemoteEnvironment handle = factory.createEnvironment(ENVIRONMENT, "workerId");
      handle.close();

      verify(docker).getContainerLogs(CONTAINER_ID);
    }

    @Test
    public void createsMultipleEnvironments() throws Exception {
      when(docker.isContainerRunning(any())).thenReturn(true);
      DockerEnvironmentFactory factory = getFactory((workerId, timeout) -> client);

      Environment fooEnv = Environments.createDockerEnvironment("foo");
      RemoteEnvironment fooHandle = factory.createEnvironment(fooEnv, "workerId");
      assertThat(fooHandle.getEnvironment(), is(equalTo(fooEnv)));

      Environment barEnv = Environments.createDockerEnvironment("bar");
      RemoteEnvironment barHandle = factory.createEnvironment(barEnv, "workerId");
      assertThat(barHandle.getEnvironment(), is(equalTo(barEnv)));
    }

    private DockerEnvironmentFactory getFactory(ControlClientPool.Source clientSource) {
      return DockerEnvironmentFactory.forServicesWithDocker(
          docker,
          provisioningServiceServer,
          clientSource,
          ID_GENERATOR,
          PipelineOptionsFactory.as(RemoteEnvironmentOptions.class));
    }
  }
}
