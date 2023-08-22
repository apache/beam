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
package org.apache.beam.it.testcontainers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.it.common.TestProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.GenericContainer;

/** Unit tests for {@link TestContainerResourceManager}. */
@RunWith(JUnit4.class)
public class TestContainerResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private GenericContainer<?> container;

  private static final String TEST_ID = "test-id";
  private static final String HOST = "1.2.3.4";
  private static final int PORT = 10000;

  private TestContainerResourceManager.Builder<TestContainerResourceManagerImpl> testManagerBuilder;

  @Before
  public void setUp() {
    testManagerBuilder =
        new TestContainerResourceManager.Builder<TestContainerResourceManagerImpl>(
            TEST_ID, null, null) {
          @Override
          public TestContainerResourceManagerImpl build() {
            return new TestContainerResourceManagerImpl(container, this);
          }
        };
  }

  @Test
  public void testCreateResourceManagerSetsCorrectDockerImageName() {
    when(container.getDockerImageName()).thenReturn("container-test:test");

    testManagerBuilder.setContainerImageName("container-test").setContainerImageTag("test").build();

    assertThat(container.getDockerImageName())
        .isEqualTo(
            testManagerBuilder.containerImageName + ":" + testManagerBuilder.containerImageTag);
  }

  @Test
  public void testCreateResourceManagerShouldStartContainerWhenNotUsingStaticResource() {
    testManagerBuilder.build();

    verify(container).start();
  }

  @Test
  public void testCreateResourceManagerShouldNotStartContainerWhenUsingStaticResource() {
    testManagerBuilder.useStaticContainer().setHost(HOST).setPort(PORT).build();

    verify(container, never()).start();
  }

  @Test
  public void
      testCreateResourceManagerShouldThrowErrorWhenUsingStaticResourceWithoutHostOrPortSet() {
    assertThrows(
        TestContainerResourceManagerException.class,
        () -> testManagerBuilder.useStaticContainer().build());
  }

  @Test
  public void testCreateResourceManagerShouldThrowErrorWhenUsingStaticResourceWithoutHostSet() {
    assertThrows(
        TestContainerResourceManagerException.class,
        () -> testManagerBuilder.useStaticContainer().setPort(PORT).build());
  }

  @Test
  public void testCreateResourceManagerShouldThrowErrorWhenUsingStaticResourceWithoutPortSet() {
    assertThrows(
        TestContainerResourceManagerException.class,
        () -> testManagerBuilder.useStaticContainer().setHost(HOST).build());
  }

  @Test
  public void testGetHostShouldReturnCorrectHostWhenManuallySet() {
    TestContainerResourceManager<?> testManager = testManagerBuilder.setHost(HOST).build();

    assertThat(testManager.getHost()).matches(HOST);
  }

  @Test
  public void testGetHostShouldReturnCorrectHostWhenHostNotSet() {
    String host = TestProperties.hostIp();
    TestContainerResourceManager<?> testManager = testManagerBuilder.build();

    assertThat(testManager.getHost()).matches(host);
  }

  @Test
  public void testGetPortShouldReturnCorrectPortWhenManuallySet() {
    TestContainerResourceManager<?> testManager =
        testManagerBuilder.setHost(HOST).setPort(PORT).build();

    assertThat(testManager.getPort(-1)).isEqualTo(PORT);
  }

  @Test
  public void testGetPortShouldReturnContainerHostWhenPortNotSet() {
    int mappedPort = 5000;
    when(container.getMappedPort(anyInt())).thenReturn(mappedPort);

    TestContainerResourceManager<?> testManager = testManagerBuilder.build();

    assertThat(testManager.getPort(PORT)).isEqualTo(mappedPort);
  }

  @Test
  public void testCleanupAllShouldCloseContainerWhenNotUsingStaticResource() {
    TestContainerResourceManager<?> testManager = testManagerBuilder.build();

    testManager.cleanupAll();
    verify(container).close();
  }

  @Test
  public void testCleanupAllShouldReturnFalseWhenContainerFailsToClose() {
    doThrow(RuntimeException.class).when(container).close();

    TestContainerResourceManager<?> testManager = testManagerBuilder.build();

    assertThrows(TestContainerResourceManagerException.class, testManager::cleanupAll);
  }

  @Test
  public void testCleanupAllShouldNotCloseContainerWhenUsingStaticResource() {
    TestContainerResourceManager<?> testManager =
        testManagerBuilder.useStaticContainer().setHost(HOST).setPort(PORT).build();

    testManager.cleanupAll();
    verify(container, never()).close();
  }

  private static class TestContainerResourceManagerImpl
      extends TestContainerResourceManager<GenericContainer<?>> {
    protected TestContainerResourceManagerImpl(
        GenericContainer<?> container, Builder<TestContainerResourceManagerImpl> builder) {
      super(container, builder);
    }
  }
}
