/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.options.GcpOptions.DefaultProjectFactory;
import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Tests for {@link GcpOptions}. */
@RunWith(JUnit4.class)
public class GcpOptionsTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testGetProjectFromCloudSdkConfigEnv() throws Exception {
    Map<String, String> environment =
        ImmutableMap.of("CLOUDSDK_CONFIG", tmpFolder.getRoot().getAbsolutePath());
    assertEquals("test-project",
        runGetProjectTest(tmpFolder.newFile("properties"), environment));
  }

  @Test
  public void testGetProjectFromAppDataEnv() throws Exception {
    Map<String, String> environment =
        ImmutableMap.of("APPDATA", tmpFolder.getRoot().getAbsolutePath());
    System.setProperty("os.name", "windows");
    assertEquals("test-project",
        runGetProjectTest(new File(tmpFolder.newFolder("gcloud"), "properties"),
            environment));
  }

  @Test
  public void testGetProjectFromUserHomeEnvOld() throws Exception {
    Map<String, String> environment = ImmutableMap.of();
    System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
    assertEquals("test-project",
        runGetProjectTest(
            new File(tmpFolder.newFolder(".config", "gcloud"), "properties"),
            environment));
  }

  @Test
  public void testGetProjectFromUserHomeEnv() throws Exception {
    Map<String, String> environment = ImmutableMap.of();
    System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
    assertEquals("test-project",
        runGetProjectTest(
            new File(tmpFolder.newFolder(".config", "gcloud", "configurations"), "config_default"),
            environment));
  }

  @Test
  public void testGetProjectFromUserHomeOldAndNewPrefersNew() throws Exception {
    Map<String, String> environment = ImmutableMap.of();
    System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
    makePropertiesFileWithProject(new File(tmpFolder.newFolder(".config", "gcloud"), "properties"),
        "old-project");
    assertEquals("test-project",
        runGetProjectTest(
            new File(tmpFolder.newFolder(".config", "gcloud", "configurations"), "config_default"),
            environment));
  }

  @Test
  public void testUnableToGetDefaultProject() throws Exception {
    System.setProperty("user.home", tmpFolder.getRoot().getAbsolutePath());
    DefaultProjectFactory projectFactory = spy(new DefaultProjectFactory());
    when(projectFactory.getEnvironment()).thenReturn(ImmutableMap.<String, String>of());
    assertNull(projectFactory.create(PipelineOptionsFactory.create()));
  }

  private static void makePropertiesFileWithProject(File path, String projectId)
      throws IOException {
    String properties = String.format("[core]%n"
        + "account = test-account@google.com%n"
        + "project = %s%n"
        + "%n"
        + "[dataflow]%n"
        + "magic = true%n", projectId);
    Files.write(properties, path, StandardCharsets.UTF_8);
  }

  private static String runGetProjectTest(File path, Map<String, String> environment)
      throws Exception {
    makePropertiesFileWithProject(path, "test-project");
    DefaultProjectFactory projectFactory = spy(new DefaultProjectFactory());
    when(projectFactory.getEnvironment()).thenReturn(environment);
    return projectFactory.create(PipelineOptionsFactory.create());
  }
}

