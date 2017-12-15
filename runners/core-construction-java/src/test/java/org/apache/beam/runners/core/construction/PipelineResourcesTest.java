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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Tests for PipelineResources.
 */
@RunWith(JUnit4.class)
public class PipelineResourcesTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file");
    File file2 = tmpFolder.newFile("file2");
    URLClassLoader classLoader = new URLClassLoader(new URL[] {
        file.toURI().toURL(),
        file2.toURI().toURL()
    });

    assertEquals(ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
        PipelineResources.detectClassPathResourcesToStage(classLoader));
  }

  @Test
  public void detectClassPathResourcesWithUnsupportedClassLoader() {
    ClassLoader mockClassLoader = Mockito.mock(ClassLoader.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to use ClassLoader to detect classpath elements.");

    PipelineResources.detectClassPathResourcesToStage(mockClassLoader);
  }

  @Test
  public void detectClassPathResourceWithNonFileResources() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    URLClassLoader classLoader = new URLClassLoader(new URL[] {
        new URL(url)
    });
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to convert url (" + url + ") to file.");

    PipelineResources.detectClassPathResourcesToStage(classLoader);
  }
}
