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

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PipelineResources.
 */
@RunWith(JUnit4.class)
public class PipelineResourcesTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file.jar");
    try (final JarOutputStream jar = new JarOutputStream(new FileOutputStream(file))) {
      // if there is anything in a valid jar this entry should be matched
      // otherwise jar has nothing inside and we don't care about it anyway
      jar.putNextEntry(new JarEntry(""));
      jar.closeEntry();
    }
    File file2 = tmpFolder.newFolder("file2");
    try (final URLClassLoader classLoader = new URLClassLoader(new URL[] {
        file.toURI().toURL(),
        file2.toURI().toURL()
    })) {
      assertEquals(ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
              PipelineResources.detectClassPathResourcesToStage(null, classLoader).stream()
                      .sorted().collect(toList()));
    }
  }

  @Test
  public void detectClassPathResourceWithFilter() throws Exception {
    File file = tmpFolder.newFile("file.jar");
    try (final JarOutputStream jar = new JarOutputStream(new FileOutputStream(file))) {
      // if there is anything in a valid jar this entry should be matched
      // otherwise jar has nothing inside and we don't care about it anyway
      jar.putNextEntry(new JarEntry(""));
      jar.closeEntry();
    }
    File file2 = tmpFolder.newFolder("file2");
    try (final URLClassLoader classLoader = new URLClassLoader(new URL[] {
        file.toURI().toURL(),
        file2.toURI().toURL()
    })) {
      assertEquals(ImmutableList.of(file.getAbsolutePath()),
              PipelineResources.detectClassPathResourcesToStage(
                      PipelineOptionsFactory.fromArgs("--classLoaderIncludeFilter=file\\.jar")
                              .as(FilterableStagingFilesPipelineOptions.class), classLoader)
                      .stream().sorted().collect(toList()));
      assertEquals(ImmutableList.of(file2.getAbsolutePath()),
              PipelineResources.detectClassPathResourcesToStage(
                      PipelineOptionsFactory.fromArgs("--classLoaderExcludeFilter=file\\.jar")
                              .as(FilterableStagingFilesPipelineOptions.class), classLoader)
                      .stream().sorted().collect(toList()));
    }
  }

  @Test
  public void detectClassPathResourceWithNonFileResources() throws Exception {
    final String url = "http://www.google.com/all-the-secrets.jar";
    try (final URLClassLoader classLoader = new URLClassLoader(new URL[] {
        new URL(url)
    })) {
      assertEquals(0, PipelineResources.detectClassPathResourcesToStage(null, classLoader).size());
    }
  }
}
