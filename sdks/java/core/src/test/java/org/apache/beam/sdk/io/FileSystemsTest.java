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
package org.apache.beam.sdk.io;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FileSystems}.
 */
@RunWith(JUnit4.class)
public class FileSystemsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSetDefaultConfig() throws Exception {
    FileSystems.setDefaultConfig("file", PipelineOptionsFactory.create());
    FileSystems.setDefaultConfig("FILE", PipelineOptionsFactory.create());
  }

  @Test
  public void testSetDefaultConfigNotFound() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No FileSystemRegistrar found for scheme: [gs-s3].");
    FileSystems.setDefaultConfig("gs-s3", PipelineOptionsFactory.create());
  }

  @Test
  public void testSetDefaultConfigInvalidScheme() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Scheme: [gs:] doesn't match URI syntax");
    FileSystems.setDefaultConfig("gs:", PipelineOptionsFactory.create());
  }

  @Test
  public void testGetLocalFileSystem() throws Exception {
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("~/home/")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("file://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("FILE://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("File://home")) instanceof LocalFileSystem);
  }

  @Test
  public void testVerifySchemesAreUnique() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Scheme: [file] has conflicting registrars");
    FileSystems.verifySchemesAreUnique(
        Sets.<FileSystemRegistrar>newHashSet(
            new LocalFileSystemRegistrar(),
            new FileSystemRegistrar() {
              @Override
              public FileSystem fromOptions(@Nullable PipelineOptions options) {
                return null;
              }

              @Override
              public String getScheme() {
                return "FILE";
              }
            }));
  }
}
