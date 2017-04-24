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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for IOChannelUtils.
 */
@RunWith(JUnit4.class)
public class IOChannelUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test(expected = IllegalArgumentException.class)
  public void testShardNameCollision() throws Exception {
    File outFolder = tmpFolder.newFolder();
    String filename = outFolder.toPath().resolve("output").toString();

    IOChannelUtils.create(filename, "", "", 2, "text").close();
    fail("IOChannelUtils.create expected to fail due "
        + "to filename collision");
  }

  @Test
  public void testHandlerNoScheme() throws Exception {
    String pathToTempFolder = tmpFolder.getRoot().getAbsolutePath();
    assertThat(IOChannelUtils.getFactory(pathToTempFolder), instanceOf(FileIOChannelFactory.class));
  }

  @Test
  public void testGetSizeBytes() throws Exception {
    String data = "TestData";
    File file = tmpFolder.newFile();
    Files.write(data, file, StandardCharsets.UTF_8);
    assertEquals(data.length(), IOChannelUtils.getSizeBytes(file.getPath()));
  }

  @Test
  public void testResolveSinglePath() throws Exception {
    String expected = tmpFolder.getRoot().toPath().resolve("aa").toString();
    assertEquals(expected, IOChannelUtils.resolve(tmpFolder.getRoot().toString(), "aa"));
  }

  @Test
  public void testResolveMultiplePaths() throws Exception {
    String expected =
        tmpFolder.getRoot().toPath().resolve("aa").resolve("bb").resolve("cc").toString();
    assertEquals(expected,
        IOChannelUtils.resolve(tmpFolder.getRoot().getPath(), "aa", "bb", "cc"));
  }

  @Test
  public void testRegisterIOFactoriesAllowOverride() throws Exception {
    IOChannelUtils.registerIOFactoriesAllowOverride(PipelineOptionsFactory.create());
    IOChannelUtils.registerIOFactoriesAllowOverride(PipelineOptionsFactory.create());
    assertNotNull(IOChannelUtils.getFactory("gs"));
    assertNotNull(IOChannelUtils.getFactory("file"));
  }

  @Test
  public void testRegisterIOFactories() throws Exception {
    IOChannelUtils.deregisterScheme("gs");
    IOChannelUtils.deregisterScheme("file");

    IOChannelUtils.registerIOFactories(PipelineOptionsFactory.create());
    assertNotNull(IOChannelUtils.getFactory("gs"));
    assertNotNull(IOChannelUtils.getFactory("file"));
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to register IOChannelFactory");
    thrown.expectMessage("override is not allowed");
    IOChannelUtils.registerIOFactories(PipelineOptionsFactory.create());
  }

  @Test
  public void testCheckDuplicateScheme() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Scheme: [file] has conflicting registrars");
    IOChannelUtils.checkDuplicateScheme(
        Sets.<IOChannelFactoryRegistrar>newHashSet(
            new FileIOChannelFactoryRegistrar(),
            new FileIOChannelFactoryRegistrar()));
  }
}
