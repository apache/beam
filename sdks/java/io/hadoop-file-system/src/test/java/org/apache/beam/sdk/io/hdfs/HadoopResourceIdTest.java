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
package org.apache.beam.sdk.io.hdfs;

import java.net.URI;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceIdTester;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link HadoopResourceId}.
 */
public class HadoopResourceIdTest {
  private Configuration configuration;
  private MiniDFSCluster hdfsCluster;
  private URI hdfsClusterBaseUri;
  private HadoopFileSystem fileSystem;
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    configuration = new Configuration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.getRoot().getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");
    fileSystem = new HadoopFileSystem(configuration);
  }

  @After
  public void tearDown() throws Exception {
    hdfsCluster.shutdown();
  }

  @Test
  public void testResourceIdTester() throws Exception {
    FileSystems.setDefaultConfigInWorkers(TestPipeline.testingPipelineOptions());
    ResourceIdTester.runResourceIdBattery(new HadoopResourceId(hdfsClusterBaseUri));
  }
}
