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
import java.util.Collections;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdTester;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link HadoopResourceId}.
 */
public class HadoopResourceIdTest {

  private MiniDFSCluster hdfsCluster;
  private URI hdfsClusterBaseUri;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.getRoot().getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");

    // Register HadoopFileSystem for this test.
    HadoopFileSystemOptions options = PipelineOptionsFactory.as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(configuration));
    FileSystems.setDefaultPipelineOptions(options);
  }

  @After
  public void tearDown() throws Exception {
    hdfsCluster.shutdown();
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-4142")
  public void testResourceIdTester() throws Exception {
    ResourceId baseDirectory =
        FileSystems.matchNewResource(
            "hdfs://" + hdfsClusterBaseUri.getPath(), true /* isDirectory */);
    ResourceIdTester.runResourceIdBattery(baseDirectory);
  }
}
