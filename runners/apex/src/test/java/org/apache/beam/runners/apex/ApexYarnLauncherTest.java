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
package org.apache.beam.runners.apex;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import java.io.File;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for dependency resolution for pipeline execution on YARN.
 */
public class ApexYarnLauncherTest {

  @Test
  public void testGetYarnDeployDependencies() throws Exception {
    List<File> deps = ApexYarnLauncher.getYarnDeployDependencies();
    String depsToString = deps.toString();
    // the beam dependencies are not present as jar when running within the Maven build reactor
    //assertThat(depsToString, containsString("beam-runners-core-"));
    //assertThat(depsToString, containsString("beam-runners-apex-"));
    assertThat(depsToString, containsString("apex-common-"));
    assertThat(depsToString, not(containsString("hadoop-")));
    assertThat(depsToString, not(containsString("zookeeper-")));
  }

  @Test
  public void testProxyLauncher() throws Exception {
    // use the embedded launcher to build the DAG only
    EmbeddedAppLauncher<?> embeddedLauncher = Launcher.getLauncher(LaunchMode.EMBEDDED);

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf) {
        dag.setAttribute(DAGContext.APPLICATION_NAME, "DummyApp");
      }
    };

    Configuration conf = new Configuration(false);
    DAG dag = embeddedLauncher.prepareDAG(app, conf);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    Properties configProperties = new Properties();
    ApexYarnLauncher launcher = new ApexYarnLauncher();
    launcher.launchApp(new MockApexYarnLauncherParams(dag, launchAttributes, configProperties));
  }

  private static class MockApexYarnLauncherParams extends  ApexYarnLauncher.LaunchParams {
    private static final long serialVersionUID = 1L;

    public MockApexYarnLauncherParams(DAG dag, AttributeMap launchAttributes,
        Properties properties) {
      super(dag, launchAttributes, properties);
    }

    @Override
    protected Launcher<?> getApexLauncher() {
      return new Launcher<AppHandle>() {
        @Override
        public AppHandle launchApp(StreamingApplication application,
            Configuration configuration, AttributeMap launchParameters)
            throws org.apache.apex.api.Launcher.LauncherException {
          EmbeddedAppLauncher<?> embeddedLauncher = Launcher.getLauncher(LaunchMode.EMBEDDED);
          DAG dag = embeddedLauncher.getDAG();
          application.populateDAG(dag, new Configuration(false));
          String appName = dag.getValue(DAGContext.APPLICATION_NAME);
          Assert.assertEquals("DummyApp", appName);
          return new AppHandle() {
            @Override
            public boolean isFinished() {
              return true;
            }
            @Override
            public void shutdown(org.apache.apex.api.Launcher.ShutdownMode arg0) {
            }
          };
        }
      };
    }

  }

  @Test
  public void testCreateJar() throws Exception {
    File baseDir = new File("./target/testCreateJar");
    File srcDir = new File(baseDir, "src");
    String file1 = "file1";
    FileUtils.forceMkdir(srcDir);
    FileUtils.write(new File(srcDir, file1), "file1");

    File jarFile = new File(baseDir, "test.jar");
    ApexYarnLauncher.createJar(srcDir, jarFile);
    Assert.assertTrue("exists: " + jarFile, jarFile.exists());
    URI uri = URI.create("jar:" + jarFile.toURI());
    final Map<String, ?> env = Collections.singletonMap("create", "true");
    try (final FileSystem zipfs = FileSystems.newFileSystem(uri, env);) {
      Assert.assertTrue("manifest", Files.isRegularFile(zipfs.getPath(JarFile.MANIFEST_NAME)));
      Assert.assertTrue("file1", Files.isRegularFile(zipfs.getPath(file1)));
    }

  }
}
