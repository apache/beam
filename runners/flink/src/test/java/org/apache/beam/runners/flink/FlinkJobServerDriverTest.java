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
package org.apache.beam.runners.flink;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Charsets;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link FlinkJobServerDriver}. */
public class FlinkJobServerDriverTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriverTest.class);

  @Test
  public void testConfigurationDefaults() {
    FlinkJobServerDriver.ServerConfiguration config =
        new FlinkJobServerDriver.ServerConfiguration();
    assertThat(config.host, is("localhost"));
    assertThat(config.port, is(8099));
    assertThat(config.artifactPort, is(8098));
    assertThat(config.flinkMasterUrl, is("[auto]"));
    assertThat(config.sdkWorkerParallelism, is(1L));
    assertThat(config.cleanArtifactsPerJob, is(false));
    FlinkJobServerDriver flinkJobServerDriver = FlinkJobServerDriver.fromConfig(config);
    assertThat(flinkJobServerDriver, is(not(nullValue())));
  }

  @Test
  public void testConfigurationFromArgs() {
    FlinkJobServerDriver driver =
        FlinkJobServerDriver.fromParams(
            new String[] {
              "--job-host=test",
              "--job-port",
              "42",
              "--artifact-port",
              "43",
              "--flink-master-url=jobmanager",
              "--sdk-worker-parallelism=4",
              "--clean-artifacts-per-job",
            });
    assertThat(driver.configuration.host, is("test"));
    assertThat(driver.configuration.port, is(42));
    assertThat(driver.configuration.artifactPort, is(43));
    assertThat(driver.configuration.flinkMasterUrl, is("jobmanager"));
    assertThat(driver.configuration.sdkWorkerParallelism, is(4L));
    assertThat(driver.configuration.cleanArtifactsPerJob, is(true));
  }

  @Test
  public void testConfigurationFromConfig() {
    FlinkJobServerDriver.ServerConfiguration config =
        new FlinkJobServerDriver.ServerConfiguration();
    FlinkJobServerDriver driver = FlinkJobServerDriver.fromConfig(config);
    assertThat(driver.configuration, is(config));
  }

  @Test(timeout = 30_000)
  public void testJobServerDriver() throws Exception {
    FlinkJobServerDriver driver = null;
    Thread driverThread = null;
    final PrintStream oldOut = System.out;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(baos);
    try {
      System.setErr(newOut);
      driver = FlinkJobServerDriver.fromParams(new String[] {"--job-port=0", "--artifact-port=0"});
      driverThread = new Thread(driver);
      driverThread.start();
      boolean success = false;
      while (!success) {
        newOut.flush();
        String output = baos.toString(Charsets.UTF_8.name());
        if (output.contains("JobService started on localhost:")
            && output.contains("ArtifactStagingService started on localhost:")) {
          success = true;
        } else {
          Thread.sleep(100);
        }
      }
      assertThat(driverThread.isAlive(), is(true));
    } finally {
      System.setErr(oldOut);
      if (driver != null) {
        driver.stop();
      }
      if (driverThread != null) {
        driverThread.interrupt();
        driverThread.join();
      }
    }
  }
}
