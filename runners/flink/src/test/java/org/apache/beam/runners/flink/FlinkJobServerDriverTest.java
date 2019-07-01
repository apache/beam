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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Charsets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link FlinkJobServerDriver}. */
public class FlinkJobServerDriverTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriverTest.class);

  @Test
  public void testConfigurationDefaults() {
    FlinkJobServerDriver.FlinkServerConfiguration config =
        new FlinkJobServerDriver.FlinkServerConfiguration();
    assertThat(config.getHost(), is("localhost"));
    assertThat(config.getPort(), is(8099));
    assertThat(config.getArtifactPort(), is(8098));
    assertThat(config.getExpansionPort(), is(8097));
    assertThat(config.getFlinkMasterUrl(), is("[auto]"));
    assertThat(config.getSdkWorkerParallelism(), is(1L));
    assertThat(config.isCleanArtifactsPerJob(), is(true));
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
              "--expansion-port",
              "44",
              "--flink-master-url=jobmanager",
              "--sdk-worker-parallelism=4",
              "--clean-artifacts-per-job=false",
            });
    FlinkJobServerDriver.FlinkServerConfiguration config =
        (FlinkJobServerDriver.FlinkServerConfiguration) driver.configuration;
    assertThat(config.getHost(), is("test"));
    assertThat(config.getPort(), is(42));
    assertThat(config.getArtifactPort(), is(43));
    assertThat(config.getExpansionPort(), is(44));
    assertThat(config.getFlinkMasterUrl(), is("jobmanager"));
    assertThat(config.getSdkWorkerParallelism(), is(4L));
    assertThat(config.isCleanArtifactsPerJob(), is(false));
  }

  @Test
  public void testConfigurationFromConfig() {
    FlinkJobServerDriver.FlinkServerConfiguration config =
        new FlinkJobServerDriver.FlinkServerConfiguration();
    FlinkJobServerDriver driver = FlinkJobServerDriver.fromConfig(config);
    assertThat(driver.configuration, is(config));
  }

  @Test(timeout = 30_000)
  public void testJobServerDriver() throws Exception {
    FlinkJobServerDriver driver = null;
    Thread driverThread = null;
    final PrintStream oldErr = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newErr = new PrintStream(baos);
    try {
      System.setErr(newErr);
      driver =
          FlinkJobServerDriver.fromParams(
              new String[] {"--job-port=0", "--artifact-port=0", "--expansion-port=0"});
      driverThread = new Thread(driver);
      driverThread.start();
      boolean success = false;
      while (!success) {
        newErr.flush();
        String output = baos.toString(Charsets.UTF_8.name());
        if (output.contains("JobService started on localhost:")
            && output.contains("ArtifactStagingService started on localhost:")
            && output.contains("ExpansionService started on localhost:")) {
          success = true;
        } else {
          Thread.sleep(100);
        }
      }
      assertThat(driverThread.isAlive(), is(true));
    } catch (Throwable t) {
      // restore to print exception
      System.setErr(oldErr);
      throw t;
    } finally {
      System.setErr(oldErr);
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
