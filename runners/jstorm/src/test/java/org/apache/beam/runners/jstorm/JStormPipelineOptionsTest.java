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
package org.apache.beam.runners.jstorm;

import static org.junit.Assert.assertEquals;

import com.alibaba.jstorm.client.ConfigExtension;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link JStormPipelineOptions}.
 */
@RunWith(JUnit4.class)
public class JStormPipelineOptionsTest {

  /**
   * Test Options.
   */
  public interface TestOptions extends JStormPipelineOptions {
    @Description("Test option")
    @Default.String("nothing")
    String getTestOption();
    void setTestOption(String testOption);
  }

  private static TestOptions options;
  private static final String[] args = new String[]{"--runner=JStormRunner", "--testOption=test"};

  @BeforeClass
  public static void prepare() {
    options = PipelineOptionsFactory.fromArgs(args).as(TestOptions.class);
  }

  @Test
  public void testRunnerOption() {
    assertEquals(JStormRunner.class, options.getRunner());
  }

  @Test
  public void testUserDefinedOption() {
    assertEquals("test", options.getTestOption());
  }

  @Test
  public void testLocalModeOption() {
    assertEquals(false, options.getLocalMode());
    options.setLocalMode(true);
    assertEquals(true, options.getLocalMode());
  }

  @Test
  public void testLocalExecuteTime() {
    assertEquals(Long.valueOf(60), options.getLocalModeExecuteTimeSec());
    options.setLocalModeExecuteTimeSec(120L);
    assertEquals(Long.valueOf(120), options.getLocalModeExecuteTimeSec());
  }

  @Test
  public void testWorkerNumberOption() {
    assertEquals(Integer.valueOf(1), options.getWorkerNumber());
    options.setWorkerNumber(2);
    assertEquals(Integer.valueOf(2), options.getWorkerNumber());
  }

  @Test
  public void testParallelism() {
    assertEquals(Integer.valueOf(1), options.getParallelism());
    options.setParallelism(2);
    assertEquals(Integer.valueOf(2), options.getParallelism());
  }

  @Test
  public void testJStormConfigOption() {
    assertEquals(Maps.newHashMap(), options.getTopologyConfig());
    options.getTopologyConfig().put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, false);
    Map config = Maps.newHashMap();
    config.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, false);
    assertEquals(config, options.getTopologyConfig());
  }
}
