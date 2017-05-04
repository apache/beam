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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link HadoopFileSystemOptions}.
 */
@RunWith(JUnit4.class)
public class HadoopFileSystemOptionsTest {
  @Test
  public void testParsingHdfsConfiguration() {
    HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(
        "--hdfsConfiguration=["
            + "{\"propertyA\": \"A\"},"
            + "{\"propertyB\": \"B\"}]").as(HadoopFileSystemOptions.class);
    assertEquals(2, options.getHdfsConfiguration().size());
    assertThat(options.getHdfsConfiguration().get(0), Matchers.<Map.Entry<String, String>>contains(
        new AbstractMap.SimpleEntry("propertyA", "A")));
    assertThat(options.getHdfsConfiguration().get(1), Matchers.<Map.Entry<String, String>>contains(
        new AbstractMap.SimpleEntry("propertyB", "B")));
  }

  @Test
  public void testDefaultUnsetEnvHdfsConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
            spy(new HadoopFileSystemOptions.ConfigurationLocator());
    when(projectFactory.getEnvironment()).thenReturn(ImmutableMap.<String, String>of());
    assertNull(projectFactory.create(PipelineOptionsFactory.create()));
  }

  @Test
  public void testDefaultJustSetHadoopConfDirConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
            spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("HADOOP_CONF_DIR", this.getClass().getResource("/").getPath());
    when(projectFactory.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList = projectFactory.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultJustSetYarnConfDirConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
            spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", this.getClass().getResource("/").getPath());
    when(projectFactory.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList = projectFactory.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultSetYarnConfDirAndHadoopConfDirAndSameConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
            spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", this.getClass().getResource("/").getPath());
    environment.put("HADOOP_CONF_DIR", this.getClass().getResource("/").getPath());
    when(projectFactory.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList = projectFactory.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultSetYarnConfDirAndHadoopConfDirNotSameConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
              spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", this.getClass().getResource("/").getPath() + "/conf");
    environment.put("HADOOP_CONF_DIR", this.getClass().getResource("/").getPath());
    when(projectFactory.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList = projectFactory.create(PipelineOptionsFactory.create());
    assertEquals(2, configurationList.size());
  }
}
