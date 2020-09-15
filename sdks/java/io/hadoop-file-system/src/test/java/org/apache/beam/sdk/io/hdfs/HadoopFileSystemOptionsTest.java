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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HadoopFileSystemOptions}. */
@RunWith(JUnit4.class)
public class HadoopFileSystemOptionsTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @SuppressWarnings("unchecked")
  @Test
  public void testParsingHdfsConfiguration() {
    HadoopFileSystemOptions options =
        PipelineOptionsFactory.fromArgs(
                "--hdfsConfiguration=[" + "{\"propertyA\": \"A\"}," + "{\"propertyB\": \"B\"}]")
            .as(HadoopFileSystemOptions.class);
    assertEquals(2, options.getHdfsConfiguration().size());
    assertThat(
        options.getHdfsConfiguration().get(0),
        Matchers.<Map.Entry<String, String>>contains(
            new AbstractMap.SimpleEntry<>("propertyA", "A")));
    assertThat(
        options.getHdfsConfiguration().get(1),
        Matchers.<Map.Entry<String, String>>contains(
            new AbstractMap.SimpleEntry<>("propertyB", "B")));
  }

  @Test
  public void testDefaultUnsetEnvHdfsConfiguration() {
    HadoopFileSystemOptions.ConfigurationLocator projectFactory =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());
    when(projectFactory.getEnvironment()).thenReturn(ImmutableMap.of());
    assertNull(projectFactory.create(PipelineOptionsFactory.create()));
  }

  @Test
  public void testDefaultJustSetHadoopConfDirConfiguration() throws IOException {
    Files.write(
        createPropertyData("A"), tmpFolder.newFile("core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), tmpFolder.newFile("hdfs-site.xml"), StandardCharsets.UTF_8);
    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("HADOOP_CONF_DIR", tmpFolder.getRoot().getAbsolutePath());
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultJustSetHadoopConfDirMultiPathConfiguration() throws IOException {
    File hadoopConfDir = tmpFolder.newFolder("hadoop");
    File otherConfDir = tmpFolder.newFolder("_other_");

    Files.write(
        createPropertyData("A"), new File(hadoopConfDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), new File(hadoopConfDir, "hdfs-site.xml"), StandardCharsets.UTF_8);

    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());

    String multiPath =
        hadoopConfDir.getAbsolutePath().concat(":").concat(otherConfDir.getAbsolutePath());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("HADOOP_CONF_DIR", multiPath);
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultJustSetYarnConfDirConfiguration() throws IOException {
    Files.write(
        createPropertyData("A"), tmpFolder.newFile("core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), tmpFolder.newFile("hdfs-site.xml"), StandardCharsets.UTF_8);
    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", tmpFolder.getRoot().getAbsolutePath());
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultJustSetYarnConfDirMultiPathConfiguration() throws IOException {
    File hadoopConfDir = tmpFolder.newFolder("hadoop");
    File otherConfDir = tmpFolder.newFolder("_other_");

    Files.write(
        createPropertyData("A"), new File(hadoopConfDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), new File(hadoopConfDir, "hdfs-site.xml"), StandardCharsets.UTF_8);

    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());

    String multiPath =
        hadoopConfDir.getAbsolutePath().concat(":").concat(otherConfDir.getAbsolutePath());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", multiPath);
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultSetYarnConfDirAndHadoopConfDirAndSameConfiguration() throws IOException {
    Files.write(
        createPropertyData("A"), tmpFolder.newFile("core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), tmpFolder.newFile("hdfs-site.xml"), StandardCharsets.UTF_8);
    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", tmpFolder.getRoot().getAbsolutePath());
    environment.put("HADOOP_CONF_DIR", tmpFolder.getRoot().getAbsolutePath());
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultSetYarnConfDirAndHadoopConfDirMultiPathAndSameConfiguration()
      throws IOException {
    File hadoopConfDir = tmpFolder.newFolder("hadoop");
    File otherConfDir = tmpFolder.newFolder("_other_");

    Files.write(
        createPropertyData("A"), new File(hadoopConfDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), new File(hadoopConfDir, "hdfs-site.xml"), StandardCharsets.UTF_8);

    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());

    String multiPath =
        hadoopConfDir.getAbsolutePath().concat(":").concat(otherConfDir.getAbsolutePath());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", multiPath);
    environment.put("HADOOP_CONF_DIR", multiPath);
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(1, configurationList.size());
    assertThat(configurationList.get(0).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(0).get("propertyB"), Matchers.equalTo("B"));
  }

  @Test
  public void testDefaultSetYarnConfDirAndHadoopConfDirNotSameConfiguration() throws IOException {
    File hadoopConfDir = tmpFolder.newFolder("hadoop");
    File yarnConfDir = tmpFolder.newFolder("yarn");
    Files.write(
        createPropertyData("A"), new File(hadoopConfDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("B"), new File(hadoopConfDir, "hdfs-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("C"), new File(yarnConfDir, "core-site.xml"), StandardCharsets.UTF_8);
    Files.write(
        createPropertyData("D"), new File(yarnConfDir, "hdfs-site.xml"), StandardCharsets.UTF_8);
    HadoopFileSystemOptions.ConfigurationLocator configurationLocator =
        spy(new HadoopFileSystemOptions.ConfigurationLocator());
    Map<String, String> environment = Maps.newHashMap();
    environment.put("YARN_CONF_DIR", hadoopConfDir.getAbsolutePath());
    environment.put("HADOOP_CONF_DIR", yarnConfDir.getAbsolutePath());
    when(configurationLocator.getEnvironment()).thenReturn(environment);

    List<Configuration> configurationList =
        configurationLocator.create(PipelineOptionsFactory.create());
    assertEquals(2, configurationList.size());
    int hadoopConfIndex = configurationList.get(0).get("propertyA") != null ? 0 : 1;
    assertThat(configurationList.get(hadoopConfIndex).get("propertyA"), Matchers.equalTo("A"));
    assertThat(configurationList.get(hadoopConfIndex).get("propertyB"), Matchers.equalTo("B"));
    assertThat(configurationList.get(1 - hadoopConfIndex).get("propertyC"), Matchers.equalTo("C"));
    assertThat(configurationList.get(1 - hadoopConfIndex).get("propertyD"), Matchers.equalTo("D"));
  }

  private static String createPropertyData(String property) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
        + "<configuration>\n"
        + "    <property>\n"
        + "        <name>property"
        + property
        + "</name>\n"
        + "        <value>"
        + property
        + "</value>\n"
        + "    </property>\n"
        + "</configuration>";
  }
}
