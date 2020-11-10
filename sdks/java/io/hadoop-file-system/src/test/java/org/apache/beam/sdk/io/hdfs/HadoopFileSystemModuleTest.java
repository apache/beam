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

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link HadoopFileSystemModule}. */
@RunWith(JUnit4.class)
public class HadoopFileSystemModuleTest {
  @Test
  public void testObjectMapperIsAbleToFindModule() {
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    assertThat(modules, hasItem(Matchers.<Module>instanceOf(HadoopFileSystemModule.class)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConfigurationSerializationDeserialization() throws Exception {
    Configuration baseConfiguration = new Configuration(false);
    baseConfiguration.set("testPropertyA", "baseA");
    baseConfiguration.set("testPropertyC", "baseC");
    Configuration configuration = new Configuration(false);
    configuration.addResource(baseConfiguration);
    configuration.set("testPropertyA", "A");
    configuration.set("testPropertyB", "B");
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new HadoopFileSystemModule());
    String serializedConfiguration = objectMapper.writeValueAsString(configuration);
    Configuration deserializedConfiguration =
        objectMapper.readValue(serializedConfiguration, Configuration.class);
    assertThat(
        deserializedConfiguration,
        Matchers.<Map.Entry<String, String>>containsInAnyOrder(
            new AbstractMap.SimpleEntry<>("testPropertyA", "A"),
            new AbstractMap.SimpleEntry<>("testPropertyB", "B"),
            new AbstractMap.SimpleEntry<>("testPropertyC", "baseC")));
  }
}
