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
package org.apache.beam.runners.gearpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.junit.Test;

/**
 * Tests for {@link GearpumpPipelineOptions}.
 */
public class PipelineOptionsTest {

  @Test
  public void testIgnoredFieldSerialization() throws IOException {
    String appName = "forTest";
    Map<String, String> serializers = Maps.newHashMap();
    serializers.put("classA", "SerializerA");
    GearpumpPipelineOptions options = PipelineOptionsFactory.create()
      .as(GearpumpPipelineOptions.class);
    Config config = ClusterConfig.master(null);
    EmbeddedCluster cluster = new EmbeddedCluster(config);
    options.setSerializers(serializers);
    options.setApplicationName(appName);
    options.setEmbeddedCluster(cluster);
    options.setParallelism(10);

    byte[] serializedOptions = serialize(options);
    GearpumpPipelineOptions deserializedOptions = new ObjectMapper()
      .readValue(serializedOptions, PipelineOptions.class).as(GearpumpPipelineOptions.class);

    assertNull(deserializedOptions.getEmbeddedCluster());
    assertNull(deserializedOptions.getSerializers());
    assertEquals(10, deserializedOptions.getParallelism());
    assertEquals(appName, deserializedOptions.getApplicationName());
  }

  private byte[] serialize(Object obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      new ObjectMapper().writeValue(baos, obj);
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't serialize PipelineOptions.", e);
    }
  }
}
