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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * a Hadoop {@link Configuration}. The serialized representation is that of a JSON map.
 *
 * <p>Note that the serialization of the Hadoop {@link Configuration} only keeps the keys and their
 * values dropping any configuration hierarchy and source information.
 */
@AutoService(Module.class)
public class HadoopFileSystemModule extends SimpleModule {
  public HadoopFileSystemModule() {
    super("HadoopFileSystemModule");
    setMixInAnnotation(Configuration.class, ConfigurationMixin.class);
  }

  /** A mixin class to add Jackson annotations to the Hadoop {@link Configuration} class. */
  @JsonDeserialize(using = ConfigurationDeserializer.class)
  @JsonSerialize(using = ConfigurationSerializer.class)
  private static class ConfigurationMixin {}

  /** A Jackson {@link JsonDeserializer} for Hadoop {@link Configuration} objects. */
  private static class ConfigurationDeserializer extends JsonDeserializer<Configuration> {
    @Override
    public Configuration deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      Map<String, String> rawConfiguration =
          jsonParser.readValueAs(new TypeReference<Map<String, String>>() {});
      Configuration configuration = new Configuration(false);
      for (Map.Entry<String, String> entry : rawConfiguration.entrySet()) {
        configuration.set(entry.getKey(), entry.getValue());
      }
      return configuration;
    }
  }

  /** A Jackson {@link JsonSerializer} for Hadoop {@link Configuration} objects. */
  private static class ConfigurationSerializer extends JsonSerializer<Configuration> {
    @Override
    public void serialize(
        Configuration configuration,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      Map<String, String> map = new TreeMap<>();
      for (Map.Entry<String, String> entry : configuration) {
        map.put(entry.getKey(), entry.getValue());
      }
      jsonGenerator.writeObject(map);
    }
  }
}
