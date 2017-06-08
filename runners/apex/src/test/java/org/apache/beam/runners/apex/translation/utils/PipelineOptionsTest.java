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
package org.apache.beam.runners.apex.translation.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.datatorrent.common.util.FSStorageAgent;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/**
 * Tests the serialization of PipelineOptions.
 */
public class PipelineOptionsTest {

  /**
   * Interface for testing.
   */
  public interface MyOptions extends ApexPipelineOptions {
    @Description("Bla bla bla")
    @Default.String("Hello")
    String getTestOption();
    void setTestOption(String value);
  }

  private static class OptionsWrapper {
    private OptionsWrapper() {
      this(null); // required for Kryo
    }
    private OptionsWrapper(ApexPipelineOptions options) {
      this.options = new SerializablePipelineOptions(options);
    }
    @Bind(JavaSerializer.class)
    private final SerializablePipelineOptions options;
  }

  @Test
  public void testSerialization() {
    OptionsWrapper wrapper = new OptionsWrapper(
        PipelineOptionsFactory.fromArgs("--testOption=nothing").as(MyOptions.class));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FSStorageAgent.store(bos, wrapper);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    OptionsWrapper wrapperCopy = (OptionsWrapper) FSStorageAgent.retrieve(bis);
    assertNotNull(wrapperCopy.options);
    assertEquals("nothing", wrapperCopy.options.get().as(MyOptions.class).getTestOption());
  }

  @Test
  public void testSerializationWithUserCustomType() {
    OptionsWrapper wrapper = new OptionsWrapper(
        PipelineOptionsFactory.fromArgs("--jacksonIncompatible=\"testValue\"")
            .as(JacksonIncompatibleOptions.class));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FSStorageAgent.store(bos, wrapper);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    OptionsWrapper wrapperCopy = (OptionsWrapper) FSStorageAgent.retrieve(bis);
    assertNotNull(wrapperCopy.options);
    assertEquals("testValue",
        wrapperCopy.options.get().as(JacksonIncompatibleOptions.class)
            .getJacksonIncompatible().value);
  }

  /** PipelineOptions used to test auto registration of Jackson modules. */
  public interface JacksonIncompatibleOptions extends ApexPipelineOptions {
    JacksonIncompatible getJacksonIncompatible();
    void setJacksonIncompatible(JacksonIncompatible value);
  }

  /** A Jackson {@link Module} to test auto-registration of modules. */
  @AutoService(Module.class)
  public static class RegisteredTestModule extends SimpleModule {
    public RegisteredTestModule() {
      super("RegisteredTestModule");
      setMixInAnnotation(JacksonIncompatible.class, JacksonIncompatibleMixin.class);
    }
  }

  /** A class which Jackson does not know how to serialize/deserialize. */
  public static class JacksonIncompatible {
    private final String value;
    public JacksonIncompatible(String value) {
      this.value = value;
    }
  }

  /** A Jackson mixin used to add annotations to other classes. */
  @JsonDeserialize(using = JacksonIncompatibleDeserializer.class)
  @JsonSerialize(using = JacksonIncompatibleSerializer.class)
  public static final class JacksonIncompatibleMixin {}

  /** A Jackson deserializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleDeserializer extends
      JsonDeserializer<JacksonIncompatible> {

    @Override
    public JacksonIncompatible deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      return new JacksonIncompatible(jsonParser.readValueAs(String.class));
    }
  }

  /** A Jackson serializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleSerializer extends JsonSerializer<JacksonIncompatible> {

    @Override
    public void serialize(JacksonIncompatible jacksonIncompatible, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      jsonGenerator.writeString(jacksonIncompatible.value);
    }
  }
}
