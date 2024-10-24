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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.NullValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Value;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link PipelineOptionsTranslation}. */
public class PipelineOptionsTranslationTest {
  /** Tests that translations can round-trip through the proto format. */
  @RunWith(Parameterized.class)
  public static class ToFromProtoTest {
    private static final ObjectMapper MAPPER =
        new ObjectMapper()
            .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

    @Parameters(name = "{index}: {0}")
    public static Iterable<? extends PipelineOptions> options() {
      PipelineOptionsFactory.register(TestUnserializableOptions.class);
      PipelineOptionsFactory.register(TestDefaultOptions.class);
      PipelineOptionsFactory.register(TestOptions.class);
      PipelineOptions emptyOptions = PipelineOptionsFactory.create();

      TestUnserializableOptions withNonSerializable =
          PipelineOptionsFactory.as(TestUnserializableOptions.class);
      withNonSerializable.setUnserializable(new Object());

      TestOptions withCustomField = PipelineOptionsFactory.as(TestOptions.class);
      withCustomField.setExample(99);

      PipelineOptions withSettings = PipelineOptionsFactory.create();
      withSettings.as(ApplicationNameOptions.class).setAppName("my_app");
      withSettings.setJobName("my_job");

      PipelineOptions withParsedSettings =
          PipelineOptionsFactory.fromArgs("--jobName=my_job --appName=my_app").create();

      return ImmutableList.of(
          emptyOptions, withNonSerializable, withCustomField, withSettings, withParsedSettings);
    }

    @Parameter(0)
    public PipelineOptions options;

    @Test
    public void testToFromProto() throws Exception {
      options.getOptionsId();
      Struct originalStruct = PipelineOptionsTranslation.toProto(options);
      PipelineOptions deserializedStruct = PipelineOptionsTranslation.fromProto(originalStruct);

      Struct reserializedStruct = PipelineOptionsTranslation.toProto(deserializedStruct);
      assertThat(reserializedStruct.getFieldsMap(), equalTo(originalStruct.getFieldsMap()));
    }

    @Test
    public void testToFromJson() throws Exception {
      options.getOptionsId();
      Struct originalStruct = PipelineOptionsTranslation.toProto(options);
      String json = PipelineOptionsTranslation.toJson(options);
      String legacyJson = MAPPER.writeValueAsString(options);

      assertThat(
          PipelineOptionsTranslation.toProto(PipelineOptionsTranslation.fromJson(json))
              .getFieldsMap(),
          equalTo(originalStruct.getFieldsMap()));
      assertThat(
          PipelineOptionsTranslation.toProto(PipelineOptionsTranslation.fromJson(legacyJson))
              .getFieldsMap(),
          equalTo(originalStruct.getFieldsMap()));
    }
  }

  /** Tests that translations contain the correct contents. */
  @RunWith(JUnit4.class)
  public static class TranslationTest {
    @Test
    public void customSettingsRetained() throws Exception {
      TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
      options.setExample(23);
      Struct serialized = PipelineOptionsTranslation.toProto(options);
      PipelineOptions deserialized = PipelineOptionsTranslation.fromProto(serialized);

      assertThat(deserialized.as(TestOptions.class).getExample(), equalTo(23));
    }

    @Test
    public void ignoredSettingsNotSerialized() throws Exception {
      TestUnserializableOptions opts = PipelineOptionsFactory.as(TestUnserializableOptions.class);
      opts.setUnserializable(new Object());

      Struct serialized = PipelineOptionsTranslation.toProto(opts);
      PipelineOptions deserialized = PipelineOptionsTranslation.fromProto(serialized);

      assertThat(
          deserialized.as(TestUnserializableOptions.class).getUnserializable(), is(nullValue()));
    }

    @Test
    public void defaultsRestored() throws Exception {
      Struct serialized =
          PipelineOptionsTranslation.toProto(PipelineOptionsFactory.as(TestDefaultOptions.class));
      PipelineOptions deserialized = PipelineOptionsTranslation.fromProto(serialized);

      assertThat(deserialized.as(TestDefaultOptions.class).getDefault(), equalTo(19));
    }

    @Test
    public void emptyStructDeserializes() throws Exception {
      Struct serialized = Struct.getDefaultInstance();
      PipelineOptions deserialized = PipelineOptionsTranslation.fromProto(serialized);

      assertThat(deserialized, notNullValue());
    }

    @Test
    public void structWithNullOptionsDeserializes() throws Exception {
      Struct serialized =
          Struct.newBuilder()
              .putFields(
                  "beam:option:option_key:v1",
                  Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
              .build();
      PipelineOptions deserialized = PipelineOptionsTranslation.fromProto(serialized);

      assertThat(deserialized, notNullValue());
    }
  }

  /** {@link PipelineOptions} with an unserializable option. */
  public interface TestUnserializableOptions extends PipelineOptions {
    @JsonIgnore
    Object getUnserializable();

    void setUnserializable(Object unserializable);
  }

  /** {@link PipelineOptions} with an default option. */
  public interface TestDefaultOptions extends PipelineOptions {
    @Default.Integer(19)
    int getDefault();

    void setDefault(int example);
  }

  /** {@link PipelineOptions} for testing. */
  public interface TestOptions extends PipelineOptions {
    int getExample();

    void setExample(int example);
  }
}
