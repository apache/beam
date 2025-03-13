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
package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializablePipelineOptions}. */
@RunWith(JUnit4.class)
public class SerializablePipelineOptionsTest {
  /** Options for testing. */
  public interface MyOptions extends PipelineOptions {
    String getFoo();

    void setFoo(String foo);

    @JsonIgnore
    @Default.String("not overridden")
    String getIgnoredField();

    void setIgnoredField(String value);
  }

  @Test
  public void testSerializationAndDeserialization() throws Exception {
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--foo=testValue", "--ignoredField=overridden")
            .as(MyOptions.class);

    SerializablePipelineOptions serializableOptions = new SerializablePipelineOptions(options);
    assertEquals("testValue", serializableOptions.get().as(MyOptions.class).getFoo());
    assertEquals("overridden", serializableOptions.get().as(MyOptions.class).getIgnoredField());

    SerializablePipelineOptions copy = SerializableUtils.clone(serializableOptions);
    assertEquals("testValue", copy.get().as(MyOptions.class).getFoo());
    assertEquals("not overridden", copy.get().as(MyOptions.class).getIgnoredField());
  }

  @Test
  public void testIndependence() throws Exception {
    SerializablePipelineOptions first =
        new SerializablePipelineOptions(
            PipelineOptionsFactory.fromArgs("--foo=first").as(MyOptions.class));
    SerializablePipelineOptions firstCopy = SerializableUtils.clone(first);
    SerializablePipelineOptions second =
        new SerializablePipelineOptions(
            PipelineOptionsFactory.fromArgs("--foo=second").as(MyOptions.class));
    SerializablePipelineOptions secondCopy = SerializableUtils.clone(second);

    assertEquals("first", first.get().as(MyOptions.class).getFoo());
    assertEquals("first", firstCopy.get().as(MyOptions.class).getFoo());
    assertEquals("second", second.get().as(MyOptions.class).getFoo());
    assertEquals("second", secondCopy.get().as(MyOptions.class).getFoo());

    first.get().as(MyOptions.class).setFoo("new first");
    firstCopy.get().as(MyOptions.class).setFoo("new firstCopy");
    second.get().as(MyOptions.class).setFoo("new second");
    secondCopy.get().as(MyOptions.class).setFoo("new secondCopy");

    assertEquals("new first", first.get().as(MyOptions.class).getFoo());
    assertEquals("new firstCopy", firstCopy.get().as(MyOptions.class).getFoo());
    assertEquals("new second", second.get().as(MyOptions.class).getFoo());
    assertEquals("new secondCopy", secondCopy.get().as(MyOptions.class).getFoo());
  }

  @Test
  public void equalityTest() {
    PipelineOptions options = PipelineOptionsFactory.create();
    SerializablePipelineOptions serializablePipelineOptions =
        new SerializablePipelineOptions(options);
    String json = serializablePipelineOptions.toString();
    SerializablePipelineOptions serializablePipelineOptions2 =
        new SerializablePipelineOptions(json);
    assertEquals(
        "SerializablePipelineOptions created from options and from json differ",
        serializablePipelineOptions,
        serializablePipelineOptions2);
  }
}
