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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkAbstractParDoWrapper;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the serialization and deserialization of PipelineOptions.
 */
public class PipelineOptionsTest {

  public interface MyOptions extends FlinkPipelineOptions {
    @Description("Bla bla bla")
    @Default.String("Hello")
    String getTestOption();
    void setTestOption(String value);
  }

  private static MyOptions options;
  private static SerializedPipelineOptions serializedOptions;

  private final static String[] args = new String[]{"--testOption=nothing"};

  @BeforeClass
  public static void beforeTest() {
    options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
    serializedOptions = new SerializedPipelineOptions(options);
  }

  @Test
  public void testDeserialization() {
    MyOptions deserializedOptions = serializedOptions.getPipelineOptions().as(MyOptions.class);
    assertEquals("nothing", deserializedOptions.getTestOption());
  }

  @Test
  public void testCaching() {
    PipelineOptions deserializedOptions = serializedOptions.getPipelineOptions().as(PipelineOptions.class);
    assertNotNull(deserializedOptions);
    assertTrue(deserializedOptions == serializedOptions.getPipelineOptions());
    assertTrue(deserializedOptions == serializedOptions.getPipelineOptions());
    assertTrue(deserializedOptions == serializedOptions.getPipelineOptions());
  }

  @Test(expected = Exception.class)
  public void testNonNull() {
    new SerializedPipelineOptions(null);
  }

  @Test(expected = Exception.class)
  public void ParDoBaseClassPipelineOptionsNullTest() {
    new TestParDoWrapper(null, WindowingStrategy.globalDefault(), new TestDoFn());
  }

  /**
   * Tests that PipelineOptions are present after serialization
   */
  @Test
  public void ParDoBaseClassPipelineOptionsSerializationTest() throws Exception {
    TestParDoWrapper wrapper =
        new TestParDoWrapper(options, WindowingStrategy.globalDefault(), new TestDoFn());

    final byte[] serialized = SerializationUtils.serialize(wrapper);
    TestParDoWrapper deserialize = (TestParDoWrapper) SerializationUtils.deserialize(serialized);

    // execute once to access options
    deserialize.flatMap(
        WindowedValue.of(
            new Object(),
            Instant.now(),
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING),
        Mockito.mock(Collector.class));

  }


  private static class TestDoFn extends OldDoFn<Object, Object> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Assert.assertNotNull(c.getPipelineOptions());
      Assert.assertEquals(
          options.getTestOption(),
          c.getPipelineOptions().as(MyOptions.class).getTestOption());
    }
  }

  private static class TestParDoWrapper extends FlinkAbstractParDoWrapper {
    public TestParDoWrapper(PipelineOptions options, WindowingStrategy windowingStrategy, OldDoFn doFn) {
      super(options, windowingStrategy, doFn);
    }


    @Override
    public WindowingInternals windowingInternalsHelper(
        WindowedValue inElement,
        Collector outCollector) {
      return null;
    }

    @Override
    public void sideOutputWithTimestampHelper(
        WindowedValue inElement,
        Object output,
        Instant timestamp,
        Collector outCollector,
        TupleTag tag) {}

    @Override
    public void outputWithTimestampHelper(
        WindowedValue inElement,
        Object output,
        Instant timestamp,
        Collector outCollector) {}
  }


}
