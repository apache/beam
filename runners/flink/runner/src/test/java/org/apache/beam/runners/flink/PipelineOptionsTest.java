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

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the serialization and deserialization of PipelineOptions.
 */
public class PipelineOptionsTest {

  private interface MyOptions extends FlinkPipelineOptions {
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
    MyOptions deserializedOptions = serializedOptions.getPipelineOptions().as(MyOptions.class);
    assertNotNull(deserializedOptions);
    assertEquals(deserializedOptions, serializedOptions.getPipelineOptions());
    assertEquals(deserializedOptions, serializedOptions.getPipelineOptions());
    assertEquals(deserializedOptions, serializedOptions.getPipelineOptions());
  }

}
