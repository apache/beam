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
package org.apache.beam.runners.apex.translators.utils;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.common.util.FSStorageAgent;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Tests the serialization of PipelineOptions.
 */
public class PipelineOptionsTest {

  public interface MyOptions extends ApexPipelineOptions {
    @Description("Bla bla bla")
    @Default.String("Hello")
    String getTestOption();
    void setTestOption(String value);
  }

  private static class MyOptionsWrapper {
    private MyOptionsWrapper() {
      this(null); // required for Kryo
    }
    private MyOptionsWrapper(ApexPipelineOptions options) {
      this.options = new SerializablePipelineOptions(options);
    }
    @Bind(JavaSerializer.class)
    private final SerializablePipelineOptions options;
  }

  private static MyOptions options;

  private final static String[] args = new String[]{"--testOption=nothing"};

  @BeforeClass
  public static void beforeTest() {
    options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
  }

  @Test
  public void testSerialization() {
    MyOptionsWrapper wrapper = new MyOptionsWrapper(PipelineOptionsTest.options);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FSStorageAgent.store(bos, wrapper);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    MyOptionsWrapper wrapperCopy = (MyOptionsWrapper)FSStorageAgent.retrieve(bis);
    assertNotNull(wrapperCopy.options);
    assertEquals("nothing", wrapperCopy.options.get().as(MyOptions.class).getTestOption());
  }

}
