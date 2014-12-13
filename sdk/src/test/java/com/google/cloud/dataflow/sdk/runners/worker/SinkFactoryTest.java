/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for SinkFactory.
 */
@RunWith(JUnit4.class)
public class SinkFactoryTest {
  static class TestSinkFactory {
    public static TestSink create(PipelineOptions options,
                                  CloudObject o,
                                  Coder<Integer> coder,
                                  ExecutionContext executionContext) {
      return new TestSink();
    }
  }

  static class TestSink extends Sink<Integer> {
    @Override
    public SinkWriter<Integer> writer() {
      return new TestSinkWriter();
    }

    /** A sink writer that drops its input values, for testing. */
    class TestSinkWriter implements SinkWriter<Integer> {
      @Override
      public long add(Integer outputElem) {
        return 4;
      }

      @Override
      public void close() {
      }
    }
  }

  @Test
  public void testCreatePredefinedSink() throws Exception {
    CloudObject spec = CloudObject.forClassName("TextSink");
    addString(spec, "filename", "/path/to/file.txt");

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(makeCloudEncoding("StringUtf8Coder"));

    Sink<?> sink = SinkFactory.create(PipelineOptionsFactory.create(),
                                      cloudSink,
                                      new BatchModeExecutionContext());
    Assert.assertThat(sink, new IsInstanceOf(TextSink.class));
  }

  @Test
  public void testCreateUserDefinedSink() throws Exception {
    CloudObject spec = CloudObject.forClass(TestSinkFactory.class);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(makeCloudEncoding("BigEndianIntegerCoder"));

    Sink<?> sink = SinkFactory.create(PipelineOptionsFactory.create(),
                                      cloudSink,
                                      new BatchModeExecutionContext());
    Assert.assertThat(sink, new IsInstanceOf(TestSink.class));
  }

  @Test
  public void testCreateUnknownSink() throws Exception {
    CloudObject spec = CloudObject.forClassName("UnknownSink");
    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(makeCloudEncoding("StringUtf8Coder"));
    try {
      SinkFactory.create(PipelineOptionsFactory.create(),
                         cloudSink,
                         new BatchModeExecutionContext());
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        CoreMatchers.containsString(
                            "unable to create a sink"));
    }
  }
}
