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
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.NoSuchElementException;

/**
 * Tests for SourceFactory.
 */
@RunWith(JUnit4.class)
public class SourceFactoryTest {
  static class TestSourceFactory {
    public static TestSource create(PipelineOptions options,
                                    CloudObject o,
                                    Coder<Integer> coder,
                                    ExecutionContext executionContext) {
      return new TestSource();
    }
  }

  static class TestSource extends Source<Integer> {
    @Override
    public SourceIterator<Integer> iterator() {
      return new TestSourceIterator();
    }

    /** A source iterator that produces no values, for testing. */
    class TestSourceIterator extends AbstractSourceIterator<Integer> {
      @Override
      public boolean hasNext() { return false; }

      @Override
      public Integer next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {
      }
    }
  }

  @Test
  public void testCreatePredefinedSource() throws Exception {
    CloudObject spec = CloudObject.forClassName("TextSource");
    addString(spec, "filename", "/path/to/file.txt");

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("StringUtf8Coder"));

    Source<?> source = SourceFactory.create(PipelineOptionsFactory.create(),
                                            cloudSource,
                                            new BatchModeExecutionContext());
    Assert.assertThat(source, new IsInstanceOf(TextSource.class));
  }

  @Test
  public void testCreateUserDefinedSource() throws Exception {
    CloudObject spec = CloudObject.forClass(TestSourceFactory.class);

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("BigEndianIntegerCoder"));

    Source<?> source = SourceFactory.create(PipelineOptionsFactory.create(),
                                            cloudSource,
                                            new BatchModeExecutionContext());
    Assert.assertThat(source, new IsInstanceOf(TestSource.class));
  }

  @Test
  public void testCreateUnknownSource() throws Exception {
    CloudObject spec = CloudObject.forClassName("UnknownSource");
    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("StringUtf8Coder"));
    try {
      SourceFactory.create(PipelineOptionsFactory.create(),
                           cloudSource,
                           new BatchModeExecutionContext());
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        CoreMatchers.containsString(
                            "unable to create a source"));
    }
  }
}
