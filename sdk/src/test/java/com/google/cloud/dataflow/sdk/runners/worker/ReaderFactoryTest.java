/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Tests for ReaderFactory.
 */
@RunWith(JUnit4.class)
public class ReaderFactoryTest {

  static class TestReaderFactory implements ReaderFactory {
    @Override
    public Reader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext executionContext,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName) {
      return new TestReader();
    }
  }

  static class TestReader extends Reader<Integer> {
    @Override
    public ReaderIterator<Integer> iterator() {
      return new TestReaderIterator();
    }

    /** A source iterator that produces no values, for testing. */
    class TestReaderIterator extends AbstractReaderIterator<Integer> {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Integer next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {}
    }
  }

  static class SingletonTestReaderFactory implements ReaderFactory {
    @Override
    public Reader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext executionContext,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName) {
      return new SingletonTestReader();
    }
  }

  static class SingletonTestReader extends Reader<WindowedValue<String>> {
    @Override
    public SingletonTestReaderIterator iterator() {
      return new SingletonTestReaderIterator();
    }

    /** A source iterator that produces no values, for testing. */
    class SingletonTestReaderIterator extends AbstractReaderIterator<WindowedValue<String>> {
      private boolean seen = false;
      @Override
      public boolean hasNext() {
        return !seen;
      }

      @Override
      public WindowedValue<String> next() {
        if (seen) {
          throw new NoSuchElementException();
        } else {
          seen = true;
          return WindowedValue.valueInGlobalWindow("something");
        }
      }

      @Override
      public void close() {}
    }
  }

  @Test
  public void testCreatePredefinedReader() throws Exception {
    CloudObject spec = CloudObject.forClassName("TextSource");
    addString(spec, "filename", "/path/to/file.txt");

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("StringUtf8Coder"));

    PipelineOptions options = PipelineOptionsFactory.create();
    Reader<?> reader = ReaderFactory.Registry.defaultRegistry().create(
        cloudSource,
        options,
        BatchModeExecutionContext.fromOptions(options),
        null,
        null);
    Assert.assertThat(reader, new IsInstanceOf(TextReader.class));
  }

  @Test
  public void testCreateUserDefinedReader() throws Exception {
    CloudObject spec = CloudObject.forClass(TestReaderFactory.class);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("BigEndianIntegerCoder"));

    PipelineOptions options = PipelineOptionsFactory.create();
    ReaderFactory.Registry registry = ReaderFactory.Registry.defaultRegistry()
        .register(TestReaderFactory.class.getName(), new TestReaderFactory());
    Reader<?> reader = registry.create(
        cloudSource,
        PipelineOptionsFactory.create(),
        BatchModeExecutionContext.fromOptions(options),
        null,
        null);
    Assert.assertThat(reader, new IsInstanceOf(TestReader.class));
  }

  @Test
  public void testCreateUnknownReader() throws Exception {
    CloudObject spec = CloudObject.forClassName("UnknownSource");
    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(makeCloudEncoding("StringUtf8Coder"));
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      ReaderFactory.Registry.defaultRegistry().create(
          cloudSource,
          options,
          BatchModeExecutionContext.fromOptions(options),
          null,
          null);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(), CoreMatchers.containsString("Unable to create a Reader"));
    }
  }
}
