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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.Source;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ReaderFactory. */
@RunWith(JUnit4.class)
public class ReaderFactoryTest {

  static class TestReaderFactory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      return new TestReader();
    }
  }

  static class TestReader extends NativeReader<Integer> {
    @Override
    public NativeReaderIterator<Integer> iterator() {
      return new TestReaderIterator();
    }

    /** A source iterator that produces no values, for testing. */
    static class TestReaderIterator extends NativeReaderIterator<Integer> {
      @Override
      public boolean start() {
        return false;
      }

      @Override
      public boolean advance() {
        return false;
      }

      @Override
      public Integer getCurrent() {
        throw new NoSuchElementException();
      }
    }
  }

  static class SingletonTestReaderFactory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext) {
      return new SingletonTestReader();
    }
  }

  static class SingletonTestReader extends NativeReader<WindowedValue<String>> {
    @Override
    public SingletonTestReaderIterator iterator() {
      return new SingletonTestReaderIterator();
    }

    /** A source iterator that produces no values, for testing. */
    static class SingletonTestReaderIterator extends NativeReaderIterator<WindowedValue<String>> {
      @Override
      public boolean start() {
        return true;
      }

      @Override
      public boolean advance() throws IOException {
        return false;
      }

      @Override
      public WindowedValue<String> getCurrent() {
        return WindowedValue.valueInGlobalWindow("something");
      }
    }
  }

  @Test
  public void testCreateReader() throws Exception {
    CloudObject spec = CloudObject.forClass(TestReaderFactory.class);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(
        CloudObjects.asCloudObject(BigEndianIntegerCoder.of(), /*sdkComponents=*/ null));

    PipelineOptions options = PipelineOptionsFactory.create();
    ReaderRegistry registry =
        ReaderRegistry.defaultRegistry()
            .register(TestReaderFactory.class.getName(), new TestReaderFactory());
    NativeReader<?> reader =
        registry.create(
            cloudSource,
            PipelineOptionsFactory.create(),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            null);
    Assert.assertThat(reader, new IsInstanceOf(TestReader.class));
  }

  @Test
  public void testCreateUnknownReader() throws Exception {
    CloudObject spec = CloudObject.forClassName("UnknownSource");
    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(CloudObjects.asCloudObject(StringUtf8Coder.of(), /*sdkComponents=*/ null));
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      ReaderRegistry.defaultRegistry()
          .create(
              cloudSource,
              options,
              BatchModeExecutionContext.forTesting(options, "testStage"),
              null);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(), CoreMatchers.containsString("Unable to create a Reader"));
    }
  }
}
