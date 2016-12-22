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
package org.apache.beam.sdk.io.exec;

import java.io.Serializable;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test on the ExecIO.
 */
public class ExecIOTest implements Serializable {

  @Category(RunnableOnService.class)
  @Test
  public void readTest() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output =
        pipeline.apply(ExecIO.read().withCommand("java -version"));

    final String javaVersion = Runtime.class.getPackage().getImplementationVersion();

    PAssert.that(output).satisfies(new SerializableFunction<Iterable<String>, Void>() {
      @Override
      public Void apply(Iterable<String> input) {
        for (String commandOutput : input) {
          Assert.assertTrue(commandOutput.contains("java version \"" + javaVersion + "\""));
        }
        return null;
      }
    });

    pipeline.run();
  }

  @Category(RunnableOnService.class)
  @Test
  public void timeoutTest() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    long start = System.currentTimeMillis();

    pipeline.apply(ExecIO.read().withCommand("sleep 30").withTimeout(10));

    pipeline.run();

    long stop = System.currentTimeMillis();

    Duration duration = new Duration(start, stop);
    Assert.assertTrue(duration.isShorterThan(Duration.standardSeconds(30)));
  }

  @Category(RunnableOnService.class)
  @Test
  public void badCommandTest() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    pipeline.apply(ExecIO.read().withCommand("foobar"));

    try {
      pipeline.run();
    } catch (Exception ioe) {
      // we should have an Exception as foobar command doesn't exist
      return;
    }

    Assert.fail("Exception should have been thrown");
  }

  @Category(RunnableOnService.class)
  @Test
  public void execFnTest() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline
        .apply(Create.of("java -version"))
        .apply(ParDo.of(new ExecIO.ExecFn()));

    final String javaVersion = Runtime.class.getPackage().getImplementationVersion();

    PAssert.that(output).satisfies(new SerializableFunction<Iterable<String>, Void>() {
      @Override
      public Void apply(Iterable<String> input) {
        for (String commandOutput : input) {
          Assert.assertTrue(commandOutput.contains("java version \"" + javaVersion + "\""));
        }
        return null;
      }
    });

    pipeline.run();
  }

  @Category(RunnableOnService.class)
  @Test
  public void writeTest() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of("java -version"))
        .apply(ExecIO.write().withWorkingDir("."));
  }

}
