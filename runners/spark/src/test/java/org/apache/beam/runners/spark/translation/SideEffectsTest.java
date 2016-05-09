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

package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.net.URI;

/**
 * Side effects test.
 */
public class SideEffectsTest implements Serializable {

  static class UserException extends RuntimeException {
  }

  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    pipeline.apply(Create.of("a")).apply(ParDo.of(new DoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        throw new UserException();
      }
    }));

    try {
      pipeline.run();
      fail("Run should thrown an exception");
    } catch (RuntimeException e) {
      assertNotNull(e.getCause());

      // TODO: remove the version check (and the setup and teardown methods) when we no
      // longer support Spark 1.3 or 1.4
      String version = SparkContextFactory.getSparkContext(options.getSparkMaster(),
          options.getAppName()).version();
      if (!version.startsWith("1.3.") && !version.startsWith("1.4.")) {
        assertTrue(e.getCause() instanceof UserException);
      }
    }
  }

  @Before
  public void setup() {
    System.setProperty(SparkContextFactory.TEST_REUSE_SPARK_CONTEXT, "true");
  }

  @After
  public void teardown() {
    System.setProperty(SparkContextFactory.TEST_REUSE_SPARK_CONTEXT, "false");
  }
}
