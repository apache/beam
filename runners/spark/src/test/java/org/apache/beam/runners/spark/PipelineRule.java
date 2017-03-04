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
package org.apache.beam.runners.spark;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A {@link org.junit.Rule} to provide a {@link Pipeline} instance for Spark runner tests.
 */
public class PipelineRule implements TestRule {

  private final SparkPipelineRule delegate;
  private final RuleChain chain;

  private PipelineRule(SparkPipelineRule delegate) {
    TestName testName = new TestName();
    this.delegate = delegate;
    this.delegate.setTestName(testName);
    this.chain = RuleChain.outerRule(testName).around(this.delegate);
  }

  public static PipelineRule streaming() {
    return new PipelineRule(new SparkStreamingPipelineRule());
  }

  public static PipelineRule batch() {
    return new PipelineRule(new SparkPipelineRule());
  }

  public Duration batchDuration() {
    return Duration.millis(delegate.options.getBatchIntervalMillis());
  }

  public TestSparkPipelineOptions getOptions() {
    return delegate.options;
  }

  public Pipeline createPipeline() {
    return Pipeline.create(delegate.options);
  }

  @Override
  public Statement apply(Statement statement, Description description) {
    return chain.apply(statement, description);
  }

  private static class SparkStreamingPipelineRule extends SparkPipelineRule {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected void before() throws Throwable {
      super.before();
      temporaryFolder.create();
      options.setForceStreaming(true);
      options.setCheckpointDir(
          temporaryFolder.newFolder(options.getJobName()).toURI().toURL().toString());
    }

    @Override
    protected void after() {
      temporaryFolder.delete();
    }
  }

  private static class SparkPipelineRule extends ExternalResource {

    protected final TestSparkPipelineOptions options =
        PipelineOptionsFactory.as(TestSparkPipelineOptions.class);

    private TestName testName;

    public void setTestName(TestName testName) {
      this.testName = testName;
    }

    @Override
    protected void before() throws Throwable {
      options.setRunner(TestSparkRunner.class);
      options.setEnableSparkMetricSinks(false);
      options.setJobName(
          testName != null ? testName.getMethodName() : "test-at-" + System.currentTimeMillis());
    }
  }
}
