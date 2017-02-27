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

  private final TestName testName = new TestName();

  private final SparkPipelineRule delegate;
  private final RuleChain chain;

  private PipelineRule() {
    this.delegate = new SparkPipelineRule(testName);
    this.chain = RuleChain.outerRule(testName).around(this.delegate);
  }

  private PipelineRule(Duration forcedTimeout) {
    this.delegate = new SparkStreamingPipelineRule(forcedTimeout, testName);
    this.chain = RuleChain.outerRule(testName).around(this.delegate);
  }

  public static PipelineRule streaming() {
    return new PipelineRule(Duration.standardSeconds(5));
  }

  public static PipelineRule batch() {
    return new PipelineRule();
  }

  public Duration batchDuration() {
    return Duration.millis(delegate.options.getBatchIntervalMillis());
  }

  public SparkPipelineOptions getOptions() {
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
    private final Duration forcedTimeout;

    SparkStreamingPipelineRule(Duration forcedTimeout, TestName testName) {
      super(testName);
      this.forcedTimeout = forcedTimeout;
    }

    @Override
    protected void before() throws Throwable {
      super.before();
      temporaryFolder.create();
      options.setForceStreaming(true);
      options.setTestTimeoutSeconds(forcedTimeout.getStandardSeconds());
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

    private final TestName testName;

    private SparkPipelineRule(TestName testName) {
      this.testName = testName;
    }

    @Override
    protected void before() throws Throwable {
      options.setRunner(TestSparkRunner.class);
      options.setEnableSparkMetricSinks(false);
      options.setJobName(testName.getMethodName());
    }
  }
}
