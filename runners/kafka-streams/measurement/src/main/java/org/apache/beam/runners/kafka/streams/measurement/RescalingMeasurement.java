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
package org.apache.beam.runners.kafka.streams.measurement;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineRunner;
import org.apache.beam.runners.kafka.streams.KafkaStreamsRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * One instance of a streaming pipeline, run as an ordinary application, for measuring what happens
 * when instances come and go.
 *
 * <p>Run several against one Kafka. They share an application id, so the consumer group divides the
 * work between them and stopping one hands its share to the others. It is an application rather
 * than a test because the numbers only mean something under a realistic load: a grouping over
 * thousands of keys, fed fast enough that no partition sits idle holding a watermark back.
 *
 * <p>The source runs at a fixed rate over a fixed key space, so a complete window is known before
 * the run starts — one line per key, the same count on each — which is what makes a shortfall
 * legible as one.
 *
 * <pre>
 *   docker compose -f runners/kafka-streams/measurement/docker-compose.yml up -d
 *   ./gradlew -Pwith-kafka-streams-runner :runners:kafka-streams:measurement:installDist
 *
 *   BIN=runners/kafka-streams/measurement/build/install/measurement/bin/measurement
 *   $BIN --applicationId=demo --instanceName=one --stateDir=/tmp/ks-one &amp;
 *   $BIN --applicationId=demo --instanceName=two --stateDir=/tmp/ks-two &amp;
 * </pre>
 *
 * <p>Each instance needs its own {@code --stateDir}; sharing one fails with a {@code
 * LockException}. Output is one line per key per window, counted by the pipeline itself rather than
 * beside it, so the tally does not depend on how many instances are running:
 *
 * <pre>
 *   &lt;millis&gt; &lt;instance&gt; window_end=&lt;millis&gt; key=&lt;key&gt; count=&lt;n&gt; skew_ms=&lt;n&gt;
 * </pre>
 *
 * <p>{@code skew_ms} is the gap between the window's event time and the wall clock when it came
 * out. A pipeline that cannot keep up should report its groups later and later while still
 * reporting all of them, so climbing skew with complete windows is congestion and missing groups
 * are something else. To watch a handover, kill one instance and watch the other; the delay before
 * it reports the dead instance's share is dominated by {@code --sessionTimeoutMs}.
 */

public final class RescalingMeasurement {

  private RescalingMeasurement() {}

  /** Whether the command line mentioned an option, so that a default is not applied over it. */
  private static boolean given(String[] args, String name) {
    for (String arg : args) {
      if (arg.equals("--" + name) || arg.startsWith("--" + name + "=")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Applies the defaults this measurement needs, where they differ from the runner's own.
   *
   * <p>The runner's defaults are meant for a pipeline, not for this. Left alone, the source is read
   * in large enough turns that the read starves the rest of the topology and no groups come out at
   * all. The parallelism is raised for a related reason: a measurement of work moving between
   * instances needs more than the single partition the runner defaults to, since with one partition
   * there is nothing to divide.
   */
  private static void applyMeasurementDefaults(String[] args, MeasurementOptions options) {
    if (!given(args, "readMaxElementsPerPoll")) {
      options.setReadMaxElementsPerPoll(200);
    }
    if (!given(args, "internalParallelism")) {
      options.setInternalParallelism(3);
    }
  }

  /** Options of the measurement itself, on top of the runner's own. */
  public interface MeasurementOptions extends KafkaStreamsPipelineOptions {

    @Description("Name for this instance in the output, so several can be told apart.")
    @Default.String("instance")
    String getInstanceName();

    void setInstanceName(String instanceName);

    @Description(
        "How many distinct keys the grouping runs over. Thousands, so that every partition of the"
            + " shuffle has work and no partition sits idle holding a watermark back. With a"
            + " window long enough to contain them all, this is also how many groups a complete"
            + " window has.")
    @Default.Integer(2_000)
    int getNumKeys();

    void setNumKeys(int numKeys);

    @Description(
        "How many elements the source produces per second. Fixed rather than as-fast-as-possible so"
            + " that a window's contents are known in advance and a shortfall is visible.")
    @Default.Integer(20_000)
    int getElementsPerSecond();

    void setElementsPerSecond(int elementsPerSecond);

    @Description("Window size in milliseconds; how often the groups are counted and reported.")
    @Default.Integer(1_000)
    int getWindowMs();

    void setWindowMs(int windowMs);
  }

  /**
   * Logs each group the pipeline produces, with how far behind the wall clock its window was.
   *
   * <p>One line per key per window. With a fixed rate over a fixed key space every window holds the
   * same groups, so counting the lines for a window says whether the window was complete, and no
   * counter has to be kept anywhere for that to be true — the count is the pipeline's own output
   * rather than a tally maintained beside it, which is what makes it independent of how many
   * instances are running.
   *
   * <p>The skew is the point of the timestamp. A pipeline that cannot keep up should report its
   * groups later and later rather than stop reporting them, so a skew that climbs while the groups
   * stay complete is the pipeline falling behind, and groups going missing is something else.
   */
  private static class ReportGroupFn extends DoFn<KV<String, Long>, Void> {
    private final String instanceName;

    ReportGroupFn(String instanceName) {
      this.instanceName = instanceName;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Long> group, BoundedWindow window) {
      long windowEnd = window.maxTimestamp().getMillis();
      long now = System.currentTimeMillis();
      System.out.printf(
          "%d %s window_end=%d key=%s count=%d skew_ms=%d%n",
          now, instanceName, windowEnd, group.getKey(), group.getValue(), now - windowEnd);
    }
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(MeasurementOptions.class);
    // Deliberately not withValidation(): that enforces the options a pipeline needs when it is
    // submitted to a job server, and --jobEndpoint above all, which means nothing here because this
    // application runs the pipeline itself.
    MeasurementOptions options = PipelineOptionsFactory.fromArgs(args).as(MeasurementOptions.class);
    if (options.getApplicationId() == null || options.getApplicationId().isEmpty()) {
      throw new IllegalArgumentException(
          "--applicationId is required, and every instance of one measurement must share it: it is"
              + " what puts them in the same consumer group and so divides the work between them.");
    }
    applyMeasurementDefaults(args, options);
    // Pipeline.create needs a runner class even though this application never calls pipeline.run()
    // — it builds the pipeline proto and hands it to the runner below itself.
    options.setRunner(KafkaStreamsRunner.class);
    // The user code runs in this same process, so no container or separate worker is needed.
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    // A window holds every key as long as it is long enough for the rate to reach them all; below
    // that the source has not got round to each key once and the window is short by construction.
    long elementsPerWindow = (long) options.getElementsPerSecond() * options.getWindowMs() / 1_000L;
    long expectedGroups = Math.min(options.getNumKeys(), elementsPerWindow);

    int numKeys = options.getNumKeys();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            "read",
            GenerateSequence.from(0)
                .withRate(options.getElementsPerSecond(), Duration.standardSeconds(1)))
        .apply(
            "key",
            // numKeys is read here rather than inside the lambda: reaching for it through options
            // would capture the PipelineOptions in the transform, which cannot be serialized.
            MapElements.into(TypeDescriptors.strings()).via((Long n) -> "key-" + (n % numKeys)))
        .apply("window", Window.into(FixedWindows.of(Duration.millis(options.getWindowMs()))))
        .apply("countPerKey", Count.perElement())
        .apply("report", ParDo.of(new ReportGroupFn(options.getInstanceName())));

    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(pipeline);
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));

    System.out.printf(
        "starting %s: application=%s keys=%d rate=%d/s parallelism=%d window=%dms"
            + " session_timeout=%dms read_per_poll=%d bundle=%d expected_groups_per_window=%d%n",
        options.getInstanceName(),
        options.getApplicationId(),
        options.getNumKeys(),
        options.getElementsPerSecond(),
        options.getInternalParallelism(),
        options.getWindowMs(),
        options.getSessionTimeoutMs(),
        options.getReadMaxElementsPerPoll(),
        options.getMaxBundleSize(),
        expectedGroups);

    // Blocks until the instance is stopped; a streaming pipeline has no end of its own.
    new KafkaStreamsPipelineRunner(options).run(proto, jobInfo);
  }
}
