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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineRunner;
import org.apache.beam.runners.kafka.streams.KafkaStreamsRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * One instance of a streaming pipeline, run as an ordinary application, for measuring what happens
 * when instances are added and removed.
 *
 * <p>Run several of these against one Kafka. They share an application id, so Kafka's consumer
 * group divides the work between them, and stopping one hands its share to the others. Each prints
 * how much it is processing once a second, which is what makes a handover visible: the instance
 * that is stopped goes silent, and the others pick its work up.
 *
 * <p>This is an application rather than a test on purpose. The numbers only mean something if the
 * pipeline is doing a realistic amount of work — a grouping over thousands of keys, fed fast enough
 * that every partition has something to do. A pipeline that trickles produces idle partitions, and
 * an idle partition holds a watermark back for reasons that have nothing to do with rescaling.
 *
 * <pre>
 *   docker compose -f runners/kafka-streams/measurement/docker-compose.yml up -d
 *   ./gradlew :runners:kafka-streams:measurement:installDist
 * </pre>
 *
 * <p>Then start two instances, sharing an application id and differing in everything local to the
 * instance. Each needs its own {@code --stateDir}: two instances sharing one directory fail with a
 * {@code LockException}, because Kafka Streams locks the state it keeps on disk.
 *
 * <pre>
 *   BIN=runners/kafka-streams/measurement/build/install/measurement/bin/measurement
 *   $BIN --applicationId=demo --instanceName=one --stateDir=/tmp/ks-one &amp;
 *   $BIN --applicationId=demo --instanceName=two --stateDir=/tmp/ks-two &amp;
 * </pre>
 *
 * <p>The read rate is worth knowing about even though it is defaulted here. Reading far faster than
 * the grouping keeps up with does not produce groups sooner, it produces none at all: at the
 * runner's own default this pipeline emits one window's worth of groups and then stops, while the
 * source goes on reading millions of elements, and at 20000 elements per poll it emits nothing at
 * all. Output stopping altogether rather than falling behind gradually is the thing to watch for
 * when changing {@code --readMaxElementsPerPoll}.
 *
 * <p>To watch a handover, kill the instance that is reading — {@code elements_read} in the output
 * says which one that is, since reading concentrates on one instance — and watch {@code
 * elements_read} on the other. The delay before it starts climbing is dominated by {@code
 * --sessionTimeoutMs}, which is how long the consumer group waits before deciding the instance is
 * gone.
 */
public final class RescalingMeasurement {

  /** What this instance has processed, printed once a second. */
  private static final AtomicLong PROCESSED = new AtomicLong();

  /**
   * Elements read from the source, so a stall before the grouping can be told from one after it.
   */
  private static final AtomicLong READ = new AtomicLong();

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
   * <p>The runner's defaults are meant for a pipeline, not for this. Left alone, the source reads
   * far faster than the grouping keeps up with, and the result is not output arriving late but
   * output stopping: one window's worth of groups is emitted and then nothing, while the source
   * goes on reading millions of elements. That reads as a broken runner rather than as a source
   * being read too fast, which is the wrong thing for a measurement to suggest when someone runs it
   * for the first time. The parallelism is raised for a related reason: a measurement of work
   * moving between instances needs more than the single partition the runner defaults to, since
   * with one partition there is nothing to divide.
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
            + " shuffle has work and no partition sits idle holding a watermark back.")
    @Default.Integer(2_000)
    int getNumKeys();

    void setNumKeys(int numKeys);

    @Description("Window size in milliseconds; how often each key's group is emitted.")
    @Default.Integer(1_000)
    int getWindowMs();

    void setWindowMs(int windowMs);
  }

  /** Spreads elements over many keys so the shuffle divides the work evenly. */
  private static class ToKeyedFn extends DoFn<Long, KV<String, Long>> {
    private final int numKeys;

    ToKeyedFn(int numKeys) {
      this.numKeys = numKeys;
    }

    @ProcessElement
    public void processElement(@Element Long value, OutputReceiver<KV<String, Long>> out) {
      READ.incrementAndGet();
      out.output(KV.of("key-" + (value % numKeys), value));
    }
  }

  /** Counts the groups this instance produced. */
  private static class CountGroupsFn extends DoFn<KV<String, Iterable<Long>>, Void> {
    @ProcessElement
    public void processElement() {
      PROCESSED.incrementAndGet();
    }
  }

  /** Prints throughput once a second, so a handover shows up as a gap and a recovery. */
  private static void reportEverySecond(String instanceName) {
    Thread reporter =
        new Thread(
            () -> {
              long previous = 0;
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  Thread.sleep(1_000L);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                long total = PROCESSED.get();
                System.out.printf(
                    "%d %s groups_per_second=%d groups_total=%d elements_read=%d%n",
                    System.currentTimeMillis(), instanceName, total - previous, total, READ.get());
                previous = total;
              }
            },
            "measurement-reporter");
    reporter.setDaemon(true);
    reporter.start();
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
    // Pipeline.create needs a runner class even though this application never calls pipeline.run()
    // — it builds the pipeline proto and hands it to the runner below itself.
    applyMeasurementDefaults(args, options);
    options.setRunner(KafkaStreamsRunner.class);
    // The user code runs in this same process, so no container or separate worker is needed.
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("read", Read.from(CountingSource.unbounded()))
        .apply("key", ParDo.of(new ToKeyedFn(options.getNumKeys())))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
        .apply("window", Window.into(FixedWindows.of(Duration.millis(options.getWindowMs()))))
        .apply("group", GroupByKey.create())
        .apply("count", ParDo.of(new CountGroupsFn()));

    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(pipeline);
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));

    System.out.printf(
        "starting %s: application=%s keys=%d parallelism=%d window=%dms session_timeout=%dms"
            + " read_per_poll=%d bundle=%d%n",
        options.getInstanceName(),
        options.getApplicationId(),
        options.getNumKeys(),
        options.getInternalParallelism(),
        options.getWindowMs(),
        options.getSessionTimeoutMs(),
        options.getReadMaxElementsPerPoll(),
        options.getMaxBundleSize());
    reportEverySecond(options.getInstanceName());

    // Blocks until the instance is stopped; a streaming pipeline has no end of its own.
    new KafkaStreamsPipelineRunner(options).run(proto, jobInfo);
  }
}
