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
package org.apache.beam.runners.kafka.streams;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsPipelineTranslator;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsTranslationContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes a portable pipeline by translating it to a Kafka Streams {@link Topology}. */
public class KafkaStreamsPipelineRunner implements PortablePipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsPipelineRunner.class);

  private final KafkaStreamsPipelineOptions pipelineOptions;

  public KafkaStreamsPipelineRunner(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
    // Only the options meaningful here are checked, not the whole interface: this runs on the job
    // server, so the client-side options PortablePipelineOptions marks required — jobEndpoint above
    // all — do not apply. Flink's PortablePipelineRunner does not validate here either.
    checkRequiredOption("applicationId", pipelineOptions.getApplicationId());
    checkRequiredOption("bootstrapServers", pipelineOptions.getBootstrapServers());
    // Also the number of watermark reports a shuffle's consumer waits for, so a non-positive value
    // would leave it waiting forever rather than failing.
    if (pipelineOptions.getInternalParallelism() < 1) {
      throw new IllegalArgumentException(
          "--internalParallelism must be at least 1, but was "
              + pipelineOptions.getInternalParallelism());
    }

    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context =
        translator.createTranslationContext(jobInfo, pipelineOptions);
    RunnerApi.Pipeline prepared = translator.prepareForTranslation(pipeline);
    translator.translate(context, prepared);

    Topology topology = context.getTopology();
    // The runner names its own bootstrap and repartition topics, which Kafka Streams treats as
    // user topics and will not create; it refuses to start if a source topic is missing.
    KafkaStreamsTopicManager.createMissingTopics(topology, pipelineOptions);
    LOG.info(
        "Translated pipeline {} into Kafka Streams topology:\n{}",
        jobInfo.jobId(),
        topology.describe());

    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig(jobInfo));
    // Kafka Streams moves the client to ERROR and keeps the exception to itself, which left failed
    // jobs saying only "unknown error". Keep the first failure so run() can rethrow it.
    AtomicReference<@Nullable Throwable> failure = new AtomicReference<>();
    kafkaStreams.setUncaughtExceptionHandler(
        throwable -> {
          failure.compareAndSet(null, throwable);
          LOG.error("Pipeline {} failed", jobInfo.jobId(), throwable);
          // A job with an owner waiting on it, not a service: a failure stops the client.
          return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
    // Before start(): Kafka Streams only accepts a state listener while still in CREATED.
    KafkaStreamsPortablePipelineResult result =
        new KafkaStreamsPortablePipelineResult(
            kafkaStreams,
            context.getMetricsContainerStepMap(),
            // Only once every task is initialized is the registered set complete, so that "all
            // finished" can mean the pipeline is finished.
            context.getTerminationTracker()::started);
    // Kafka Streams has no notion of a finished pipeline, so the runner stops the client once every
    // processor reaches the terminal watermark. Registered before start() so a fast drain is seen.
    context
        .getTerminationTracker()
        .onAllTerminated(
            () -> closeInBackground(kafkaStreams, jobInfo.jobId(), "the pipeline is drained"));
    kafkaStreams.start();
    // The job service reads the result's state once, when this method returns, so returning while
    // the pipeline is still running would leave the job reported as RUNNING for good. Blocking here
    // is what FlinkPipelineRunner does too, by blocking in executor.execute().
    //
    // A bounded pipeline unblocks this by draining: the processors report themselves terminated,
    // the callback above stops the client, and the result's latch is released. A streaming pipeline
    // never reaches the terminal watermark, so this blocks until the job is cancelled, which is the
    // intended behaviour for a job that has no end.
    result.waitUntilFinish();
    if (Thread.currentThread().isInterrupted()) {
      // Cancelled: the job service interrupts this thread, and the invocation future it would
      // otherwise have used to cancel the result has already been cancelled with it. Stop the
      // client so it does not outlive the job — from another thread, since close() waits on the
      // stream threads and the joins it does would throw straight back out of an interrupted one.
      closeInBackground(kafkaStreams, jobInfo.jobId(), "the job was cancelled");
    }
    Throwable thrown = failure.get();
    if (thrown != null) {
      // Thrown rather than returned as a failed result: the job service reads the state of what is
      // returned, but only what is thrown carries a reason the user can act on.
      throw new RuntimeException("Pipeline " + jobInfo.jobId() + " failed", thrown);
    }
    return result;
  }

  /**
   * Stops the Kafka Streams client from a thread of its own.
   *
   * <p>Never called from a thread that {@code close()} itself waits for. When the pipeline drains,
   * that is the task thread which reported the last termination; when the job is cancelled, it is
   * the interrupted invocation thread. In both cases closing inline would either wait on the thread
   * doing the closing or abandon the shutdown part-way.
   */
  private static void closeInBackground(KafkaStreams kafkaStreams, String jobId, String reason) {
    Thread closer =
        new Thread(
            () -> {
              LOG.info("Stopping the Kafka Streams client for job {}: {}", jobId, reason);
              kafkaStreams.close();
            },
            "kafka-streams-runner-shutdown-" + jobId);
    closer.setDaemon(true);
    closer.start();
  }

  private static void checkRequiredOption(String name, @Nullable String value) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing required pipeline option --" + name + " for the Kafka Streams runner");
    }
  }

  // Visible for testing: the session timeout and the heartbeat derived from it.
  Properties streamsConfig(JobInfo jobInfo) {
    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, pipelineOptions.getBootstrapServers());
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, pipelineOptions.getApplicationId());
    props.put(StreamsConfig.STATE_DIR_CONFIG, pipelineOptions.getStateDir());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    // The job id identifies the pipeline, which every instance of it shares, so on its own it does
    // not identify an instance. Kafka Streams names threads, consumers and metrics after the client
    // id, so two workers running the same job would produce logs and JMX metrics that cannot be
    // told
    // apart — in a deployment whose whole point is that you add workers. Keeping the job id as the
    // prefix leaves the pipeline recognizable; the suffix is what makes each worker distinct, and
    // is
    // what Kafka Streams does by default when no client id is set.
    props.put(StreamsConfig.CLIENT_ID_CONFIG, jobInfo.jobId() + "-" + UUID.randomUUID());
    // How quickly a lost instance is noticed, which is the floor on how quickly its work moves
    // elsewhere. The heartbeat must be shorter than the timeout, or a healthy instance would be
    // declared dead between beats; a third is the ratio Kafka's own defaults use. Deriving it
    // rather than exposing it keeps the pair consistent whatever the timeout is set to.
    int sessionTimeoutMs = pipelineOptions.getSessionTimeoutMs();
    props.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), sessionTimeoutMs);
    props.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG),
        Math.max(1, sessionTimeoutMs / 3));
    return props;
  }
}
