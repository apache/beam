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
package org.apache.beam.runners.flink;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.flink.api.common.time.Deadline;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink job entry point to launch a Beam pipeline by executing an external SDK driver program.
 *
 * <p>Designed for non-interactive Flink REST client and container with Beam job server jar and SDK
 * client (for example when using the FlinkK8sOperator). In the future it would be possible to
 * support driver program execution in a separate (sidecar) container by introducing a client
 * environment abstraction similar to how it exists for SDK workers.
 *
 * <p>Using this entry point eliminates the need to build jar files with materialized pipeline
 * protos offline. Allows the driver program to access actual execution environment and services, on
 * par with code executed by SDK workers.
 *
 * <p>The entry point starts the job server and provides the endpoint to the the driver program.
 *
 * <p>The external driver program constructs the Beam pipeline and submits it to the job service.
 *
 * <p>The job service defers execution of the pipeline to the plan environment and returns the
 * "detached" status to the driver program.
 *
 * <p>Upon arrival of the job invocation, the entry point executes the runner, which prepares
 * ("executes") the Flink job through the plan environment.
 *
 * <p>Finally Flink launches the job.
 */
public class FlinkPortableClientEntryPoint {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkPortableClientEntryPoint.class);
  private static final String JOB_ENDPOINT_FLAG = "--job_endpoint";
  private static final Duration JOB_INVOCATION_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration JOB_SERVICE_STARTUP_TIMEOUT = Duration.ofSeconds(30);

  private final String driverCmd;
  private FlinkJobServerDriver jobServer;
  private Thread jobServerThread;
  private DetachedJobInvokerFactory jobInvokerFactory;
  private int jobPort = 0; // pick any free port

  public FlinkPortableClientEntryPoint(String driverCmd) {
    Preconditions.checkState(
        !driverCmd.contains(JOB_ENDPOINT_FLAG),
        "Driver command must not contain " + JOB_ENDPOINT_FLAG);
    this.driverCmd = driverCmd;
  }

  /** Main method to be called standalone or by Flink (CLI or REST API). */
  public static void main(String[] args) throws Exception {
    LOG.info("entry points args: {}", Arrays.asList(args));
    EntryPointConfiguration configuration = parseArgs(args);
    FlinkPortableClientEntryPoint runner =
        new FlinkPortableClientEntryPoint(configuration.driverCmd);
    try {
      runner.startJobService();
      runner.runDriverProgram();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Job %s failed.", configuration.driverCmd), e);
    } finally {
      LOG.info("Stopping job service");
      runner.stopJobService();
    }
    LOG.info("Job submitted successfully.");
  }

  private static class EntryPointConfiguration {
    @Option(
        name = "--driver-cmd",
        required = true,
        usage =
            "Command that launches the Python driver program. "
                + "(The job service endpoint will be appended as --job_endpoint=localhost:<port>.)")
    private String driverCmd;
  }

  private static EntryPointConfiguration parseArgs(String[] args) {
    EntryPointConfiguration configuration = new EntryPointConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      parser.printUsage(System.err);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }

  private void startJobService() throws Exception {
    jobInvokerFactory = new DetachedJobInvokerFactory();
    jobServer =
        FlinkJobServerDriver.fromConfig(
            FlinkJobServerDriver.parseArgs(
                new String[] {"--job-port=" + jobPort, "--artifact-port=0", "--expansion-port=0"}),
            jobInvokerFactory);
    jobServerThread = new Thread(jobServer);
    jobServerThread.start();

    Deadline deadline = Deadline.fromNow(JOB_SERVICE_STARTUP_TIMEOUT);
    while (jobServer.getJobServerUrl() == null && deadline.hasTimeLeft()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException interruptEx) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptEx);
      }
    }

    if (!jobServerThread.isAlive()) {
      throw new IllegalStateException("Job service thread is not alive");
    }

    if (jobServer.getJobServerUrl() == null) {
      String msg = String.format("Timeout of %s waiting for job service to start.", deadline);
      throw new TimeoutException(msg);
    }
  }

  private void runDriverProgram() throws Exception {
    ProcessManager processManager = ProcessManager.create();
    String executable = "bash";
    List<String> args =
        ImmutableList.of(
            "-c",
            String.format("%s %s=%s", driverCmd, JOB_ENDPOINT_FLAG, jobServer.getJobServerUrl()));
    String processId = "client1";
    File outputFile = File.createTempFile("beam-driver-program", ".log");

    try {
      final ProcessManager.RunningProcess driverProcess =
          processManager.startProcess(processId, executable, args, System.getenv(), outputFile);
      driverProcess.isAliveOrThrow();
      LOG.info("Started driver program");

      // await effect of the driver program submitting the job
      jobInvokerFactory.executeDetachedJob();
    } catch (Exception e) {
      try {
        processManager.stopProcess(processId);
      } catch (Exception processKillException) {
        e.addSuppressed(processKillException);
      }
      byte[] output = Files.readAllBytes(outputFile.toPath());
      String msg =
          String.format(
              "Failed to start job with driver program: %s %s output: %s",
              executable, args, new String(output, Charset.defaultCharset()));
      throw new RuntimeException(msg, e);
    }
  }

  private void stopJobService() throws InterruptedException {
    if (jobServer != null) {
      jobServer.stop();
    }
    if (jobServerThread != null) {
      jobServerThread.interrupt();
      jobServerThread.join();
    }
  }

  private class DetachedJobInvokerFactory implements FlinkJobServerDriver.JobInvokerFactory {

    private CountDownLatch latch = new CountDownLatch(1);
    private volatile PortablePipelineRunner actualPipelineRunner;
    private volatile RunnerApi.Pipeline pipeline;
    private volatile JobInfo jobInfo;

    private PortablePipelineRunner handoverPipelineRunner =
        new PortablePipelineRunner() {
          @Override
          public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
            DetachedJobInvokerFactory.this.pipeline = pipeline;
            DetachedJobInvokerFactory.this.jobInfo = jobInfo;
            LOG.info("Pipeline execution handover for {}", jobInfo.jobId());
            latch.countDown();
            return new FlinkPortableRunnerResult.Detached();
          }
        };

    @Override
    public JobInvoker create() {
      return new FlinkJobInvoker(
          (FlinkJobServerDriver.FlinkServerConfiguration) jobServer.configuration) {
        @Override
        protected JobInvocation createJobInvocation(
            String invocationId,
            String retrievalToken,
            ListeningExecutorService executorService,
            RunnerApi.Pipeline pipeline,
            FlinkPipelineOptions flinkOptions,
            PortablePipelineRunner pipelineRunner) {
          // replace pipeline runner to handover execution
          actualPipelineRunner = pipelineRunner;
          return super.createJobInvocation(
              invocationId,
              retrievalToken,
              executorService,
              pipeline,
              flinkOptions,
              handoverPipelineRunner);
        }
      };
    }

    private void executeDetachedJob() throws Exception {
      long timeoutSeconds = JOB_INVOCATION_TIMEOUT.getSeconds();
      if (latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
        actualPipelineRunner.run(pipeline, jobInfo);
      } else {
        throw new TimeoutException(
            String.format("Timeout of %s seconds waiting for job submission.", timeoutSeconds));
      }
    }
  }
}
