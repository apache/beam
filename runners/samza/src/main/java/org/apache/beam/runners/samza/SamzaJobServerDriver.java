package org.apache.beam.runners.samza;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.runners.fnexecution.jobsubmission.InMemoryJobService;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.Struct;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server. */
public class SamzaJobServerDriver {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobServerDriver.class);

  private final ServerConfiguration config;

  /** Configuration for the jobServer. */
  private static class ServerConfiguration {
    @Option(name = "--job-port", usage = "The job service port. (Default: 11440)")
    private int jobPort = 11440;

    @Option(name = "--control-port", usage = "The FnControl port. (Default: 11441)")
    private int controlPort = 11441;
  }

  private SamzaJobServerDriver(ServerConfiguration config) {
    this.config = config;
  }

  public static void main(String[] args) throws Exception {
    ServerConfiguration configuration = new ServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
      fromConfig(configuration).run();
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments {}", Arrays.asList(args), e);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    } catch (Exception e) {
      LOG.error("Hit exception with SamzaJobServer. Exiting...", e);
      throw e;
    }
  }

  public static SamzaJobServerDriver fromConfig(ServerConfiguration config) {
    return new SamzaJobServerDriver(config);
  }

  private InMemoryJobService createJobService() throws IOException {
    JobInvoker jobInvoker =
        new JobInvoker() {
          @Override
          public JobInvocation invoke(
              RunnerApi.Pipeline pipeline, Struct options, @Nullable String retrievalToken)
              throws IOException {
            SamzaPipelineOptions samzaPipelineOptions =
                PipelineOptionsTranslation.fromProto(options).as(SamzaPipelineOptions.class);
            Map<String, String> overrideConfig =
                samzaPipelineOptions.getConfigOverride() != null
                    ? samzaPipelineOptions.getConfigOverride()
                    : new HashMap<>();
            overrideConfig.put(SamzaRunnerOverrideConfigs.IS_PORTABLE_MODE, String.valueOf(true));
            overrideConfig.put(
                SamzaRunnerOverrideConfigs.FN_CONTROL_PORT, String.valueOf(config.controlPort));
            samzaPipelineOptions.setConfigOverride(overrideConfig);
            return new SamzaJobInvocation(pipeline, samzaPipelineOptions);
          }
        };
    return InMemoryJobService.create(
        null,
        (String session) -> {
          try {
            return BeamFileSystemArtifactStagingService.generateStagingSessionToken(
                session, "/tmp/beam-artifact-staging");
          } catch (Exception exn) {
            throw new RuntimeException(exn);
          }
        },
        stagingSessionToken -> {},
        jobInvoker);
  }

  private void run() throws Exception {
    InMemoryJobService service = createJobService();
    GrpcFnServer<InMemoryJobService> jobServiceGrpcFnServer =
        GrpcFnServer.allocatePortAndCreateFor(
            service, ServerFactory.createWithPortSupplier(() -> config.jobPort));
    LOG.info("JobServer started on {}", jobServiceGrpcFnServer.getApiServiceDescriptor().getUrl());
    try {
      jobServiceGrpcFnServer.getServer().awaitTermination();
    } finally {
      LOG.info("JobServer closing");
      jobServiceGrpcFnServer.close();
    }
  }
}
