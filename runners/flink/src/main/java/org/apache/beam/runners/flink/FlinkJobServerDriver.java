package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.beam.artifact.local.LocalFileSystemArtifactStagerService;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingServiceProvider;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.JobService;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Driver program that starts a job server. */
public class FlinkJobServerDriver implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriver.class);

  private static class ServerConfiguration {
    @Option(
        name = "--job-host",
        required = true,
        usage = "The job server host string"
    )
    private String host = "";

    @Option(
        name = "--artifacts-dir",
        usage = "The location to store staged artifact files"
    )
    private String artifactStagingPath = "/tmp/beam-artifact-staging";
  }

  public static void main(String[] args) {
    ServerConfiguration configuration = new ServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace(System.err);
      printUsage(parser);
      return;
    }
    FlinkJobServerDriver driver = fromConfig(configuration);
    driver.run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format(
            "Usage: java %s arguments...", FlinkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static FlinkJobServerDriver fromConfig(ServerConfiguration configuration) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("flink-runner-job-server").setDaemon(true).build();
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(threadFactory));
    ServerFactory serverFactory = ServerFactory.createDefault();
    return create(configuration, executor, serverFactory);
  }

  public static FlinkJobServerDriver create(
      ServerConfiguration configuration,
      ListeningExecutorService executor,
      ServerFactory serverFactory) {
    return new FlinkJobServerDriver(configuration, executor, serverFactory);
  }

  private final ListeningExecutorService executor;
  private final ServerConfiguration configuration;
  private final ServerFactory serverFactory;

  private FlinkJobServerDriver(
      ServerConfiguration configuration,
      ListeningExecutorService executor,
      ServerFactory serverFactory) {
    this.configuration = configuration;
    this.executor = executor;
    this.serverFactory = serverFactory;
  }

  @Override
  public void run() {
    try {
      GrpcFnServer<JobService> server = createJobServer();
      server.getServer().awaitTermination();
    } catch (InterruptedException e) {
      LOG.warn("Job server interrupted", e);
    } catch (Exception e) {
      LOG.warn("Exception during job server creation", e);
    }
  }

  private GrpcFnServer<JobService> createJobServer() throws IOException {
    JobService service = createJobService();
    Endpoints.ApiServiceDescriptor descriptor =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(configuration.host).build();
    return GrpcFnServer.create(service, descriptor, serverFactory);
  }

  private JobService createJobService() {
    ArtifactStagingServiceProvider artifactStagingServiceProvider =
        createArtifactStagingServiceProvider();
    JobInvoker invoker = createJobInvoker();
    return JobService.create(artifactStagingServiceProvider, invoker);
  }

  private ArtifactStagingServiceProvider createArtifactStagingServiceProvider() {
    return jobPreparationId -> {
      Path location = Paths.get(configuration.artifactStagingPath).resolve(jobPreparationId);
      ArtifactStagingService service =
          LocalFileSystemArtifactStagerService.withRootDirectory(location.toFile());
      return GrpcFnServer.allocatePortAndCreateFor(service, serverFactory);
    };
  }

  private JobInvoker createJobInvoker() {
    return FlinkJobInvoker.create(executor);
  }
}
