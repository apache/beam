package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.JobPreparation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job Invoker for the {@link FlinkRunner}.
 */
public class FlinkJobInvoker implements JobInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvoker.class);

  public static FlinkJobInvoker create(ListeningExecutorService executorService) {
    return new FlinkJobInvoker(executorService);
  }

  private final ListeningExecutorService executorService;

  private FlinkJobInvoker(ListeningExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public JobInvocation invoke(JobPreparation preparation, @Nullable String artifactToken)
      throws IOException {
    LOG.debug("Invoking job preparation {}", preparation.id());
    String invocationId =
        String.format("%s_%d", preparation.id(), ThreadLocalRandom.current().nextInt());
    // TODO: How to make Java/Python agree on names of keys and their values?
    LOG.trace("Parsing pipeline options");
    FlinkPipelineOptions options = PipelineOptionsTranslation.fromProto(preparation.options())
        .as(FlinkPipelineOptions.class);

    // Set Flink Master to [auto] if no option was specified.
    if (options.getFlinkMaster() == null) {
      options.setFlinkMaster("[auto]");
    }

    options.setRunner(null);

    ArtifactSource artifactSource = preparation.stagingService().getService().createAccessor();
    return FlinkJobInvocation.create(
        invocationId,
        executorService,
        preparation.pipeline(),
        options,
        artifactSource);
  }
}
