/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.Joiner;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.JobSpecification;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowAggregatorTransforms;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.DataflowReleaseInfo;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to the Dataflow representation
 * using the {@link DataflowPipelineTranslator} and then submitting
 * them to a Dataflow service for execution.
 */
public class DataflowPipelineRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineRunner.class);

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /** Client for the Dataflow service. This is used to actually submit jobs. */
  private final Dataflow dataflowClient;

  /** Translator for this DataflowPipelineRunner, based on options. */
  private final DataflowPipelineTranslator translator;

  /** A set of user defined functions to invoke at different points in execution. */
  private DataflowPipelineRunnerHooks hooks;

  // Environment version information
  private static final String ENVIRONMENT_MAJOR_VERSION = "2";

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties that configure the runner.
   * @return The newly created runner.
   */
  public static DataflowPipelineRunner fromOptions(PipelineOptions options) {
    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerStandardIOFactories(options);

    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (dataflowOptions.getProject() == null) {
      missing.add("project");
    }
    if (dataflowOptions.getAppName() == null) {
      missing.add("appName");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required values: " + Joiner.on(',').join(missing));
    }

    PathValidator validator = dataflowOptions.getPathValidator();
    Preconditions.checkArgument(!(Strings.isNullOrEmpty(dataflowOptions.getTempLocation())
        && Strings.isNullOrEmpty(dataflowOptions.getStagingLocation())),
        "Missing required value: at least one of tempLocation or stagingLocation must be set.");
    if (dataflowOptions.getStagingLocation() != null) {
      validator.verifyPath(dataflowOptions.getStagingLocation());
    }
    if (dataflowOptions.getTempLocation() != null) {
      validator.verifyPath(dataflowOptions.getTempLocation());
    }
    if (Strings.isNullOrEmpty(dataflowOptions.getTempLocation())) {
      dataflowOptions.setTempLocation(dataflowOptions.getStagingLocation());
    } else if (Strings.isNullOrEmpty(dataflowOptions.getStagingLocation())) {
      try {
        dataflowOptions.setStagingLocation(
            IOChannelUtils.resolve(dataflowOptions.getTempLocation(), "staging"));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to resolve PipelineOptions.stagingLocation "
            + "from PipelineOptions.tempLocation. Please set the staging location explicitly.", e);
      }
    }

    if (dataflowOptions.getFilesToStage() == null) {
      dataflowOptions.setFilesToStage(detectClassPathResourcesToStage(
          DataflowPipelineRunner.class.getClassLoader()));
      LOG.info("PipelineOptions.filesToStage was not specified. "
          + "Defaulting to files from the classpath: will stage {} files. "
          + "Enable logging at DEBUG level to see which files will be staged.",
          dataflowOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", dataflowOptions.getFilesToStage());
    }

    // Verify jobName according to service requirements.
    String jobName = dataflowOptions.getJobName().toLowerCase();
    Preconditions.checkArgument(
        jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"),
        "JobName invalid; the name must consist of only the characters "
            + "[-a-z0-9], starting with a letter and ending with a letter "
            + "or number");

    return new DataflowPipelineRunner(dataflowOptions);
  }

  private DataflowPipelineRunner(DataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = options.getDataflowClient();
    this.translator = DataflowPipelineTranslator.fromOptions(options);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (Combine.GroupedValues.class.equals(transform.getClass())
        || GroupByKey.class.equals(transform.getClass())) {
      PCollection<?> pc = (PCollection<?>) input;
      // TODO: Redundant with translator registration?
      @SuppressWarnings("unchecked")
      OutputT outputT = (OutputT) PCollection.createPrimitiveOutputInternal(
          pc.getPipeline(),
          pc.getWindowingStrategy(),
          pc.isBounded());
      return outputT;
    } else if (Create.Values.class.equals(transform.getClass())) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      OutputT output = (OutputT)
          ((Create.Values) transform).applyHelper(input, options.isStreaming());
      return output;
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    LOG.info("Executing pipeline on the Dataflow Service, which will have billing implications "
        + "related to Google Compute Engine usage and other Google Cloud Services.");

    List<DataflowPackage> packages = options.getStager().stageFiles();
    JobSpecification jobSpecification = translator.translate(pipeline, packages);
    Job newJob = jobSpecification.getJob();

    // Set a unique client_request_id in the CreateJob request.
    // This is used to ensure idempotence of job creation across retried
    // attempts to create a job. Specifically, if the service returns a job with
    // a different client_request_id, it means the returned one is a different
    // job previously created with the same job name, and that the job creation
    // has been effectively rejected. The SDK should return
    // Error::Already_Exists to user in that case.
    int randomNum = new Random().nextInt(9000) + 1000;
    String requestId = DateTimeFormat.forPattern("YYYYMMddHHmmssmmm").withZone(DateTimeZone.UTC)
        .print(DateTimeUtils.currentTimeMillis()) + "_" + randomNum;
    newJob.setClientRequestId(requestId);

    String version = DataflowReleaseInfo.getReleaseInfo().getVersion();
    System.out.println("Dataflow SDK version: " + version);

    newJob.getEnvironment().setUserAgent(DataflowReleaseInfo.getReleaseInfo());
    // The Dataflow Service may write to the temporary directory directly, so
    // must be verified.
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    if (!Strings.isNullOrEmpty(options.getTempLocation())) {
      newJob.getEnvironment().setTempStoragePrefix(
          dataflowOptions.getPathValidator().verifyPath(options.getTempLocation()));
    }
    newJob.getEnvironment().setDataset(options.getTempDatasetId());
    newJob.getEnvironment().setClusterManagerApiService(
        options.getClusterManagerApi().getApiServiceName());
    newJob.getEnvironment().setExperiments(options.getExperiments());

    // Requirements about the service.
    Map<String, Object> environmentVersion = new HashMap<>();
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, ENVIRONMENT_MAJOR_VERSION);
    newJob.getEnvironment().setVersion(environmentVersion);
    // Default jobType is DATA_PARALLEL, which is for java batch.
    String jobType = "DATA_PARALLEL";

    if (options.isStreaming()) {
      jobType = "STREAMING";
    }
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);

    if (hooks != null) {
      hooks.modifyEnvironmentBeforeSubmission(newJob.getEnvironment());
    }

    if (!Strings.isNullOrEmpty(options.getDataflowJobFile())) {
      try (PrintWriter printWriter = new PrintWriter(
          new File(options.getDataflowJobFile()))) {
        String workSpecJson = DataflowPipelineTranslator.jobToString(newJob);
        printWriter.print(workSpecJson);
        LOG.info("Printed workflow specification to {}", options.getDataflowJobFile());
      } catch (IllegalStateException ex) {
        LOG.warn("Cannot translate workflow spec to json for debug.");
      } catch (FileNotFoundException ex) {
        LOG.warn("Cannot create workflow spec output file.");
      }
    }

    String reloadJobId = null;
    if (options.getReload()) {
      reloadJobId = getJobIdFromName(options.getJobName());
    }
    Job jobResult;
    try {
      Dataflow.Projects.Jobs.Create createRequest =
          dataflowClient.projects().jobs()
          .create(options.getProject(), newJob);
      if (reloadJobId != null) {
        createRequest.setReplaceJobId(reloadJobId);
      }
      jobResult = createRequest.execute();
    } catch (GoogleJsonResponseException e) {
        throw new RuntimeException("Failed to create a workflow job: "
            + (e.getDetails() != null ? e.getDetails().getMessage() : e), e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a workflow job", e);
    }
    // If the service returned client request id, the SDK needs to compare it
    // with the original id generated in the request, if they are not the same
    // (i.e., the returned job is not created by this request), throw
    // Error::Already_Exists.
    if (jobResult.getClientRequestId() != null && !jobResult.getClientRequestId().isEmpty()
        && !jobResult.getClientRequestId().equals(requestId)) {
      throw new RuntimeException("The job you are trying to create with name " + newJob.getName()
          + " already exists and is active in system with job id: " + jobResult.getId()
          + ". If you want to submit a new job in parallel, try again with a different name.");
    }

    LOG.info("To access the Dataflow monitoring console, please navigate to {}",
        MonitoringUtil.getJobMonitoringPageURL(options.getProject(), jobResult.getId()));
    System.out.println("Submitted job: " + jobResult.getId());

    boolean usingCustomApiRootUrl =
        !DataflowPipelineDebugOptions.DEFAULT_API_ROOT.equals(dataflowOptions.getApiRootUrl());
    final String setApiEndpointCommand =
        (usingCustomApiRootUrl
         ? MonitoringUtil.getEndpointOverridePrefixCommand(dataflowOptions.getApiRootUrl())
         : "");
    LOG.info("To cancel the job using the 'gcloud' tool, run:\n> {}{}",
        setApiEndpointCommand,
        MonitoringUtil.getGcloudCancelCommand(options.getProject(), jobResult.getId()));

    // Obtain all of the extractors from the PTransforms used in the pipeline so the
    // DataflowPipelineJob has access to them.
    AggregatorPipelineExtractor aggregatorExtractor = new AggregatorPipelineExtractor(pipeline);
    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        aggregatorExtractor.getAggregatorSteps();

    DataflowAggregatorTransforms aggregatorTransforms =
        new DataflowAggregatorTransforms(aggregatorSteps, jobSpecification.getStepNames());

    // Use a raw client for post-launch monitoring, as status calls may fail
    // regularly and need not be retried automatically.
    DataflowPipelineJob dataflowPipelineJob =
        new DataflowPipelineJob(options.getProject(), jobResult.getId(),
            Transport.newRawDataflowClient(options).build(), aggregatorTransforms);

    return dataflowPipelineJob;
  }

  /**
   * Returns the DataflowPipelineTranslator associated with this object.
   */
  public DataflowPipelineTranslator getTranslator() {
    return translator;
  }

  /**
   * Sets callbacks to invoke during execution see {@code DataflowPipelineRunnerHooks}.
   */
  @Experimental
  public void setHooks(DataflowPipelineRunnerHooks hooks) {
    this.hooks = hooks;
  }


  /////////////////////////////////////////////////////////////////////////////

  @Override
  public String toString() {
    return "DataflowPipelineRunner#" + options.getJobName();
  }

  /**
   * Attempts to detect all the resources the class loader has access to. This does not recurse
   * to class loader parents stopping it from pulling in resources from the system class loader.
   *
   * @param classLoader The URLClassLoader to use to detect resources to stage.
   * @throws IllegalArgumentException  If either the class loader is not a URLClassLoader or one
   * of the resources the class loader exposes is not a file resource.
   * @return A list of absolute paths to the resources the class loader uses.
   */
  protected static List<String> detectClassPathResourcesToStage(ClassLoader classLoader) {
    if (!(classLoader instanceof URLClassLoader)) {
      String message = String.format("Unable to use ClassLoader to detect classpath elements. "
          + "Current ClassLoader is %s, only URLClassLoaders are supported.", classLoader);
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }

    List<String> files = new ArrayList<>();
    for (URL url : ((URLClassLoader) classLoader).getURLs()) {
      try {
        files.add(new File(url.toURI()).getAbsolutePath());
      } catch (IllegalArgumentException | URISyntaxException e) {
        String message = String.format("Unable to convert url (%s) to file.", url);
        LOG.error(message);
        throw new IllegalArgumentException(message, e);
      }
    }
    return files;
  }

  /**
   * Finds the id for the running job of the given name.
   */
  private String getJobIdFromName(String jobName) {
    try {
      ListJobsResponse listResult;
      String token = null;
      do {
        listResult = dataflowClient.projects().jobs()
            .list(options.getProject())
            .setPageToken(token)
            .execute();
        token = listResult.getNextPageToken();
        for (Job job : listResult.getJobs()) {
          if (job.getName().equals(jobName)
              && MonitoringUtil.toState(job.getCurrentState()).equals(State.RUNNING)) {
            return job.getId();
          }
        }
      } while (token != null);
    } catch (GoogleJsonResponseException e) {
      throw new RuntimeException(
          "Got error while looking up jobs: "
          + (e.getDetails() != null ? e.getDetails().getMessage() : e), e);
    } catch (IOException e) {
      throw new RuntimeException("Got error while looking up jobs: ", e);
    }

    throw new IllegalArgumentException("Could not find running job named " + jobName);
  }
}
