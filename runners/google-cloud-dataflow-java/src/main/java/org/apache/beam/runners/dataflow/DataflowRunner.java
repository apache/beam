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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.clouddebugger.v2.Clouddebugger;
import com.google.api.services.clouddebugger.v2.model.Debuggee;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeRequest;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeResponse;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.DataflowTemplateJob;
import org.apache.beam.runners.dataflow.util.DataflowTransport;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.PubsubUnboundedSink;
import org.apache.beam.sdk.io.PubsubUnboundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.PathValidator;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.ValueWithRecordId;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to the Dataflow representation using the {@link DataflowPipelineTranslator} and then submitting
 * them to a Dataflow service for execution.
 *
 * <h3>Permissions</h3>
 *
 * <p>When reading from a Dataflow source or writing to a Dataflow sink using
 * {@code DataflowRunner}, the Google cloudservices account and the Google compute engine service
 * account of the GCP project running the Dataflow Job will need access to the corresponding
 * source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
public class DataflowRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowRunner.class);

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /** Client for the Dataflow service. This is used to actually submit jobs. */
  private final DataflowClient dataflowClient;

  /** Translator for this DataflowRunner, based on options. */
  private final DataflowPipelineTranslator translator;

  /** Custom transforms implementations. */
  private final Map<Class<?>, Class<?>> overrides;

  /** A set of user defined functions to invoke at different points in execution. */
  private DataflowRunnerHooks hooks;

  // The limit of CreateJob request size.
  private static final int CREATE_JOB_REQUEST_LIMIT_BYTES = 10 * 1024 * 1024;

  @VisibleForTesting
  static final int GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT = 1024 * 1024;

  private final Set<PCollection<?>> pcollectionsRequiringIndexedFormat;

  /**
   * Project IDs must contain lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic validation of project IDs.
   */
  public static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]+[a-z0-9]";

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties that configure the runner.
   * @return The newly created runner.
   */
  public static DataflowRunner fromOptions(PipelineOptions options) {
    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerIOFactoriesAllowOverride(options);

    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (dataflowOptions.getAppName() == null) {
      missing.add("appName");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required values: " + Joiner.on(',').join(missing));
    }

    PathValidator validator = dataflowOptions.getPathValidator();
    String gcpTempLocation;
    try {
      gcpTempLocation = dataflowOptions.getGcpTempLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException("DataflowRunner requires gcpTempLocation, "
          + "but failed to retrieve a value from PipelineOptions", e);
    }
    validator.validateOutputFilePrefixSupported(gcpTempLocation);

    String stagingLocation;
    try {
      stagingLocation = dataflowOptions.getStagingLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException("DataflowRunner requires stagingLocation, "
          + "but failed to retrieve a value from PipelineOptions", e);
    }
    validator.validateOutputFilePrefixSupported(stagingLocation);

    if (!Strings.isNullOrEmpty(dataflowOptions.getSaveProfilesToGcs())) {
      validator.validateOutputFilePrefixSupported(dataflowOptions.getSaveProfilesToGcs());
    }

    if (dataflowOptions.getFilesToStage() == null) {
      dataflowOptions.setFilesToStage(detectClassPathResourcesToStage(
          DataflowRunner.class.getClassLoader()));
      LOG.info("PipelineOptions.filesToStage was not specified. "
          + "Defaulting to files from the classpath: will stage {} files. "
          + "Enable logging at DEBUG level to see which files will be staged.",
          dataflowOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", dataflowOptions.getFilesToStage());
    }

    // Verify jobName according to service requirements, truncating converting to lowercase if
    // necessary.
    String jobName =
        dataflowOptions
            .getJobName()
            .toLowerCase();
    checkArgument(
        jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"),
        "JobName invalid; the name must consist of only the characters "
            + "[-a-z0-9], starting with a letter and ending with a letter "
            + "or number");
    if (!jobName.equals(dataflowOptions.getJobName())) {
      LOG.info(
          "PipelineOptions.jobName did not match the service requirements. "
              + "Using {} instead of {}.",
          jobName,
          dataflowOptions.getJobName());
    }
    dataflowOptions.setJobName(jobName);

    // Verify project
    String project = dataflowOptions.getProject();
    if (project.matches("[0-9]*")) {
      throw new IllegalArgumentException("Project ID '" + project
          + "' invalid. Please make sure you specified the Project ID, not project number.");
    } else if (!project.matches(PROJECT_ID_REGEXP)) {
      throw new IllegalArgumentException("Project ID '" + project
          + "' invalid. Please make sure you specified the Project ID, not project description.");
    }

    DataflowPipelineDebugOptions debugOptions =
        dataflowOptions.as(DataflowPipelineDebugOptions.class);
    // Verify the number of worker threads is a valid value
    if (debugOptions.getNumberOfWorkerHarnessThreads() < 0) {
      throw new IllegalArgumentException("Number of worker harness threads '"
          + debugOptions.getNumberOfWorkerHarnessThreads()
          + "' invalid. Please make sure the value is non-negative.");
    }

    if (dataflowOptions.isStreaming() && dataflowOptions.getGcsUploadBufferSizeBytes() == null) {
      dataflowOptions.setGcsUploadBufferSizeBytes(GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT);
    }

    return new DataflowRunner(dataflowOptions);
  }

  @VisibleForTesting protected DataflowRunner(DataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = DataflowClient.create(options);
    this.translator = DataflowPipelineTranslator.fromOptions(options);
    this.pcollectionsRequiringIndexedFormat = new HashSet<>();
    this.ptransformViewsWithNonDeterministicKeyCoders = new HashSet<>();

    ImmutableMap.Builder<Class<?>, Class<?>> builder = ImmutableMap.<Class<?>, Class<?>>builder();
    if (options.isStreaming()) {
      builder.put(Combine.GloballyAsSingletonView.class,
                  StreamingCombineGloballyAsSingletonView.class);
      builder.put(View.AsMap.class, StreamingViewAsMap.class);
      builder.put(View.AsMultimap.class, StreamingViewAsMultimap.class);
      builder.put(View.AsSingleton.class, StreamingViewAsSingleton.class);
      builder.put(View.AsList.class, StreamingViewAsList.class);
      builder.put(View.AsIterable.class, StreamingViewAsIterable.class);
      builder.put(Read.Unbounded.class, StreamingUnboundedRead.class);
      builder.put(Read.Bounded.class, StreamingBoundedRead.class);
      // In streaming mode must use either the custom Pubsub unbounded source/sink or
      // defer to Windmill's built-in implementation.
      builder.put(PubsubIO.Read.PubsubBoundedReader.class, UnsupportedIO.class);
      builder.put(PubsubIO.Write.PubsubBoundedWriter.class, UnsupportedIO.class);
      if (options.getExperiments() == null
          || !options.getExperiments().contains("enable_custom_pubsub_source")) {
        builder.put(PubsubUnboundedSource.class, StreamingPubsubIORead.class);
      }
      if (options.getExperiments() == null
          || !options.getExperiments().contains("enable_custom_pubsub_sink")) {
        builder.put(PubsubUnboundedSink.class, StreamingPubsubIOWrite.class);
      }
    } else {
      builder.put(Read.Unbounded.class, UnsupportedIO.class);
      builder.put(Write.Bound.class, BatchWrite.class);
      // In batch mode must use the custom Pubsub bounded source/sink.
      builder.put(PubsubUnboundedSource.class, UnsupportedIO.class);
      builder.put(PubsubUnboundedSink.class, UnsupportedIO.class);
      if (options.getExperiments() == null
          || !options.getExperiments().contains("disable_ism_side_input")) {
        builder.put(View.AsMap.class, BatchViewOverrides.BatchViewAsMap.class);
        builder.put(View.AsMultimap.class, BatchViewOverrides.BatchViewAsMultimap.class);
        builder.put(View.AsSingleton.class, BatchViewOverrides.BatchViewAsSingleton.class);
        builder.put(View.AsList.class, BatchViewOverrides.BatchViewAsList.class);
        builder.put(View.AsIterable.class, BatchViewOverrides.BatchViewAsIterable.class);
      }
    }
    overrides = builder.build();
  }

  /**
   * Applies the given transform to the input. For transforms with customized definitions
   * for the Dataflow pipeline runner, the application is intercepted and modified here.
   */
  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {

    if (Combine.GroupedValues.class.equals(transform.getClass())) {
      // For both Dataflow runners (streaming and batch), GroupByKey and GroupedValues are
      // primitives. Returning a primitive output instead of the expanded definition
      // signals to the translator that translation is necessary.
      @SuppressWarnings("unchecked")
      PCollection<?> pc = (PCollection<?>) input;
      @SuppressWarnings("unchecked")
      OutputT outputT =
          (OutputT)
              PCollection.createPrimitiveOutputInternal(
                  pc.getPipeline(), pc.getWindowingStrategy(), pc.isBounded());
      return outputT;
    } else if (Flatten.FlattenPCollectionList.class.equals(transform.getClass())
        && ((PCollectionList<?>) input).size() == 0) {
      // This can cause downstream coder inference to be screwy. Most of the time, that won't be
      // hugely impactful, because there will never be any elements encoded with this coder;
      // the issue stems from flattening this with another PCollection.
      return (OutputT)
          Pipeline.applyTransform(
              input.getPipeline().begin(), Create.empty(VoidCoder.of()));
    } else if (overrides.containsKey(transform.getClass())) {
      // It is the responsibility of whoever constructs overrides to ensure this is type safe.
      @SuppressWarnings("unchecked")
      Class<PTransform<InputT, OutputT>> transformClass =
          (Class<PTransform<InputT, OutputT>>) transform.getClass();

      @SuppressWarnings("unchecked")
      Class<PTransform<InputT, OutputT>> customTransformClass =
          (Class<PTransform<InputT, OutputT>>) overrides.get(transform.getClass());

      PTransform<InputT, OutputT> customTransform =
          InstanceBuilder.ofType(customTransformClass)
          .withArg(DataflowRunner.class, this)
          .withArg(transformClass, transform)
          .build();

      return Pipeline.applyTransform(input, customTransform);
    } else {
      return super.apply(transform, input);
    }
  }

  private String debuggerMessage(String projectId, String uniquifier) {
    return String.format("To debug your job, visit Google Cloud Debugger at: "
        + "https://console.developers.google.com/debug?project=%s&dbgee=%s",
        projectId, uniquifier);
  }

  private void maybeRegisterDebuggee(DataflowPipelineOptions options, String uniquifier) {
    if (!options.getEnableCloudDebugger()) {
      return;
    }

    if (options.getDebuggee() != null) {
      throw new RuntimeException("Should not specify the debuggee");
    }

    Clouddebugger debuggerClient = DataflowTransport.newClouddebuggerClient(options).build();
    Debuggee debuggee = registerDebuggee(debuggerClient, uniquifier);
    options.setDebuggee(debuggee);

    System.out.println(debuggerMessage(options.getProject(), debuggee.getUniquifier()));
  }

  private Debuggee registerDebuggee(Clouddebugger debuggerClient, String uniquifier) {
    RegisterDebuggeeRequest registerReq = new RegisterDebuggeeRequest();
    registerReq.setDebuggee(new Debuggee()
        .setProject(options.getProject())
        .setUniquifier(uniquifier)
        .setDescription(uniquifier)
        .setAgentVersion("google.com/cloud-dataflow-java/v1"));

    try {
      RegisterDebuggeeResponse registerResponse =
          debuggerClient.controller().debuggees().register(registerReq).execute();
      Debuggee debuggee = registerResponse.getDebuggee();
      if (debuggee.getStatus() != null && debuggee.getStatus().getIsError()) {
        throw new RuntimeException("Unable to register with the debugger: "
            + debuggee.getStatus().getDescription().getFormat());
      }

      return debuggee;
    } catch (IOException e) {
      throw new RuntimeException("Unable to register with the debugger: ", e);
    }
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    logWarningIfPCollectionViewHasNonDeterministicKeyCoder(pipeline);

    LOG.info("Executing pipeline on the Dataflow Service, which will have billing implications "
        + "related to Google Compute Engine usage and other Google Cloud Services.");

    List<DataflowPackage> packages = options.getStager().stageFiles();


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

    // Try to create a debuggee ID. This must happen before the job is translated since it may
    // update the options.
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    maybeRegisterDebuggee(dataflowOptions, requestId);

    JobSpecification jobSpecification =
        translator.translate(pipeline, this, packages);
    Job newJob = jobSpecification.getJob();
    newJob.setClientRequestId(requestId);

    ReleaseInfo releaseInfo = ReleaseInfo.getReleaseInfo();
    String version = releaseInfo.getVersion();
    checkState(
        !version.equals("${pom.version}"),
        "Unable to submit a job to the Dataflow service with unset version ${pom.version}");
    System.out.println("Dataflow SDK version: " + version);

    newJob.getEnvironment().setUserAgent(releaseInfo);
    // The Dataflow Service may write to the temporary directory directly, so
    // must be verified.
    if (!isNullOrEmpty(options.getGcpTempLocation())) {
      newJob.getEnvironment().setTempStoragePrefix(
          dataflowOptions.getPathValidator().verifyPath(options.getGcpTempLocation()));
    }
    newJob.getEnvironment().setDataset(options.getTempDatasetId());
    newJob.getEnvironment().setExperiments(options.getExperiments());

    // Set the Docker container image that executes Dataflow worker harness, residing in Google
    // Container Registry. Translator is guaranteed to create a worker pool prior to this point.
    String workerHarnessContainerImage =
        options.as(DataflowPipelineWorkerPoolOptions.class)
        .getWorkerHarnessContainerImage();
    for (WorkerPool workerPool : newJob.getEnvironment().getWorkerPools()) {
      workerPool.setWorkerHarnessContainerImage(workerHarnessContainerImage);
    }

    // Requirements about the service.
    Map<String, Object> environmentVersion = new HashMap<>();
    environmentVersion.put(
        PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY,
        DataflowRunnerInfo.getDataflowRunnerInfo().getEnvironmentMajorVersion());
    newJob.getEnvironment().setVersion(environmentVersion);
    // Default jobType is JAVA_BATCH_AUTOSCALING: A Java job with workers that the job can
    // autoscale if specified.
    String jobType = "JAVA_BATCH_AUTOSCALING";

    if (options.isStreaming()) {
      jobType = "STREAMING";
    }
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);

    if (hooks != null) {
      hooks.modifyEnvironmentBeforeSubmission(newJob.getEnvironment());
    }

    if (!isNullOrEmpty(options.getDataflowJobFile())
        || !isNullOrEmpty(options.getTemplateLocation())) {
      boolean isTemplate = !isNullOrEmpty(options.getTemplateLocation());
      if (isTemplate) {
        checkArgument(isNullOrEmpty(options.getDataflowJobFile()),
            "--dataflowJobFile and --templateLocation are mutually exclusive.");
      }
      String fileLocation = firstNonNull(
          options.getTemplateLocation(), options.getDataflowJobFile());
      checkArgument(fileLocation.startsWith("/") || fileLocation.startsWith("gs://"),
          String.format(
              "Location must be local or on Cloud Storage, got {}.", fileLocation));
      String workSpecJson = DataflowPipelineTranslator.jobToString(newJob);
      try (PrintWriter printWriter = new PrintWriter(
          Channels.newOutputStream(IOChannelUtils.create(fileLocation, MimeTypes.TEXT)))) {
        printWriter.print(workSpecJson);
        LOG.info("Printed job specification to {}", fileLocation);
      } catch (IOException ex) {
        String error =
            String.format("Cannot create output file at %s", fileLocation);
        if (isTemplate) {
          throw new RuntimeException(error, ex);
        } else {
          LOG.warn(error, ex);
        }
      }
      if (isTemplate) {
        LOG.info("Template successfully created.");
        return new DataflowTemplateJob();
      }
    }

    String jobIdToUpdate = null;
    if (options.isUpdate()) {
      jobIdToUpdate = getJobIdFromName(options.getJobName());
      newJob.setTransformNameMapping(options.getTransformNameMapping());
      newJob.setReplaceJobId(jobIdToUpdate);
    }
    Job jobResult;
    try {
      jobResult = dataflowClient.createJob(newJob);
    } catch (GoogleJsonResponseException e) {
      String errorMessages = "Unexpected errors";
      if (e.getDetails() != null) {
        if (Utf8.encodedLength(newJob.toString()) >= CREATE_JOB_REQUEST_LIMIT_BYTES) {
          errorMessages = "The size of the serialized JSON representation of the pipeline "
              + "exceeds the allowable limit. "
              + "For more information, please check the FAQ link below:\n"
              + "https://cloud.google.com/dataflow/faq";
        } else {
          errorMessages = e.getDetails().getMessage();
        }
      }
      throw new RuntimeException("Failed to create a workflow job: " + errorMessages, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a workflow job", e);
    }

    // Obtain all of the extractors from the PTransforms used in the pipeline so the
    // DataflowPipelineJob has access to them.
    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        pipeline.getAggregatorSteps();

    DataflowAggregatorTransforms aggregatorTransforms =
        new DataflowAggregatorTransforms(aggregatorSteps, jobSpecification.getStepNames());

    // Use a raw client for post-launch monitoring, as status calls may fail
    // regularly and need not be retried automatically.
    DataflowPipelineJob dataflowPipelineJob =
        new DataflowPipelineJob(jobResult.getId(), options, aggregatorTransforms);

    // If the service returned client request id, the SDK needs to compare it
    // with the original id generated in the request, if they are not the same
    // (i.e., the returned job is not created by this request), throw
    // DataflowJobAlreadyExistsException or DataflowJobAlreadyUpdatedException
    // depending on whether this is a reload or not.
    if (jobResult.getClientRequestId() != null && !jobResult.getClientRequestId().isEmpty()
        && !jobResult.getClientRequestId().equals(requestId)) {
      // If updating a job.
      if (options.isUpdate()) {
        throw new DataflowJobAlreadyUpdatedException(dataflowPipelineJob,
            String.format("The job named %s with id: %s has already been updated into job id: %s "
                + "and cannot be updated again.",
                newJob.getName(), jobIdToUpdate, jobResult.getId()));
      } else {
        throw new DataflowJobAlreadyExistsException(dataflowPipelineJob,
            String.format("There is already an active job named %s with id: %s. If you want "
                + "to submit a second job, try again by setting a different name using --jobName.",
                newJob.getName(), jobResult.getId()));
      }
    }

    LOG.info("To access the Dataflow monitoring console, please navigate to {}",
        MonitoringUtil.getJobMonitoringPageURL(options.getProject(), jobResult.getId()));
    System.out.println("Submitted job: " + jobResult.getId());

    LOG.info("To cancel the job using the 'gcloud' tool, run:\n> {}",
        MonitoringUtil.getGcloudCancelCommand(options, jobResult.getId()));

    return dataflowPipelineJob;
  }

  /**
   * Returns the DataflowPipelineTranslator associated with this object.
   */
  public DataflowPipelineTranslator getTranslator() {
    return translator;
  }

  /**
   * Sets callbacks to invoke during execution see {@code DataflowRunnerHooks}.
   */
  @Experimental
  public void setHooks(DataflowRunnerHooks hooks) {
    this.hooks = hooks;
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Outputs a warning about PCollection views without deterministic key coders. */
  private void logWarningIfPCollectionViewHasNonDeterministicKeyCoder(Pipeline pipeline) {
    // We need to wait till this point to determine the names of the transforms since only
    // at this time do we know the hierarchy of the transforms otherwise we could
    // have just recorded the full names during apply time.
    if (!ptransformViewsWithNonDeterministicKeyCoders.isEmpty()) {
      final SortedSet<String> ptransformViewNamesWithNonDeterministicKeyCoders = new TreeSet<>();
      pipeline.traverseTopologically(new PipelineVisitor() {
        @Override
        public void visitValue(PValue value, TransformHierarchy.Node producer) {
        }

        @Override
        public void visitPrimitiveTransform(TransformHierarchy.Node node) {
          if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
            ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
          }
        }

        @Override
        public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
          if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
            ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
          }
          return CompositeBehavior.ENTER_TRANSFORM;
        }

        @Override
        public void leaveCompositeTransform(TransformHierarchy.Node node) {
        }
      });

      LOG.warn("Unable to use indexed implementation for View.AsMap and View.AsMultimap for {} "
          + "because the key coder is not deterministic. Falling back to singleton implementation "
          + "which may cause memory and/or performance problems. Future major versions of "
          + "Dataflow will require deterministic key coders.",
          ptransformViewNamesWithNonDeterministicKeyCoders);
    }
  }

  /**
   * Returns true if the passed in {@link PCollection} needs to be materialiazed using
   * an indexed format.
   */
  boolean doesPCollectionRequireIndexedFormat(PCollection<?> pcol) {
    return pcollectionsRequiringIndexedFormat.contains(pcol);
  }

  /**
   * Marks the passed in {@link PCollection} as requiring to be materialized using
   * an indexed format.
   */
  void addPCollectionRequiringIndexedFormat(PCollection<?> pcol) {
    pcollectionsRequiringIndexedFormat.add(pcol);
  }

  /** A set of {@link View}s with non-deterministic key coders. */
  private Set<PTransform<?, ?>> ptransformViewsWithNonDeterministicKeyCoders;

  /**
   * Records that the {@link PTransform} requires a deterministic key coder.
   */
  void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    ptransformViewsWithNonDeterministicKeyCoders.add(ptransform);
  }

  /**
   * Specialized implementation which overrides
   * {@link org.apache.beam.sdk.io.Write.Bound Write.Bound} to provide Google
   * Cloud Dataflow specific path validation of {@link FileBasedSink}s.
   */
  private static class BatchWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final DataflowRunner runner;
    private final Write.Bound<T> transform;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchWrite(DataflowRunner runner, Write.Bound<T> transform) {
      this.runner = runner;
      this.transform = transform;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (transform.getSink() instanceof FileBasedSink) {
        FileBasedSink<?> sink = (FileBasedSink<?>) transform.getSink();
        if (sink.getBaseOutputFilenameProvider().isAccessible()) {
          PathValidator validator = runner.options.getPathValidator();
          validator.validateOutputFilePrefixSupported(
              sink.getBaseOutputFilenameProvider().get());
        }
      }
      return transform.expand(input);
    }
  }

  // ================================================================================
  // PubsubIO translations
  // ================================================================================

  /**
   * Suppress application of {@link PubsubUnboundedSource#expand} in streaming mode so that we
   * can instead defer to Windmill's implementation.
   */
  private static class StreamingPubsubIORead<T> extends PTransform<PBegin, PCollection<T>> {
    private final PubsubUnboundedSource<T> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    public StreamingPubsubIORead(
        DataflowRunner runner, PubsubUnboundedSource<T> transform) {
      this.transform = transform;
    }

    PubsubUnboundedSource<T> getOverriddenTransform() {
      return transform;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return PCollection.<T>createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
          .setCoder(transform.getElementCoder());
    }

    @Override
    protected String getKindString() {
      return "StreamingPubsubIORead";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingPubsubIORead.class, new StreamingPubsubIOReadTranslator<>());
    }
  }

  /** Rewrite {@link StreamingPubsubIORead} to the appropriate internal node. */
  private static class StreamingPubsubIOReadTranslator<T>
      implements TransformTranslator<StreamingPubsubIORead<T>> {
    @Override
    public void translate(StreamingPubsubIORead<T> transform, TranslationContext context) {
      checkArgument(
          context.getPipelineOptions().isStreaming(),
          "StreamingPubsubIORead is only for streaming pipelines.");
      PubsubUnboundedSource<T> overriddenTransform = transform.getOverriddenTransform();
      StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
      stepContext.addInput(PropertyNames.FORMAT, "pubsub");
      if (overriddenTransform.getTopicProvider() != null) {
        if (overriddenTransform.getTopicProvider().isAccessible()) {
          stepContext.addInput(
              PropertyNames.PUBSUB_TOPIC, overriddenTransform.getTopic().getV1Beta1Path());
        } else {
          stepContext.addInput(
              PropertyNames.PUBSUB_TOPIC_OVERRIDE,
              ((NestedValueProvider) overriddenTransform.getTopicProvider()).propertyName());
        }
      }
      if (overriddenTransform.getSubscriptionProvider() != null) {
        if (overriddenTransform.getSubscriptionProvider().isAccessible()) {
          stepContext.addInput(
              PropertyNames.PUBSUB_SUBSCRIPTION,
              overriddenTransform.getSubscription().getV1Beta1Path());
        } else {
          stepContext.addInput(
              PropertyNames.PUBSUB_SUBSCRIPTION_OVERRIDE,
              ((NestedValueProvider) overriddenTransform.getSubscriptionProvider()).propertyName());
        }
      }
      if (overriddenTransform.getTimestampLabel() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TIMESTAMP_LABEL, overriddenTransform.getTimestampLabel());
      }
      if (overriddenTransform.getIdLabel() != null) {
        stepContext.addInput(PropertyNames.PUBSUB_ID_LABEL, overriddenTransform.getIdLabel());
      }
      if (overriddenTransform.getWithAttributesParseFn() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
            byteArrayToJsonString(
                serializeToByteArray(overriddenTransform.getWithAttributesParseFn())));
      }
      stepContext.addOutput(context.getOutput(transform));
    }
  }

  /**
   * Suppress application of {@link PubsubUnboundedSink#expand} in streaming mode so that we
   * can instead defer to Windmill's implementation.
   */
  private static class StreamingPubsubIOWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final PubsubUnboundedSink<T> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    public StreamingPubsubIOWrite(
        DataflowRunner runner, PubsubUnboundedSink<T> transform) {
      this.transform = transform;
    }

    PubsubUnboundedSink<T> getOverriddenTransform() {
      return transform;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      return PDone.in(input.getPipeline());
    }

    @Override
    protected String getKindString() {
      return "StreamingPubsubIOWrite";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingPubsubIOWrite.class, new StreamingPubsubIOWriteTranslator<>());
    }
  }

  /**
   * Rewrite {@link StreamingPubsubIOWrite} to the appropriate internal node.
   */
  private static class StreamingPubsubIOWriteTranslator<T> implements
      TransformTranslator<StreamingPubsubIOWrite<T>> {

    @Override
    public void translate(
        StreamingPubsubIOWrite<T> transform,
        TranslationContext context) {
      checkArgument(context.getPipelineOptions().isStreaming(),
                    "StreamingPubsubIOWrite is only for streaming pipelines.");
      PubsubUnboundedSink<T> overriddenTransform = transform.getOverriddenTransform();
      StepTranslationContext stepContext = context.addStep(transform, "ParallelWrite");
      stepContext.addInput(PropertyNames.FORMAT, "pubsub");
      if (overriddenTransform.getTopicProvider().isAccessible()) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TOPIC, overriddenTransform.getTopic().getV1Beta1Path());
      } else {
        stepContext.addInput(
            PropertyNames.PUBSUB_TOPIC_OVERRIDE,
            ((NestedValueProvider) overriddenTransform.getTopicProvider()).propertyName());
      }
      if (overriddenTransform.getTimestampLabel() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TIMESTAMP_LABEL, overriddenTransform.getTimestampLabel());
      }
      if (overriddenTransform.getIdLabel() != null) {
        stepContext.addInput(PropertyNames.PUBSUB_ID_LABEL, overriddenTransform.getIdLabel());
      }
      if (overriddenTransform.getFormatFn() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
            byteArrayToJsonString(serializeToByteArray(overriddenTransform.getFormatFn())));
        // No coder is needed in this case since the formatFn formats directly into a byte[],
        // however the Dataflow backend require a coder to be set.
        stepContext.addEncodingInput(WindowedValue.getValueOnlyCoder(VoidCoder.of()));
      } else if (overriddenTransform.getElementCoder() != null) {
        stepContext.addEncodingInput(WindowedValue.getValueOnlyCoder(
            overriddenTransform.getElementCoder()));
      }
      stepContext.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));
    }
  }

  // ================================================================================

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.io.Read.Unbounded Read.Unbounded} for the
   * Dataflow runner in streaming mode.
   *
   * <p>In particular, if an UnboundedSource requires deduplication, then features of WindmillSink
   * are leveraged to do the deduplication.
   */
  private static class StreamingUnboundedRead<T> extends PTransform<PInput, PCollection<T>> {
    private final UnboundedSource<T, ?> source;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingUnboundedRead(DataflowRunner runner, Read.Unbounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return source.getDefaultOutputCoder();
    }

    @Override
    public final PCollection<T> expand(PInput input) {
      source.validate();

      if (source.requiresDeduping()) {
        return Pipeline.applyTransform(input, new ReadWithIds<>(source))
            .apply(new Deduplicate<T>());
      } else {
        return Pipeline.applyTransform(input, new ReadWithIds<>(source))
            .apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn<T>()));
      }
    }

    /**
     * {@link PTransform} that reads {@code (record,recordId)} pairs from an
     * {@link UnboundedSource}.
     */
    private static class ReadWithIds<T>
        extends PTransform<PInput, PCollection<ValueWithRecordId<T>>> {
      private final UnboundedSource<T, ?> source;

      private ReadWithIds(UnboundedSource<T, ?> source) {
        this.source = source;
      }

      @Override
      public final PCollection<ValueWithRecordId<T>> expand(PInput input) {
        return PCollection.<ValueWithRecordId<T>>createPrimitiveOutputInternal(
            input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED);
      }

      @Override
      protected Coder<ValueWithRecordId<T>> getDefaultOutputCoder() {
        return ValueWithRecordId.ValueWithRecordIdCoder.of(source.getDefaultOutputCoder());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(source);
      }

      public UnboundedSource<T, ?> getSource() {
        return source;
      }
    }

    @Override
    public String getKindString() {
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          ReadWithIds.class, new ReadWithIdsTranslator());
    }

    private static class ReadWithIdsTranslator
        implements TransformTranslator<ReadWithIds<?>> {
      @Override
      public void translate(ReadWithIds<?> transform,
          TranslationContext context) {
        ReadTranslator.translateReadHelper(transform.getSource(), transform, context);
      }
    }
  }

  /**
   * Remove values with duplicate ids.
   */
  private static class Deduplicate<T>
      extends PTransform<PCollection<ValueWithRecordId<T>>, PCollection<T>> {
    // Use a finite set of keys to improve bundling.  Without this, the key space
    // will be the space of ids which is potentially very large, which results in much
    // more per-key overhead.
    private static final int NUM_RESHARD_KEYS = 10000;
    @Override
    public PCollection<T> expand(PCollection<ValueWithRecordId<T>> input) {
      return input
          .apply(WithKeys.of(new SerializableFunction<ValueWithRecordId<T>, Integer>() {
                    @Override
                    public Integer apply(ValueWithRecordId<T> value) {
                      return Arrays.hashCode(value.getId()) % NUM_RESHARD_KEYS;
                    }
                  }))
          // Reshuffle will dedup based on ids in ValueWithRecordId by passing the data through
          // WindmillSink.
          .apply(Reshuffle.<Integer, ValueWithRecordId<T>>of())
          .apply("StripIds", ParDo.of(
              new DoFn<KV<Integer, ValueWithRecordId<T>>, T>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(c.element().getValue().getValue());
                }
              }));
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.io.Read.Bounded Read.Bounded} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    /** Builds an instance of this class from the overridden transform. */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingBoundedRead(DataflowRunner runner, Read.Bounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return source.getDefaultOutputCoder();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      return Pipeline.applyTransform(input, new DataflowUnboundedReadFromBoundedSource<>(source))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
  }

  /**
   * A marker {@link DoFn} for writing the contents of a {@link PCollection} to a streaming
   * {@link PCollectionView} backend implementation.
   */
  @Deprecated
  public static class StreamingPCollectionViewWriterFn<T> extends DoFn<Iterable<T>, T> {
    private final PCollectionView<?> view;
    private final Coder<T> dataCoder;

    public static <T> StreamingPCollectionViewWriterFn<T> create(
        PCollectionView<?> view, Coder<T> dataCoder) {
      return new StreamingPCollectionViewWriterFn<>(view, dataCoder);
    }

    private StreamingPCollectionViewWriterFn(PCollectionView<?> view, Coder<T> dataCoder) {
      this.view = view;
      this.dataCoder = dataCoder;
    }

    public PCollectionView<?> getView() {
      return view;
    }

    public Coder<T> getDataCoder() {
      return dataCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow w) throws Exception {
      throw new UnsupportedOperationException(
          String.format(
              "%s is a marker class only and should never be executed.", getClass().getName()));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap}
   * for the Dataflow runner in streaming mode.
   */
  private static class StreamingViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {
    private final DataflowRunner runner;

    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingViewAsMap(DataflowRunner runner, View.AsMap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, V>> view =
          PCollectionViews.mapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
          .apply(View.CreatePCollectionView.<KV<K, V>, Map<K, V>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMap";
    }
  }

  /**
   * Specialized expansion for {@link
   * org.apache.beam.sdk.transforms.View.AsMultimap View.AsMultimap} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {
    private final DataflowRunner runner;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingViewAsMultimap(DataflowRunner runner, View.AsMultimap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, Iterable<V>>> view =
          PCollectionViews.multimapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
          .apply(View.CreatePCollectionView.<KV<K, V>, Map<K, Iterable<V>>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMultimap";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingViewAsList(DataflowRunner runner, View.AsList<T> transform) {}

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      PCollectionView<List<T>> view =
          PCollectionViews.listView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
          .apply(View.CreatePCollectionView.<T, List<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsList";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsIterable View.AsIterable} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingViewAsIterable(DataflowRunner runner, View.AsIterable<T> transform) { }

    @Override
    public PCollectionView<Iterable<T>> expand(PCollection<T> input) {
      PCollectionView<Iterable<T>> view =
          PCollectionViews.iterableView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, input.getCoder())))
          .apply(View.CreatePCollectionView.<T, Iterable<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsIterable";
    }
  }

  private static class WrapAsList<T> extends DoFn<T, List<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Arrays.asList(c.element()));
    }
  }

  /**
   * Specialized expansion for
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingViewAsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {
    private View.AsSingleton<T> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingViewAsSingleton(DataflowRunner runner, View.AsSingleton<T> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> expand(PCollection<T> input) {
      Combine.Globally<T, T> combine = Combine.globally(
          new SingletonCombine<>(transform.hasDefaultValue(), transform.defaultValue()));
      if (!transform.hasDefaultValue()) {
        combine = combine.withoutDefaults();
      }
      return input.apply(combine.asSingletonView());
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsSingleton";
    }

    private static class SingletonCombine<T> extends Combine.BinaryCombineFn<T> {
      private boolean hasDefaultValue;
      private T defaultValue;

      SingletonCombine(boolean hasDefaultValue, T defaultValue) {
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
      }

      @Override
      public T apply(T left, T right) {
        throw new IllegalArgumentException("PCollection with more than one element "
            + "accessed as a singleton view. Consider using Combine.globally().asSingleton() to "
            + "combine the PCollection into a single value");
      }

      @Override
      public T identity() {
        if (hasDefaultValue) {
          return defaultValue;
        } else {
          throw new IllegalArgumentException(
              "Empty PCollection accessed as a singleton view. "
              + "Consider setting withDefault to provide a default value");
        }
      }
    }
  }

  private static class StreamingCombineGloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {
    Combine.GloballyAsSingletonView<InputT, OutputT> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public StreamingCombineGloballyAsSingletonView(
        DataflowRunner runner,
        Combine.GloballyAsSingletonView<InputT, OutputT> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(Combine.<InputT, OutputT>globally(transform.getCombineFn())
              .withoutDefaults()
              .withFanout(transform.getFanout()));

      PCollectionView<OutputT> view = PCollectionViews.singletonView(
          combined.getPipeline(),
          combined.getWindowingStrategy(),
          transform.getInsertDefault(),
          transform.getInsertDefault()
            ? transform.getCombineFn().defaultValue() : null,
          combined.getCoder());
      return combined
          .apply(ParDo.of(new WrapAsList<OutputT>()))
          .apply(ParDo.of(StreamingPCollectionViewWriterFn.create(view, combined.getCoder())))
          .apply(View.CreatePCollectionView.<OutputT, OutputT>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingCombineGloballyAsSingletonView";
    }
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use by {@link StreamingViewAsMap}, {@link StreamingViewAsMultimap},
   * {@link StreamingViewAsList}, {@link StreamingViewAsIterable}.
   * They require the input {@link PCollection} fits in memory.
   * For a large {@link PCollection} this is expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  /**
   * Specialized expansion for unsupported IO transforms and DoFns that throws an error.
   */
  private static class UnsupportedIO<InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> {
    @Nullable
    private PTransform<?, ?> transform;
    @Nullable
    private DoFn<?, ?> doFn;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner, Read.Unbounded<?> transform) {
      this.transform = transform;
    }

    /**
     * Builds an instance of this class from the overridden doFn.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner,
                         PubsubIO.Read<?>.PubsubBoundedReader doFn) {
      this.doFn = doFn;
    }

    /**
     * Builds an instance of this class from the overridden doFn.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner,
                         PubsubIO.Write<?>.PubsubBoundedWriter doFn) {
      this.doFn = doFn;
    }

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner, PubsubUnboundedSource<?> transform) {
      this.transform = transform;
    }

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner, PubsubUnboundedSink<?> transform) {
      this.transform = transform;
    }


    @Override
    public OutputT expand(InputT input) {
      String mode = input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()
          ? "streaming" : "batch";
      String name =
          transform == null
              ? NameUtils.approximateSimpleName(doFn)
              : NameUtils.approximatePTransformName(transform.getClass());
      throw new UnsupportedOperationException(
          String.format("The DataflowRunner in %s mode does not support %s.", mode, name));
    }
  }

  @Override
  public String toString() {
    return "DataflowRunner#" + options.getJobName();
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
        listResult = dataflowClient.listJobs(token);
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
