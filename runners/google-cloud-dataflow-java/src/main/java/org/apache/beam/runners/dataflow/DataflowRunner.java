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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.beam.runners.core.construction.DeduplicatedFlattenFactory;
import org.apache.beam.runners.core.construction.EmptyFlattenAsCreateFactory;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.runners.dataflow.BatchViewOverrides.BatchCombineGloballyAsSingletonViewFactory;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.StreamingViewOverrides.StreamingCreatePCollectionViewFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.DataflowTemplateJob;
import org.apache.beam.runners.dataflow.util.DataflowTransport;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.transforms.DoFn;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
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
  }

  private List<PTransformOverride> getOverrides(boolean streaming) {
    ImmutableList.Builder<PTransformOverride> overridesBuilder = ImmutableList.builder();
    // Create is implemented in terms of a Read, so it must precede the override to Read in
    // streaming
    overridesBuilder
        .add(
            PTransformOverride.of(
                PTransformMatchers.flattenWithDuplicateInputs(),
                DeduplicatedFlattenFactory.create()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.emptyFlatten(), EmptyFlattenAsCreateFactory.instance()));
    if (streaming) {
      if (!hasExperiment(options, "enable_custom_pubsub_source")) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(PubsubUnboundedSource.class),
                new ReflectiveRootOverrideFactory(StreamingPubsubIORead.class, this)));
      }
      if (!hasExperiment(options, "enable_custom_pubsub_sink")) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(PubsubUnboundedSink.class),
                new StreamingPubsubIOWriteOverrideFactory(this)));
      }
      overridesBuilder
          .add(
              // Streaming Bounded Read is implemented in terms of Streaming Unbounded Read, and
              // must precede it
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Read.Bounded.class),
                  new ReflectiveRootOverrideFactory(StreamingBoundedRead.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Read.Unbounded.class),
                  new ReflectiveRootOverrideFactory(StreamingUnboundedRead.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.CreatePCollectionView.class),
                  new StreamingCreatePCollectionViewFactory()));
    } else {
      overridesBuilder
          // State and timer pardos are implemented by expansion to GBK-then-ParDo
          .add(
              PTransformOverride.of(
                  PTransformMatchers.stateOrTimerParDoMulti(),
                  BatchStatefulParDoOverrides.multiOutputOverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.stateOrTimerParDoSingle(),
                  BatchStatefulParDoOverrides.singleOutputOverrideFactory()))

          // Write uses views internally
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Write.class), new BatchWriteFactory(this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                  new BatchCombineGloballyAsSingletonViewFactory(this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsMap.class),
                  new ReflectiveOneToOneOverrideFactory(
                      BatchViewOverrides.BatchViewAsMap.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsMultimap.class),
                  new ReflectiveOneToOneOverrideFactory(
                      BatchViewOverrides.BatchViewAsMultimap.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsSingleton.class),
                  new ReflectiveOneToOneOverrideFactory(
                      BatchViewOverrides.BatchViewAsSingleton.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsList.class),
                  new ReflectiveOneToOneOverrideFactory(
                      BatchViewOverrides.BatchViewAsList.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsIterable.class),
                  new ReflectiveOneToOneOverrideFactory(
                      BatchViewOverrides.BatchViewAsIterable.class, this)));
    }
    overridesBuilder
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(Reshuffle.class), new ReshuffleOverrideFactory()))
        // Order is important. Streaming views almost all use Combine internally.
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(Combine.GroupedValues.class),
                new PrimitiveCombineGroupedValuesOverrideFactory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(ParDo.SingleOutput.class),
                new PrimitiveParDoSingleFactory()));
    return overridesBuilder.build();
  }

  private static class ReflectiveOneToOneOverrideFactory<
          InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, TransformT> {
    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;
    private final DataflowRunner runner;

    private ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement,
        DataflowRunner runner) {
      this.replacement = replacement;
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      PTransform<PCollection<InputT>, PCollection<OutputT>> rep =
          InstanceBuilder.ofType(replacement)
              .withArg(DataflowRunner.class, runner)
              .withArg(
                  (Class<TransformT>) transform.getTransform().getClass(), transform.getTransform())
              .build();
      return PTransformReplacement.of(PTransformReplacements.getSingletonMainInput(transform), rep);
    }
  }

  private static class ReflectiveRootOverrideFactory<T>
      implements PTransformOverrideFactory<
          PBegin, PCollection<T>, PTransform<PInput, PCollection<T>>> {
    private final Class<PTransform<PBegin, PCollection<T>>> replacement;
    private final DataflowRunner runner;

    private ReflectiveRootOverrideFactory(
        Class<PTransform<PBegin, PCollection<T>>> replacement, DataflowRunner runner) {
      this.replacement = replacement;
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PInput, PCollection<T>>> transform) {
      PTransform<PInput, PCollection<T>> original = transform.getTransform();
      return PTransformReplacement.of(
          transform.getPipeline().begin(),
          InstanceBuilder.ofType(replacement)
              .withArg(DataflowRunner.class, runner)
              .withArg(
                  (Class<? super PTransform<PInput, PCollection<T>>>) original.getClass(), original)
              .build());
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
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
    if (containsUnboundedPCollection(pipeline)) {
      options.setStreaming(true);
    }
    replaceTransforms(pipeline);

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
    String workerHarnessContainerImage = getContainerImageForJob(options);
    for (WorkerPool workerPool : newJob.getEnvironment().getWorkerPools()) {
      workerPool.setWorkerHarnessContainerImage(workerHarnessContainerImage);
    }

    newJob.getEnvironment().setVersion(getEnvironmentVersion(options));

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
      checkArgument(
          fileLocation.startsWith("/") || fileLocation.startsWith("gs://"),
          "Location must be local or on Cloud Storage, got %s.",
          fileLocation);
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

  /** Returns true if the specified experiment is enabled, handling null experiments. */
  public static boolean hasExperiment(DataflowPipelineDebugOptions options, String experiment) {
    List<String> experiments =
        firstNonNull(options.getExperiments(), Collections.<String>emptyList());
    return experiments.contains(experiment);
  }

  /** Helper to configure the Dataflow Job Environment based on the user's job options. */
  private static Map<String, Object> getEnvironmentVersion(DataflowPipelineOptions options) {
    DataflowRunnerInfo runnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String majorVersion;
    String jobType;
    if (hasExperiment(options, "beam_fn_api")) {
      majorVersion = runnerInfo.getFnApiEnvironmentMajorVersion();
      jobType = options.isStreaming() ? "FNAPI_STREAMING" : "FNAPI_BATCH";
    } else {
      majorVersion = runnerInfo.getLegacyEnvironmentMajorVersion();
      jobType = options.isStreaming() ? "STREAMING" : "JAVA_BATCH_AUTOSCALING";
    }
    return ImmutableMap.<String, Object>of(
        PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, majorVersion,
        PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);
  }

  @VisibleForTesting
  void replaceTransforms(Pipeline pipeline) {
    boolean streaming = options.isStreaming() || containsUnboundedPCollection(pipeline);
    // Ensure all outputs of all reads are consumed before potentially replacing any
    // Read PTransforms
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    pipeline.replaceAll(getOverrides(streaming));
  }

  private boolean containsUnboundedPCollection(Pipeline p) {
    class BoundednessVisitor extends PipelineVisitor.Defaults {
      IsBounded boundedness = IsBounded.BOUNDED;

      @Override
      public void visitValue(PValue value, Node producer) {
        if (value instanceof PCollection) {
          boundedness = boundedness.and(((PCollection) value).isBounded());
        }
      }
    }
    BoundednessVisitor visitor = new BoundednessVisitor();
    p.traverseTopologically(visitor);
    return visitor.boundedness == IsBounded.UNBOUNDED;
  };

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
      pipeline.traverseTopologically(
          new PipelineVisitor() {
            @Override
            public void visitValue(PValue value, TransformHierarchy.Node producer) {}

            @Override
            public void visitPrimitiveTransform(TransformHierarchy.Node node) {
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
            }

            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              if (node.getTransform() instanceof View.AsMap
                  || node.getTransform() instanceof View.AsMultimap) {
                PCollection<KV<?, ?>> input =
                    (PCollection<KV<?, ?>>) Iterables.getOnlyElement(node.getInputs().values());
                KvCoder<?, ?> inputCoder = (KvCoder) input.getCoder();
                try {
                  inputCoder.getKeyCoder().verifyDeterministic();
                } catch (NonDeterministicException e) {
                  ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
                }
              }
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
              return CompositeBehavior.ENTER_TRANSFORM;
            }

            @Override
            public void leaveCompositeTransform(TransformHierarchy.Node node) {}
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

  private class BatchWriteFactory<T>
      implements PTransformOverrideFactory<PCollection<T>, PDone, Write<T>> {
    private final DataflowRunner runner;
    private BatchWriteFactory(DataflowRunner dataflowRunner) {
      this.runner = dataflowRunner;
    }

    @Override
    public PTransformReplacement<PCollection<T>, PDone> getReplacementTransform(
        AppliedPTransform<PCollection<T>, PDone, Write<T>> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchWrite<>(runner, transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PDone newOutput) {
      return Collections.emptyMap();
    }
  }

  /**
   * Specialized implementation which overrides
   * {@link org.apache.beam.sdk.io.Write Write} to provide Google
   * Cloud Dataflow specific path validation of {@link FileBasedSink}s.
   */
  private static class BatchWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final DataflowRunner runner;
    private final Write<T> transform;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchWrite(DataflowRunner runner, Write<T> transform) {
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
      PCollection<T> input = context.getInput(transform);
      stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
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
  private static class StreamingUnboundedRead<T> extends PTransform<PBegin, PCollection<T>> {
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
    public final PCollection<T> expand(PBegin input) {
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

      return Pipeline.applyTransform(input, new UnboundedReadFromBoundedSource<>(source))
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

  static class CombineGroupedValues<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {
    private final Combine.GroupedValues<K, InputT, OutputT> original;

    CombineGroupedValues(GroupedValues<K, InputT, OutputT> original) {
      this.original = original;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<InputT>>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
    }

    public Combine.GroupedValues<K, InputT, OutputT> getOriginalCombine() {
      return original;
    }
  }

  private static class PrimitiveCombineGroupedValuesOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>,
          Combine.GroupedValues<K, InputT, OutputT>> {
    @Override
    public PTransformReplacement<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>,
                    GroupedValues<K, InputT, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new CombineGroupedValues<>(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<KV<K, OutputT>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private class StreamingPubsubIOWriteOverrideFactory<T>
      implements PTransformOverrideFactory<PCollection<T>, PDone, PubsubUnboundedSink<T>> {
    private final DataflowRunner runner;

    private StreamingPubsubIOWriteOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<T>, PDone> getReplacementTransform(
        AppliedPTransform<PCollection<T>, PDone, PubsubUnboundedSink<T>> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingPubsubIOWrite<>(runner, transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PDone newOutput) {
      return Collections.emptyMap();
    }
  }

  @VisibleForTesting
  static String getContainerImageForJob(DataflowPipelineOptions options) {
    String workerHarnessContainerImage = options.getWorkerHarnessContainerImage();
    if (!workerHarnessContainerImage.contains("IMAGE")) {
      return workerHarnessContainerImage;
    } else if (hasExperiment(options, "beam_fn_api")) {
      return workerHarnessContainerImage.replace("IMAGE", "java");
    } else if (options.isStreaming()) {
      return workerHarnessContainerImage.replace("IMAGE", "beam-java-streaming");
    } else {
      return workerHarnessContainerImage.replace("IMAGE", "beam-java-batch");
    }
  }
}
