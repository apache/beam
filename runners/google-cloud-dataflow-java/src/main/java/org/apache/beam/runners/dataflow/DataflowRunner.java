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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.beam.sdk.util.StringUtils.approximatePTransformName;
import static org.apache.beam.sdk.util.StringUtils.approximateSimpleName;
import static org.apache.beam.sdk.util.WindowedValue.valueInEmptyWindows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.clouddebugger.v2.Clouddebugger;
import com.google.api.services.clouddebugger.v2.model.Debuggee;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeRequest;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeResponse;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Utf8;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.TransformTranslator;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.internal.AssignWindows;
import org.apache.beam.runners.dataflow.internal.DataflowAggregatorTransforms;
import org.apache.beam.runners.dataflow.internal.DataflowUnboundedReadFromBoundedSource;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.internal.ReadTranslator;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.DataflowTransport;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
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
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.PathValidator;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.ValueWithRecordId;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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
 * <p><h3>Permissions</h3>
 *
 * When reading from a Dataflow source or writing to a Dataflow sink using {@code DataflowRunner},
 * the Google cloudservices account and the Google compute engine service account of the GCP project
 * running the Dataflow Job will need access to the corresponding source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
public class DataflowRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowRunner.class);

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /** Client for the Dataflow service. This is used to actually submit jobs. */
  private final Dataflow dataflowClient;

  /** Translator for this DataflowRunner, based on options. */
  private final DataflowPipelineTranslator translator;

  /** Custom transforms implementations. */
  private final Map<Class<?>, Class<?>> overrides;

  /** A set of user defined functions to invoke at different points in execution. */
  private DataflowRunnerHooks hooks;

  // Environment version information.
  private static final String ENVIRONMENT_MAJOR_VERSION = "5";

  // Default Docker container images that execute Dataflow worker harness, residing in Google
  // Container Registry, separately for Batch and Streaming.
  public static final String BATCH_WORKER_HARNESS_CONTAINER_IMAGE =
      "dataflow.gcr.io/v1beta3/beam-java-batch:beam-master-20160926";
  public static final String STREAMING_WORKER_HARNESS_CONTAINER_IMAGE =
      "dataflow.gcr.io/v1beta3/beam-java-streaming:beam-master-20160926";

  // The limit of CreateJob request size.
  private static final int CREATE_JOB_REQUEST_LIMIT_BYTES = 10 * 1024 * 1024;

  @VisibleForTesting
  static final int GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT = 1 * 1024 * 1024;

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
    IOChannelUtils.registerStandardIOFactories(options);

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
    checkArgument(
        !isNullOrEmpty(dataflowOptions.getGcpTempLocation()),
        "DataflowRunner requires gcpTempLocation, and it is missing in PipelineOptions.");
    validator.validateOutputFilePrefixSupported(dataflowOptions.getGcpTempLocation());
    checkArgument(
        !isNullOrEmpty(dataflowOptions.getStagingLocation()),
        "DataflowRunner requires stagingLocation, and it is missing in PipelineOptions.");
    validator.validateOutputFilePrefixSupported(dataflowOptions.getStagingLocation());

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
    this.dataflowClient = options.getDataflowClient();
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
      builder.put(Window.Bound.class, AssignWindows.class);
      // In streaming mode must use either the custom Pubsub unbounded source/sink or
      // defer to Windmill's built-in implementation.
      builder.put(PubsubIO.Read.Bound.PubsubBoundedReader.class, UnsupportedIO.class);
      builder.put(PubsubIO.Write.Bound.PubsubBoundedWriter.class, UnsupportedIO.class);
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
      builder.put(Window.Bound.class, AssignWindows.class);
      builder.put(Write.Bound.class, BatchWrite.class);
      // In batch mode must use the custom Pubsub bounded source/sink.
      builder.put(PubsubUnboundedSource.class, UnsupportedIO.class);
      builder.put(PubsubUnboundedSink.class, UnsupportedIO.class);
      if (options.getExperiments() == null
          || !options.getExperiments().contains("disable_ism_side_input")) {
        builder.put(View.AsMap.class, BatchViewAsMap.class);
        builder.put(View.AsMultimap.class, BatchViewAsMultimap.class);
        builder.put(View.AsSingleton.class, BatchViewAsSingleton.class);
        builder.put(View.AsList.class, BatchViewAsList.class);
        builder.put(View.AsIterable.class, BatchViewAsIterable.class);
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

    if (Combine.GroupedValues.class.equals(transform.getClass())
        || GroupByKey.class.equals(transform.getClass())) {

      // For both Dataflow runners (streaming and batch), GroupByKey and GroupedValues are
      // primitives. Returning a primitive output instead of the expanded definition
      // signals to the translator that translation is necessary.
      @SuppressWarnings("unchecked")
      PCollection<?> pc = (PCollection<?>) input;
      @SuppressWarnings("unchecked")
      OutputT outputT = (OutputT) PCollection.createPrimitiveOutputInternal(
          pc.getPipeline(),
          transform instanceof GroupByKey
              ? ((GroupByKey<?, ?>) transform).updateWindowingStrategy(pc.getWindowingStrategy())
              : pc.getWindowingStrategy(),
          pc.isBounded());
      return outputT;
    } else if (Window.Bound.class.equals(transform.getClass())) {
      /*
       * TODO: make this the generic way overrides are applied (using super.apply() rather than
       * Pipeline.applyTransform(); this allows the apply method to be replaced without inserting
       * additional nodes into the graph.
       */
      // casting to wildcard
      @SuppressWarnings("unchecked")
      OutputT windowed = (OutputT) applyWindow((Window.Bound<?>) transform, (PCollection<?>) input);
      return windowed;
    } else if (Flatten.FlattenPCollectionList.class.equals(transform.getClass())
        && ((PCollectionList<?>) input).size() == 0) {
      return (OutputT) Pipeline.applyTransform(input.getPipeline().begin(), Create.of());
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

  private <T> PCollection<T> applyWindow(
      Window.Bound<?> intitialTransform, PCollection<?> initialInput) {
    // types are matched at compile time
    @SuppressWarnings("unchecked")
    Window.Bound<T> transform = (Window.Bound<T>) intitialTransform;
    @SuppressWarnings("unchecked")
    PCollection<T> input = (PCollection<T>) initialInput;
    return super.apply(new AssignWindows<>(transform), input);
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

    String version = ReleaseInfo.getReleaseInfo().getVersion();
    System.out.println("Dataflow SDK version: " + version);

    newJob.getEnvironment().setUserAgent(ReleaseInfo.getReleaseInfo());
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
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, ENVIRONMENT_MAJOR_VERSION);
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

    if (!isNullOrEmpty(options.getDataflowJobFile())) {
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

    String jobIdToUpdate = null;
    if (options.isUpdate()) {
      jobIdToUpdate = getJobIdFromName(options.getJobName());
      newJob.setTransformNameMapping(options.getTransformNameMapping());
      newJob.setReplaceJobId(jobIdToUpdate);
    }
    Job jobResult;
    try {
      jobResult = dataflowClient
              .projects()
              .jobs()
              .create(options.getProject(), newJob)
              .execute();
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
    DataflowPipelineJob dataflowPipelineJob = new DataflowPipelineJob(
        options.getProject(), jobResult.getId(), options, aggregatorTransforms);

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
        public void visitValue(PValue value, TransformTreeNode producer) {
        }

        @Override
        public void visitPrimitiveTransform(TransformTreeNode node) {
          if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
            ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
          }
        }

        @Override
        public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
          if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
            ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
          }
          return CompositeBehavior.ENTER_TRANSFORM;
        }

        @Override
        public void leaveCompositeTransform(TransformTreeNode node) {
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
  private void addPCollectionRequiringIndexedFormat(PCollection<?> pcol) {
    pcollectionsRequiringIndexedFormat.add(pcol);
  }

  /** A set of {@link View}s with non-deterministic key coders. */
  Set<PTransform<?, ?>> ptransformViewsWithNonDeterministicKeyCoders;

  /**
   * Records that the {@link PTransform} requires a deterministic key coder.
   */
  private void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    ptransformViewsWithNonDeterministicKeyCoders.add(ptransform);
  }

  /**
   * A {@link GroupByKey} transform for the {@link DataflowRunner} which sorts
   * values using the secondary key {@code K2}.
   *
   * <p>The {@link PCollection} created created by this {@link PTransform} will have values in
   * the empty window. Care must be taken *afterwards* to either re-window
   * (using {@link Window#into}) or only use {@link PTransform}s that do not depend on the
   * values being within a window.
   */
  static class GroupByKeyAndSortValuesOnly<K1, K2, V>
      extends PTransform<PCollection<KV<K1, KV<K2, V>>>, PCollection<KV<K1, Iterable<KV<K2, V>>>>> {
    private GroupByKeyAndSortValuesOnly() {
    }

    @Override
    public PCollection<KV<K1, Iterable<KV<K2, V>>>> apply(PCollection<KV<K1, KV<K2, V>>> input) {
      PCollection<KV<K1, Iterable<KV<K2, V>>>> rval =
          PCollection.<KV<K1, Iterable<KV<K2, V>>>>createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          IsBounded.BOUNDED);

      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K1, KV<K2, V>> inputCoder = (KvCoder) input.getCoder();
      rval.setCoder(
          KvCoder.of(inputCoder.getKeyCoder(),
          IterableCoder.of(inputCoder.getValueCoder())));
      return rval;
    }
  }

  /**
   * A {@link PTransform} that groups the values by a hash of the window's byte representation
   * and sorts the values using the windows byte representation.
   */
  private static class GroupByWindowHashAsKeyAndWindowAsSortKey<T, W extends BoundedWindow> extends
      PTransform<PCollection<T>, PCollection<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>>> {

    /**
     * A {@link DoFn} that for each element outputs a {@code KV} structure suitable for
     * grouping by the hash of the window's byte representation and sorting the grouped values
     * using the window's byte representation.
     */
    @SystemDoFnInternal
    private static class UseWindowHashAsKeyAndWindowAsSortKeyDoFn<T, W extends BoundedWindow>
        extends DoFn<T, KV<Integer, KV<W, WindowedValue<T>>>> {

      private final IsmRecordCoder<?> ismCoderForHash;
      private UseWindowHashAsKeyAndWindowAsSortKeyDoFn(IsmRecordCoder<?> ismCoderForHash) {
        this.ismCoderForHash = ismCoderForHash;
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow untypedWindow) throws Exception {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        c.output(
            KV.of(ismCoderForHash.hash(ImmutableList.of(window)),
                KV.of(window,
                    WindowedValue.of(
                        c.element(),
                        c.timestamp(),
                        window,
                        c.pane()))));
      }
    }

    private final IsmRecordCoder<?> ismCoderForHash;
    private GroupByWindowHashAsKeyAndWindowAsSortKey(IsmRecordCoder<?> ismCoderForHash) {
      this.ismCoderForHash = ismCoderForHash;
    }

    @Override
    public PCollection<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>> apply(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();
      PCollection<KV<Integer, KV<W, WindowedValue<T>>>> rval =
          input.apply(ParDo.of(
              new UseWindowHashAsKeyAndWindowAsSortKeyDoFn<T, W>(ismCoderForHash)));
      rval.setCoder(
          KvCoder.of(
              VarIntCoder.of(),
              KvCoder.of(windowCoder,
                  FullWindowedValueCoder.of(input.getCoder(), windowCoder))));
      return rval.apply(new GroupByKeyAndSortValuesOnly<Integer, W, WindowedValue<T>>());
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of files in the {@link IsmFormat} sharded by the hash of the windows
   * byte representation and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {

    /**
     * A {@link DoFn} that outputs {@link IsmRecord}s. These records are structured as follows:
     * <ul>
     *   <li>Key 1: Window
     *   <li>Value: Windowed value
     * </ul>
     */
    static class IsmRecordForSingularValuePerWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
                             IsmRecord<WindowedValue<T>>> {

      private final Coder<W> windowCoder;
      IsmRecordForSingularValuePerWindowDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        T previousValue = null;

        Iterator<KV<W, WindowedValue<T>>> iterator = c.element().getValue().iterator();
        while (iterator.hasNext()) {
          KV<W, WindowedValue<T>> next = iterator.next();
          Object currentWindowStructuralValue = windowCoder.structuralValue(next.getKey());

          // Verify that the user isn't trying to have more than one element per window as
          // a singleton.
          checkState(!previousWindowStructuralValue.isPresent()
              || !previousWindowStructuralValue.get().equals(currentWindowStructuralValue),
              "Multiple values [%s, %s] found for singleton within window [%s].",
              previousValue,
              next.getValue().getValue(),
              next.getKey());

          c.output(
              IsmRecord.of(
                  ImmutableList.of(next.getKey()), next.getValue()));

          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousValue = next.getValue().getValue();
        }
      }
    }

    private final DataflowRunner runner;
    private final View.AsSingleton<T> transform;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsSingleton(DataflowRunner runner, View.AsSingleton<T> transform) {
      this.runner = runner;
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> apply(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      return BatchViewAsSingleton.<T, T, T, BoundedWindow>applyForSingleton(
          runner,
          input,
          new IsmRecordForSingularValuePerWindowDoFn<T, BoundedWindow>(windowCoder),
          transform.hasDefaultValue(),
          transform.defaultValue(),
          input.getCoder());
    }

    static <T, FinalT, ViewT, W extends BoundedWindow> PCollectionView<ViewT>
        applyForSingleton(
            DataflowRunner runner,
            PCollection<T> input,
            DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
                             IsmRecord<WindowedValue<FinalT>>> doFn,
            boolean hasDefault,
            FinalT defaultValue,
            Coder<FinalT> defaultValueCoder) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      PCollectionView<ViewT> view =
          (PCollectionView<ViewT>) PCollectionViews.<FinalT, W>singletonView(
              input.getPipeline(),
              (WindowingStrategy) input.getWindowingStrategy(),
              hasDefault,
              defaultValue,
              defaultValueCoder);

      IsmRecordCoder<WindowedValue<FinalT>> ismCoder =
          coderForSingleton(windowCoder, defaultValueCoder);

      PCollection<IsmRecord<WindowedValue<FinalT>>> reifiedPerWindowAndSorted = input
              .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
              .apply(ParDo.of(doFn));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      return reifiedPerWindowAndSorted.apply(
          CreatePCollectionView.<IsmRecord<WindowedValue<FinalT>>, ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsSingleton";
    }

    static <T> IsmRecordCoder<WindowedValue<T>> coderForSingleton(
        Coder<? extends BoundedWindow> windowCoder, Coder<T> valueCoder) {
      return IsmRecordCoder.of(
          1, // We hash using only the window
          0, // There are no metadata records
          ImmutableList.<Coder<?>>of(windowCoder),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsIterable View.AsIterable} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the windows byte representation
   * and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Key 2: Index offset within window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsIterable(DataflowRunner runner, View.AsIterable<T> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Iterable<T>> apply(PCollection<T> input) {
      PCollectionView<Iterable<T>> view = PCollectionViews.iterableView(
          input.getPipeline(), input.getWindowingStrategy(), input.getCoder());
      return BatchViewAsList.applyForIterableLike(runner, input, view);
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the window's byte representation
   * and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Key 2: Index offset within window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    /**
     * A {@link DoFn} which creates {@link IsmRecord}s assuming that each element is within the
     * global window. Each {@link IsmRecord} has
     * <ul>
     *   <li>Key 1: Global window</li>
     *   <li>Key 2: Index offset within window</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForGlobalWindowDoFn<T>
        extends DoFn<T, IsmRecord<WindowedValue<T>>> {

      long indexInBundle;
      @StartBundle
      public void startBundle(Context c) throws Exception {
        indexInBundle = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(IsmRecord.of(
            ImmutableList.of(GlobalWindow.INSTANCE, indexInBundle),
            WindowedValue.of(
                c.element(),
                c.timestamp(),
                GlobalWindow.INSTANCE,
                c.pane())));
        indexInBundle += 1;
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows
     * to locate the window boundaries. The {@link IsmRecord} has:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Key 2: Index offset within window</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForNonGlobalWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
                             IsmRecord<WindowedValue<T>>> {

      private final Coder<W> windowCoder;
      ToIsmRecordForNonGlobalWindowDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        long elementsInWindow = 0;
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        for (KV<W, WindowedValue<T>> value : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(value.getKey());
          // Compare to see if this is a new window so we can reset the index counter i
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Reset i since we have a new window.
            elementsInWindow = 0;
          }
          c.output(IsmRecord.of(
              ImmutableList.of(value.getKey(), elementsInWindow),
              value.getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          elementsInWindow += 1;
        }
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsList(DataflowRunner runner, View.AsList<T> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<List<T>> apply(PCollection<T> input) {
      PCollectionView<List<T>> view = PCollectionViews.listView(
          input.getPipeline(), input.getWindowingStrategy(), input.getCoder());
      return applyForIterableLike(runner, input, view);
    }

    static <T, W extends BoundedWindow, ViewT> PCollectionView<ViewT> applyForIterableLike(
        DataflowRunner runner,
        PCollection<T> input,
        PCollectionView<ViewT> view) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      IsmRecordCoder<WindowedValue<T>> ismCoder = coderForListLike(windowCoder, input.getCoder());

      // If we are working in the global window, we do not need to do a GBK using the window
      // as the key since all the elements of the input PCollection are already such.
      // We just reify the windowed value while converting them to IsmRecords and generating
      // an index based upon where we are within the bundle. Each bundle
      // maps to one file exactly.
      if (input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
        PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted =
            input.apply(ParDo.of(new ToIsmRecordForGlobalWindowDoFn<T>()));
        reifiedPerWindowAndSorted.setCoder(ismCoder);

        runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
        return reifiedPerWindowAndSorted.apply(
            CreatePCollectionView.<IsmRecord<WindowedValue<T>>, ViewT>of(view));
      }

      PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted = input
              .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
              .apply(ParDo.of(new ToIsmRecordForNonGlobalWindowDoFn<T, W>(windowCoder)));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      return reifiedPerWindowAndSorted.apply(
          CreatePCollectionView.<IsmRecord<WindowedValue<T>>, ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsList";
    }

    static <T> IsmRecordCoder<WindowedValue<T>> coderForListLike(
        Coder<? extends BoundedWindow> windowCoder, Coder<T> valueCoder) {
      // TODO: swap to use a variable length long coder which has values which compare
      // the same as their byte representation compare lexicographically within the key coder
      return IsmRecordCoder.of(
          1, // We hash using only the window
          0, // There are no metadata records
          ImmutableList.of(windowCoder, BigEndianLongCoder.of()),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte
   * representation. Each record is structured as follows:
   * <ul>
   *   <li>Key 1: User key K</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: 0L (constant)</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   * <ul>
   *   <li>Key 1: Metadata Key</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index [0, size of map]</li>
   *   <li>Value: variable length long byte representation of size of map if index is 0,
   *              otherwise the byte representation of a key</li>
   * </ul>
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]}  for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]}
   * and iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in
   * {@code [1, size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing
   * a warning to users to specify a deterministic key coder.
   */
  static class BatchViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

    /**
     * A {@link DoFn} which groups elements by window boundaries. For each group,
     * the group of elements is transformed into a {@link TransformedMap}.
     * The transformed {@code Map<K, V>} is backed by a {@code Map<K, WindowedValue<V>>}
     * and contains a function {@code WindowedValue<V> -> V}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Value: Transformed map containing a transform that removes the encapsulation
     *              of the window around each value,
     *              {@code Map<K, WindowedValue<V>> -> Map<K, V>}.</li>
     * </ul>
     */
    static class ToMapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
                             IsmRecord<WindowedValue<TransformedMap<K,
                                                     WindowedValue<V>,
                                                     V>>>> {

      private final Coder<W> windowCoder;
      ToMapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c)
          throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Map<K, WindowedValue<V>> map = new HashMap<>();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            c.output(IsmRecord.of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.<V>of(), map))));
            map = new HashMap<>();
          }

          // Verify that the user isn't trying to insert the same key multiple times.
          checkState(!map.containsKey(kv.getValue().getValue().getKey()),
              "Multiple values [%s, %s] found for single key [%s] within window [%s].",
              map.get(kv.getValue().getValue().getKey()),
              kv.getValue().getValue().getValue(),
              kv.getKey());
          map.put(kv.getValue().getValue().getKey(),
                  kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        c.output(IsmRecord.of(
            ImmutableList.of(previousWindow.get()),
            valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.<V>of(), map))));
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMap(DataflowRunner runner, View.AsMap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, V>> apply(PCollection<KV<K, V>> input) {
      return this.<BoundedWindow>applyInternal(input);
    }

    private <W extends BoundedWindow> PCollectionView<Map<K, V>>
        applyInternal(PCollection<KV<K, V>> input) {

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        PCollectionView<Map<K, V>> view = PCollectionViews.mapView(
            input.getPipeline(), input.getWindowingStrategy(), inputCoder);
        return BatchViewAsMultimap.applyForMapLike(runner, input, view, true /* unique keys */);
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);

        // Since the key coder is not deterministic, we convert the map into a singleton
        // and return a singleton view equivalent.
        return applyForSingletonFallback(input);
      }
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsMap";
    }

    /** Transforms the input {@link PCollection} into a singleton {@link Map} per window. */
    private <W extends BoundedWindow> PCollectionView<Map<K, V>>
        applyForSingletonFallback(PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Function<WindowedValue<V>, V>> transformCoder =
          (Coder) SerializableCoder.of(WindowedValueToValue.class);

      Coder<TransformedMap<K, WindowedValue<V>, V>> finalValueCoder =
          TransformedMapCoder.of(
          transformCoder,
          MapCoder.of(
              inputCoder.getKeyCoder(),
              FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder)));

      TransformedMap<K, WindowedValue<V>, V> defaultValue = new TransformedMap<>(
          WindowedValueToValue.<V>of(),
          ImmutableMap.<K, WindowedValue<V>>of());

      return BatchViewAsSingleton.<KV<K, V>,
                                   TransformedMap<K, WindowedValue<V>, V>,
                                   Map<K, V>,
                                   W> applyForSingleton(
          runner,
          input,
          new ToMapDoFn<K, V, W>(windowCoder),
          true,
          defaultValue,
          finalValueCoder);
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMultimap View.AsMultimap} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte
   * representation. Each record is structured as follows:
   * <ul>
   *   <li>Key 1: User key K</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index offset for a given key and window.</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   * <ul>
   *   <li>Key 1: Metadata Key</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index [0, size of map]</li>
   *   <li>Value: variable length long byte representation of size of map if index is 0,
   *              otherwise the byte representation of a key</li>
   * </ul>
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]}  for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]}
   * and iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in
   * {@code [1, size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing
   * a warning to users to specify a deterministic key coder.
   */
  static class BatchViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {
    /**
     * A {@link PTransform} that groups elements by the hash of window's byte representation
     * if the input {@link PCollection} is not within the global window. Otherwise by the hash
     * of the window and key's byte representation. This {@link PTransform} also sorts
     * the values by the combination of the window and key's byte representations.
     */
    private static class GroupByKeyHashAndSortByKeyAndWindow<K, V, W extends BoundedWindow>
        extends PTransform<PCollection<KV<K, V>>,
                           PCollection<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>>> {

      @SystemDoFnInternal
      private static class GroupByKeyHashAndSortByKeyAndWindowDoFn<K, V, W>
          extends DoFn<KV<K, V>, KV<Integer, KV<KV<K, W>, WindowedValue<V>>>> {

        private final IsmRecordCoder<?> coder;
        private GroupByKeyHashAndSortByKeyAndWindowDoFn(IsmRecordCoder<?> coder) {
          this.coder = coder;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow untypedWindow) throws Exception {
          @SuppressWarnings("unchecked")
          W window = (W) untypedWindow;

          c.output(
              KV.of(coder.hash(ImmutableList.of(c.element().getKey())),
                  KV.of(KV.of(c.element().getKey(), window),
                      WindowedValue.of(
                          c.element().getValue(),
                          c.timestamp(),
                          untypedWindow,
                          c.pane()))));
        }
      }

      private final IsmRecordCoder<?> coder;
      public GroupByKeyHashAndSortByKeyAndWindow(IsmRecordCoder<?> coder) {
        this.coder = coder;
      }

      @Override
      public PCollection<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>>
          apply(PCollection<KV<K, V>> input) {

        @SuppressWarnings("unchecked")
        Coder<W> windowCoder = (Coder<W>)
            input.getWindowingStrategy().getWindowFn().windowCoder();
        @SuppressWarnings("unchecked")
        KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();

        PCollection<KV<Integer, KV<KV<K, W>, WindowedValue<V>>>> keyedByHash;
        keyedByHash = input.apply(
            ParDo.of(new GroupByKeyHashAndSortByKeyAndWindowDoFn<K, V, W>(coder)));
        keyedByHash.setCoder(
            KvCoder.of(
                VarIntCoder.of(),
                KvCoder.of(KvCoder.of(inputCoder.getKeyCoder(), windowCoder),
                    FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder))));

        return keyedByHash.apply(
            new GroupByKeyAndSortValuesOnly<Integer, KV<K, W>, WindowedValue<V>>());
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows
     * and keys to locate window and key boundaries. The main output {@link IsmRecord}s have:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Key 2: User key K</li>
     *   <li>Key 3: Index offset for a given key and window.</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     *
     * <p>Additionally, we output all the unique keys per window seen to {@code outputForEntrySet}
     * and the unique key count per window to {@code outputForSize}.
     *
     * <p>Finally, if this {@link DoFn} has been requested to perform unique key checking, it will
     * throw an {@link IllegalStateException} if more than one key per window is found.
     */
    static class ToIsmRecordForMapLikeDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>,
                             IsmRecord<WindowedValue<V>>> {

      private final TupleTag<KV<Integer, KV<W, Long>>> outputForSize;
      private final TupleTag<KV<Integer, KV<W, K>>> outputForEntrySet;
      private final Coder<W> windowCoder;
      private final Coder<K> keyCoder;
      private final IsmRecordCoder<WindowedValue<V>> ismCoder;
      private final boolean uniqueKeysExpected;
      ToIsmRecordForMapLikeDoFn(
          TupleTag<KV<Integer, KV<W, Long>>> outputForSize,
          TupleTag<KV<Integer, KV<W, K>>> outputForEntrySet,
          Coder<W> windowCoder,
          Coder<K> keyCoder,
          IsmRecordCoder<WindowedValue<V>> ismCoder,
          boolean uniqueKeysExpected) {
        this.outputForSize = outputForSize;
        this.outputForEntrySet = outputForEntrySet;
        this.windowCoder = windowCoder;
        this.keyCoder = keyCoder;
        this.ismCoder = ismCoder;
        this.uniqueKeysExpected = uniqueKeysExpected;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        long currentKeyIndex = 0;
        // We use one based indexing while counting
        long currentUniqueKeyCounter = 1;
        Iterator<KV<KV<K, W>, WindowedValue<V>>> iterator = c.element().getValue().iterator();

        KV<KV<K, W>, WindowedValue<V>> currentValue = iterator.next();
        Object currentKeyStructuralValue =
            keyCoder.structuralValue(currentValue.getKey().getKey());
        Object currentWindowStructuralValue =
            windowCoder.structuralValue(currentValue.getKey().getValue());

        while (iterator.hasNext()) {
          KV<KV<K, W>, WindowedValue<V>> nextValue = iterator.next();
          Object nextKeyStructuralValue =
              keyCoder.structuralValue(nextValue.getKey().getKey());
          Object nextWindowStructuralValue =
              windowCoder.structuralValue(nextValue.getKey().getValue());

          outputDataRecord(c, currentValue, currentKeyIndex);

          final long nextKeyIndex;
          final long nextUniqueKeyCounter;

          // Check to see if its a new window
          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            // The next value is a new window, so we output for size the number of unique keys
            // seen and the last key of the window. We also reset the next key index the unique
            // key counter.
            outputMetadataRecordForSize(c, currentValue, currentUniqueKeyCounter);
            outputMetadataRecordForEntrySet(c, currentValue);

            nextKeyIndex = 0;
            nextUniqueKeyCounter = 1;
          } else if (!currentKeyStructuralValue.equals(nextKeyStructuralValue)){
            // It is a new key within the same window so output the key for the entry set,
            // reset the key index and increase the count of unique keys seen within this window.
            outputMetadataRecordForEntrySet(c, currentValue);

            nextKeyIndex = 0;
            nextUniqueKeyCounter = currentUniqueKeyCounter + 1;
          } else if (!uniqueKeysExpected) {
            // It is not a new key so we don't have to output the number of elements in this
            // window or increase the unique key counter. All we do is increase the key index.

            nextKeyIndex = currentKeyIndex + 1;
            nextUniqueKeyCounter = currentUniqueKeyCounter;
          } else {
            throw new IllegalStateException(String.format(
                "Unique keys are expected but found key %s with values %s and %s in window %s.",
                currentValue.getKey().getKey(),
                currentValue.getValue().getValue(),
                nextValue.getValue().getValue(),
                currentValue.getKey().getValue()));
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
          currentKeyStructuralValue = nextKeyStructuralValue;
          currentKeyIndex = nextKeyIndex;
          currentUniqueKeyCounter = nextUniqueKeyCounter;
        }

        outputDataRecord(c, currentValue, currentKeyIndex);
        outputMetadataRecordForSize(c, currentValue, currentUniqueKeyCounter);
        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a record with the number of unique keys seen.
        outputMetadataRecordForEntrySet(c, currentValue);
      }

      /** This outputs the data record. */
      private void outputDataRecord(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value, long keyIndex) {
        IsmRecord<WindowedValue<V>> ismRecord = IsmRecord.of(
            ImmutableList.of(
                value.getKey().getKey(),
                value.getKey().getValue(),
                keyIndex),
            value.getValue());
        c.output(ismRecord);
      }

      /**
       * This outputs records which will be used to compute the number of keys for a given window.
       */
      private void outputMetadataRecordForSize(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value, long uniqueKeyCount) {
        c.sideOutput(outputForSize,
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(),
                                                 value.getKey().getValue())),
                KV.of(value.getKey().getValue(), uniqueKeyCount)));
      }

      /** This outputs records which will be used to construct the entry set. */
      private void outputMetadataRecordForEntrySet(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value) {
        c.sideOutput(outputForEntrySet,
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(),
                                                 value.getKey().getValue())),
                KV.of(value.getKey().getValue(), value.getKey().getKey())));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window of:
       * <ul>
       *   <li>Key 1: META key</li>
       *   <li>Key 2: window</li>
       *   <li>Key 3: 0L (constant)</li>
       *   <li>Value: sum of values for window</li>
       * </ul>
       *
       * <p>This {@link DoFn} is meant to be used to compute the number of unique keys
       * per window for map and multimap side inputs.
       */
    static class ToIsmMetadataRecordForSizeDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, Long>>>, IsmRecord<WindowedValue<V>>> {
      private final Coder<W> windowCoder;
      ToIsmMetadataRecordForSizeDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Iterator<KV<W, Long>> iterator = c.element().getValue().iterator();
        KV<W, Long> currentValue = iterator.next();
        Object currentWindowStructuralValue = windowCoder.structuralValue(currentValue.getKey());
        long size = 0;
        while (iterator.hasNext()) {
          KV<W, Long> nextValue = iterator.next();
          Object nextWindowStructuralValue = windowCoder.structuralValue(nextValue.getKey());

          size += currentValue.getValue();
          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            c.output(IsmRecord.<WindowedValue<V>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
            size = 0;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        size += currentValue.getValue();
        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(IsmRecord.<WindowedValue<V>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window and key pair of:
       * <ul>
       *   <li>Key 1: META key</li>
       *   <li>Key 2: window</li>
       *   <li>Key 3: index offset (1-based index)</li>
       *   <li>Value: key</li>
       * </ul>
       *
       * <p>This {@link DoFn} is meant to be used to output index to key records
       * per window for map and multimap side inputs.
       */
    static class ToIsmMetadataRecordForKeyDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, K>>>, IsmRecord<WindowedValue<V>>> {

      private final Coder<K> keyCoder;
      private final Coder<W> windowCoder;
      ToIsmMetadataRecordForKeyDoFn(Coder<K> keyCoder, Coder<W> windowCoder) {
        this.keyCoder = keyCoder;
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Iterator<KV<W, K>> iterator = c.element().getValue().iterator();
        KV<W, K> currentValue = iterator.next();
        Object currentWindowStructuralValue = windowCoder.structuralValue(currentValue.getKey());
        long elementsInWindow = 1;
        while (iterator.hasNext()) {
          KV<W, K> nextValue = iterator.next();
          Object nextWindowStructuralValue = windowCoder.structuralValue(nextValue.getKey());

          c.output(IsmRecord.<WindowedValue<V>>meta(
              ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
              CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
          elementsInWindow += 1;

          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            elementsInWindow = 1;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(IsmRecord.<WindowedValue<V>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
            CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
      }
    }

    /**
     * A {@link DoFn} which partitions sets of elements by window boundaries. Within each
     * partition, the set of elements is transformed into a {@link TransformedMap}.
     * The transformed {@code Map<K, Iterable<V>>} is backed by a
     * {@code Map<K, Iterable<WindowedValue<V>>>} and contains a function
     * {@code Iterable<WindowedValue<V>> -> Iterable<V>}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Value: Transformed map containing a transform that removes the encapsulation
     *              of the window around each value,
     *              {@code Map<K, Iterable<WindowedValue<V>>> -> Map<K, Iterable<V>>}.</li>
     * </ul>
     */
    static class ToMultimapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
                             IsmRecord<WindowedValue<TransformedMap<K,
                                                                    Iterable<WindowedValue<V>>,
                                                                    Iterable<V>>>>> {

      private final Coder<W> windowCoder;
      ToMultimapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c)
          throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Multimap<K, WindowedValue<V>> multimap = HashMultimap.create();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            @SuppressWarnings({"unchecked", "rawtypes"})
            Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
            c.output(IsmRecord.<WindowedValue<TransformedMap<K,
                                                             Iterable<WindowedValue<V>>,
                                                             Iterable<V>>>>of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(
                    new TransformedMap<>(
                        IterableWithWindowedValuesToIterable.<V>of(), resultMap))));
            multimap = HashMultimap.create();
          }

          multimap.put(kv.getValue().getValue().getKey(),
                       kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
        c.output(IsmRecord.<WindowedValue<TransformedMap<K,
                                                         Iterable<WindowedValue<V>>,
                                                         Iterable<V>>>>of(
            ImmutableList.of(previousWindow.get()),
            valueInEmptyWindows(
                new TransformedMap<>(IterableWithWindowedValuesToIterable.<V>of(), resultMap))));
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMultimap(DataflowRunner runner, View.AsMultimap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      return this.<BoundedWindow>applyInternal(input);
    }

    private <W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>>
        applyInternal(PCollection<KV<K, V>> input) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        PCollectionView<Map<K, Iterable<V>>> view = PCollectionViews.multimapView(
            input.getPipeline(), input.getWindowingStrategy(), inputCoder);

        return applyForMapLike(runner, input, view, false /* unique keys not expected */);
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);

        // Since the key coder is not deterministic, we convert the map into a singleton
        // and return a singleton view equivalent.
        return applyForSingletonFallback(input);
      }
    }

    /** Transforms the input {@link PCollection} into a singleton {@link Map} per window. */
    private <W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>>
        applyForSingletonFallback(PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Function<Iterable<WindowedValue<V>>, Iterable<V>>> transformCoder =
          (Coder) SerializableCoder.of(IterableWithWindowedValuesToIterable.class);

      Coder<TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>>> finalValueCoder =
          TransformedMapCoder.of(
          transformCoder,
          MapCoder.of(
              inputCoder.getKeyCoder(),
              IterableCoder.of(
                  FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder))));

      TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>> defaultValue =
          new TransformedMap<>(
              IterableWithWindowedValuesToIterable.<V>of(),
              ImmutableMap.<K, Iterable<WindowedValue<V>>>of());

      return BatchViewAsSingleton.<KV<K, V>,
                                   TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>>,
                                   Map<K, Iterable<V>>,
                                   W> applyForSingleton(
          runner,
          input,
          new ToMultimapDoFn<K, V, W>(windowCoder),
          true,
          defaultValue,
          finalValueCoder);
    }

    private static <K, V, W extends BoundedWindow, ViewT> PCollectionView<ViewT> applyForMapLike(
        DataflowRunner runner,
        PCollection<KV<K, V>> input,
        PCollectionView<ViewT> view,
        boolean uniqueKeysExpected) throws NonDeterministicException {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      // If our key coder is deterministic, we can use the key portion of each KV
      // part of a composite key containing the window , key and index.
      inputCoder.getKeyCoder().verifyDeterministic();

      IsmRecordCoder<WindowedValue<V>> ismCoder =
          coderForMapLike(windowCoder, inputCoder.getKeyCoder(), inputCoder.getValueCoder());

      // Create the various output tags representing the main output containing the data stream
      // and the side outputs containing the metadata about the size and entry set.
      TupleTag<IsmRecord<WindowedValue<V>>> mainOutputTag = new TupleTag<>();
      TupleTag<KV<Integer, KV<W, Long>>> outputForSizeTag = new TupleTag<>();
      TupleTag<KV<Integer, KV<W, K>>> outputForEntrySetTag = new TupleTag<>();

      // Process all the elements grouped by key hash, and sorted by key and then window
      // outputting to all the outputs defined above.
      PCollectionTuple outputTuple = input
           .apply("GBKaSVForData", new GroupByKeyHashAndSortByKeyAndWindow<K, V, W>(ismCoder))
           .apply(ParDo.of(new ToIsmRecordForMapLikeDoFn<K, V, W>(
                   outputForSizeTag, outputForEntrySetTag,
                   windowCoder, inputCoder.getKeyCoder(), ismCoder, uniqueKeysExpected))
                       .withOutputTags(mainOutputTag,
                                       TupleTagList.of(
                                           ImmutableList.<TupleTag<?>>of(outputForSizeTag,
                                                                         outputForEntrySetTag))));

      // Set the coder on the main data output.
      PCollection<IsmRecord<WindowedValue<V>>> perHashWithReifiedWindows =
          outputTuple.get(mainOutputTag);
      perHashWithReifiedWindows.setCoder(ismCoder);

      // Set the coder on the metadata output for size and process the entries
      // producing a [META, Window, 0L] record per window storing the number of unique keys
      // for each window.
      PCollection<KV<Integer, KV<W, Long>>> outputForSize = outputTuple.get(outputForSizeTag);
      outputForSize.setCoder(
          KvCoder.of(VarIntCoder.of(),
                     KvCoder.of(windowCoder, VarLongCoder.of())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapSizeMetadata = outputForSize
          .apply("GBKaSVForSize", new GroupByKeyAndSortValuesOnly<Integer, W, Long>())
          .apply(ParDo.of(new ToIsmMetadataRecordForSizeDoFn<K, V, W>(windowCoder)));
      windowMapSizeMetadata.setCoder(ismCoder);

      // Set the coder on the metadata output destined to build the entry set and process the
      // entries producing a [META, Window, Index] record per window key pair storing the key.
      PCollection<KV<Integer, KV<W, K>>> outputForEntrySet =
          outputTuple.get(outputForEntrySetTag);
      outputForEntrySet.setCoder(
          KvCoder.of(VarIntCoder.of(),
                     KvCoder.of(windowCoder, inputCoder.getKeyCoder())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapKeysMetadata = outputForEntrySet
          .apply("GBKaSVForKeys", new GroupByKeyAndSortValuesOnly<Integer, W, K>())
          .apply(ParDo.of(
              new ToIsmMetadataRecordForKeyDoFn<K, V, W>(inputCoder.getKeyCoder(), windowCoder)));
      windowMapKeysMetadata.setCoder(ismCoder);

      // Set that all these outputs should be materialized using an indexed format.
      runner.addPCollectionRequiringIndexedFormat(perHashWithReifiedWindows);
      runner.addPCollectionRequiringIndexedFormat(windowMapSizeMetadata);
      runner.addPCollectionRequiringIndexedFormat(windowMapKeysMetadata);

      PCollectionList<IsmRecord<WindowedValue<V>>> outputs =
          PCollectionList.of(ImmutableList.of(
              perHashWithReifiedWindows, windowMapSizeMetadata, windowMapKeysMetadata));

      return Pipeline.applyTransform(outputs,
                                     Flatten.<IsmRecord<WindowedValue<V>>>pCollections())
          .apply(CreatePCollectionView.<IsmRecord<WindowedValue<V>>,
                                        ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsMultimap";
    }

    static <V> IsmRecordCoder<WindowedValue<V>> coderForMapLike(
        Coder<? extends BoundedWindow> windowCoder, Coder<?> keyCoder, Coder<V> valueCoder) {
      // TODO: swap to use a variable length long coder which has values which compare
      // the same as their byte representation compare lexicographically within the key coder
      return IsmRecordCoder.of(
          1, // We use only the key for hashing when producing value records
          2, // Since the key is not present, we add the window to the hash when
             // producing metadata records
          ImmutableList.of(
              MetadataKeyCoder.of(keyCoder),
              windowCoder,
              BigEndianLongCoder.of()),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * A {@code Map<K, V2>} backed by a {@code Map<K, V1>} and a function that transforms
   * {@code V1 -> V2}.
   */
  static class TransformedMap<K, V1, V2>
      extends ForwardingMap<K, V2> {
    private final Function<V1, V2> transform;
    private final Map<K, V1> originalMap;
    private final Map<K, V2> transformedMap;

    private TransformedMap(Function<V1, V2> transform, Map<K, V1> originalMap) {
      this.transform = transform;
      this.originalMap = Collections.unmodifiableMap(originalMap);
      this.transformedMap = Maps.transformValues(originalMap, transform);
    }

    @Override
    protected Map<K, V2> delegate() {
      return transformedMap;
    }
  }

  /**
   * A {@link Coder} for {@link TransformedMap}s.
   */
  static class TransformedMapCoder<K, V1, V2>
      extends StandardCoder<TransformedMap<K, V1, V2>> {
    private final Coder<Function<V1, V2>> transformCoder;
    private final Coder<Map<K, V1>> originalMapCoder;

    private TransformedMapCoder(
        Coder<Function<V1, V2>> transformCoder, Coder<Map<K, V1>> originalMapCoder) {
      this.transformCoder = transformCoder;
      this.originalMapCoder = originalMapCoder;
    }

    public static <K, V1, V2> TransformedMapCoder<K, V1, V2> of(
        Coder<Function<V1, V2>> transformCoder, Coder<Map<K, V1>> originalMapCoder) {
      return new TransformedMapCoder<>(transformCoder, originalMapCoder);
    }

    @JsonCreator
    public static <K, V1, V2> TransformedMapCoder<K, V1, V2> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      checkArgument(components.size() == 2,
          "Expecting 2 components, got " + components.size());
      @SuppressWarnings("unchecked")
      Coder<Function<V1, V2>> transformCoder = (Coder<Function<V1, V2>>) components.get(0);
      @SuppressWarnings("unchecked")
      Coder<Map<K, V1>> originalMapCoder = (Coder<Map<K, V1>>) components.get(1);
      return of(transformCoder, originalMapCoder);
    }

    @Override
    public void encode(TransformedMap<K, V1, V2> value, OutputStream outStream,
        Coder.Context context) throws CoderException, IOException {
      transformCoder.encode(value.transform, outStream, context.nested());
      originalMapCoder.encode(value.originalMap, outStream, context.nested());
    }

    @Override
    public TransformedMap<K, V1, V2> decode(
        InputStream inStream, Coder.Context context) throws CoderException, IOException {
      return new TransformedMap<>(
          transformCoder.decode(inStream, context.nested()),
          originalMapCoder.decode(inStream, context.nested()));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(transformCoder, originalMapCoder);
    }

    @Override
    public void verifyDeterministic()
        throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
      verifyDeterministic("Expected transform coder to be deterministic.", transformCoder);
      verifyDeterministic("Expected map coder to be deterministic.", originalMapCoder);
    }
  }

  /**
   * A {@link Function} which converts {@code WindowedValue<V>} to {@code V}.
   */
  private static class WindowedValueToValue<V> implements
      Function<WindowedValue<V>, V>, Serializable {
    private static final WindowedValueToValue<?> INSTANCE = new WindowedValueToValue<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> WindowedValueToValue<V> of() {
      return (WindowedValueToValue) INSTANCE;
    }

    @Override
    public V apply(WindowedValue<V> input) {
      return input.getValue();
    }
  }

  /**
   * A {@link Function} which converts {@code Iterable<WindowedValue<V>>} to {@code Iterable<V>}.
   */
  private static class IterableWithWindowedValuesToIterable<V> implements
      Function<Iterable<WindowedValue<V>>, Iterable<V>>, Serializable {
    private static final IterableWithWindowedValuesToIterable<?> INSTANCE =
        new IterableWithWindowedValuesToIterable<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> IterableWithWindowedValuesToIterable<V> of() {
      return (IterableWithWindowedValuesToIterable) INSTANCE;
    }

    @Override
    public Iterable<V> apply(Iterable<WindowedValue<V>> input) {
      return Iterables.transform(input, WindowedValueToValue.<V>of());
    }
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
    public PDone apply(PCollection<T> input) {
      if (transform.getSink() instanceof FileBasedSink) {
        FileBasedSink<?> sink = (FileBasedSink<?>) transform.getSink();
        PathValidator validator = runner.options.getPathValidator();
        validator.validateOutputFilePrefixSupported(sink.getBaseOutputFilename());
      }
      return transform.apply(input);
    }
  }

  // ================================================================================
  // PubsubIO translations
  // ================================================================================

  /**
   * Suppress application of {@link PubsubUnboundedSource#apply} in streaming mode so that we
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
    public PCollection<T> apply(PBegin input) {
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
          StreamingPubsubIORead.class, new StreamingPubsubIOReadTranslator());
    }
  }

  /**
   * Rewrite {@link StreamingPubsubIORead} to the appropriate internal node.
   */
  private static class StreamingPubsubIOReadTranslator implements
      TransformTranslator<StreamingPubsubIORead> {
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void translate(
        StreamingPubsubIORead transform,
        TranslationContext context) {
      translateTyped(transform, context);
    }

    private <T> void translateTyped(
        StreamingPubsubIORead<T> transform,
        TranslationContext context) {
      checkArgument(context.getPipelineOptions().isStreaming(),
                    "StreamingPubsubIORead is only for streaming pipelines.");
      PubsubUnboundedSource<T> overriddenTransform = transform.getOverriddenTransform();
      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, "pubsub");
      if (overriddenTransform.getTopic() != null) {
        context.addInput(PropertyNames.PUBSUB_TOPIC,
                         overriddenTransform.getTopic().getV1Beta1Path());
      }
      if (overriddenTransform.getSubscription() != null) {
        context.addInput(
            PropertyNames.PUBSUB_SUBSCRIPTION,
            overriddenTransform.getSubscription().getV1Beta1Path());
      }
      if (overriddenTransform.getTimestampLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_TIMESTAMP_LABEL,
                         overriddenTransform.getTimestampLabel());
      }
      if (overriddenTransform.getIdLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_ID_LABEL, overriddenTransform.getIdLabel());
      }
      context.addValueOnlyOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
  }

  /**
   * Suppress application of {@link PubsubUnboundedSink#apply} in streaming mode so that we
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
    public PDone apply(PCollection<T> input) {
      return PDone.in(input.getPipeline());
    }

    @Override
    protected String getKindString() {
      return "StreamingPubsubIOWrite";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingPubsubIOWrite.class, new StreamingPubsubIOWriteTranslator());
    }
  }

  /**
   * Rewrite {@link StreamingPubsubIOWrite} to the appropriate internal node.
   */
  private static class StreamingPubsubIOWriteTranslator implements
      TransformTranslator<StreamingPubsubIOWrite> {

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void translate(
        StreamingPubsubIOWrite transform,
        TranslationContext context) {
      translateTyped(transform, context);
    }

    private <T> void translateTyped(
        StreamingPubsubIOWrite<T> transform,
        TranslationContext context) {
      checkArgument(context.getPipelineOptions().isStreaming(),
                    "StreamingPubsubIOWrite is only for streaming pipelines.");
      PubsubUnboundedSink<T> overriddenTransform = transform.getOverriddenTransform();
      context.addStep(transform, "ParallelWrite");
      context.addInput(PropertyNames.FORMAT, "pubsub");
      context.addInput(PropertyNames.PUBSUB_TOPIC, overriddenTransform.getTopic().getV1Beta1Path());
      if (overriddenTransform.getTimestampLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_TIMESTAMP_LABEL,
                         overriddenTransform.getTimestampLabel());
      }
      if (overriddenTransform.getIdLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_ID_LABEL, overriddenTransform.getIdLabel());
      }
      context.addEncodingInput(
          WindowedValue.getValueOnlyCoder(overriddenTransform.getElementCoder()));
      context.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));
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
    public final PCollection<T> apply(PInput input) {
      source.validate();

      if (source.requiresDeduping()) {
        return Pipeline.applyTransform(input, new ReadWithIds<T>(source))
            .apply(new Deduplicate<T>());
      } else {
        return Pipeline.applyTransform(input, new ReadWithIds<T>(source))
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
      public final PCollection<ValueWithRecordId<T>> apply(PInput input) {
        return PCollection.<ValueWithRecordId<T>>createPrimitiveOutputInternal(
            input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED);
      }

      @Override
      protected Coder<ValueWithRecordId<T>> getDefaultOutputCoder() {
        return ValueWithRecordId.ValueWithRecordIdCoder.of(source.getDefaultOutputCoder());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("source", source.getClass()));
        builder.include(source);
      }

      public UnboundedSource<T, ?> getSource() {
        return source;
      }
    }

    @Override
    public String getKindString() {
      return "Read(" + approximateSimpleName(source.getClass()) + ")";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          ReadWithIds.class, new ReadWithIdsTranslator());
    }

    private static class ReadWithIdsTranslator
        implements DataflowPipelineTranslator.TransformTranslator<ReadWithIds<?>> {
      @Override
      public void translate(ReadWithIds<?> transform,
          DataflowPipelineTranslator.TranslationContext context) {
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
    public PCollection<T> apply(PCollection<ValueWithRecordId<T>> input) {
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
    public final PCollection<T> apply(PBegin input) {
      source.validate();

      return Pipeline.applyTransform(input, new DataflowUnboundedReadFromBoundedSource<>(source))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
  }

  /**
   * A specialized {@link DoFn} for writing the contents of a {@link PCollection}
   * to a streaming {@link PCollectionView} backend implementation.
   */
  @Deprecated
  public static class StreamingPCollectionViewWriterFn<T>
  extends OldDoFn<Iterable<T>, T> implements OldDoFn.RequiresWindowAccess {
    private final PCollectionView<?> view;
    private final Coder<T> dataCoder;

    public static <T> StreamingPCollectionViewWriterFn<T> create(
        PCollectionView<?> view, Coder<T> dataCoder) {
      return new StreamingPCollectionViewWriterFn<T>(view, dataCoder);
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

    @Override
    public void processElement(ProcessContext c) throws Exception {
      List<WindowedValue<T>> output = new ArrayList<>();
      for (T elem : c.element()) {
        output.add(WindowedValue.of(elem, c.timestamp(), c.window(), c.pane()));
      }

      c.windowingInternals().writePCollectionViewData(
          view.getTagInternal(), output, dataCoder);
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
    public PCollectionView<Map<K, V>> apply(PCollection<KV<K, V>> input) {
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
    public PCollectionView<Map<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
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
    public PCollectionView<List<T>> apply(PCollection<T> input) {
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
    public PCollectionView<Iterable<T>> apply(PCollection<T> input) {
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
    public PCollectionView<T> apply(PCollection<T> input) {
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
    public PCollectionView<OutputT> apply(PCollection<InputT> input) {
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
      return new ArrayList<T>();
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
                         PubsubIO.Read.Bound<?>.PubsubBoundedReader doFn) {
      this.doFn = doFn;
    }

    /**
     * Builds an instance of this class from the overridden doFn.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public UnsupportedIO(DataflowRunner runner,
                         PubsubIO.Write.Bound<?>.PubsubBoundedWriter doFn) {
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
    public OutputT apply(InputT input) {
      String mode = input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()
          ? "streaming" : "batch";
      String name =
          transform == null
              ? approximateSimpleName(doFn.getClass())
              : approximatePTransformName(transform.getClass());
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
