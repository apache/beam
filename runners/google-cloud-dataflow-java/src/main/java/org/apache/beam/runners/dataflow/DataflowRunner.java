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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.clouddebugger.v2.CloudDebugger;
import com.google.api.services.clouddebugger.v2.model.Debuggee;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeRequest;
import com.google.api.services.clouddebugger.v2.model.RegisterDebuggeeResponse;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.WorkerPool;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.runners.core.construction.DeduplicatedFlattenFactory;
import org.apache.beam.runners.core.construction.EmptyFlattenAsCreateFactory;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.runners.core.construction.WriteFilesTranslation;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.StreamingViewOverrides.StreamingCreatePCollectionViewFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.util.DataflowTemplateJob;
import org.apache.beam.runners.dataflow.util.DataflowTransport;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.PackageUtil.StagedFile;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.storage.PathValidator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Utf8;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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
 * <p>When reading from a Dataflow source or writing to a Dataflow sink using {@code
 * DataflowRunner}, the Google cloudservices account and the Google compute engine service account
 * of the GCP project running the Dataflow Job will need access to the corresponding source/sink.
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

  @VisibleForTesting static final int GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT = 1024 * 1024;

  @VisibleForTesting static final String PIPELINE_FILE_NAME = "pipeline.pb";
  @VisibleForTesting static final String DATAFLOW_GRAPH_FILE_NAME = "dataflow_graph.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Use an {@link ObjectMapper} configured with any {@link Module}s in the class path allowing for
   * user specified configuration injection into the ObjectMapper. This supports user custom types
   * on {@link PipelineOptions}.
   */
  private static final ObjectMapper MAPPER_WITH_MODULES =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private final Set<PCollection<?>> pcollectionsRequiringIndexedFormat;

  private final Set<PCollection<?>> pcollectionsRequiringAutoSharding;

  /**
   * Project IDs must contain lowercase letters, digits, or dashes. IDs must start with a letter and
   * may not end with a dash. This regex isn't exact - this allows for patterns that would be
   * rejected by the service, but this is sufficient for basic validation of project IDs.
   */
  public static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]+[a-z0-9]";

  /** Dataflow service endpoints are expected to match this pattern. */
  static final String ENDPOINT_REGEXP = "https://[\\S]*googleapis\\.com[/]?";

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties that configure the runner.
   * @return The newly created runner.
   */
  public static DataflowRunner fromOptions(PipelineOptions options) {
    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (dataflowOptions.getAppName() == null) {
      missing.add("appName");
    }

    if (Strings.isNullOrEmpty(dataflowOptions.getRegion())
        && isServiceEndpoint(dataflowOptions.getDataflowEndpoint())) {
      missing.add("region");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required pipeline options: " + Joiner.on(',').join(missing));
    }

    validateWorkerSettings(PipelineOptionsValidator.validate(GcpOptions.class, options));

    PathValidator validator = dataflowOptions.getPathValidator();
    String gcpTempLocation;
    try {
      gcpTempLocation = dataflowOptions.getGcpTempLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "DataflowRunner requires gcpTempLocation, "
              + "but failed to retrieve a value from PipelineOptions",
          e);
    }
    validator.validateOutputFilePrefixSupported(gcpTempLocation);

    String stagingLocation;
    try {
      stagingLocation = dataflowOptions.getStagingLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "DataflowRunner requires stagingLocation, "
              + "but failed to retrieve a value from PipelineOptions",
          e);
    }
    validator.validateOutputFilePrefixSupported(stagingLocation);

    if (!isNullOrEmpty(dataflowOptions.getSaveProfilesToGcs())) {
      validator.validateOutputFilePrefixSupported(dataflowOptions.getSaveProfilesToGcs());
    }

    if (dataflowOptions.getFilesToStage() != null) {
      // The user specifically requested these files, so fail now if they do not exist.
      // (automatically detected classpath elements are permitted to not exist, so later
      // staging will not fail on nonexistent files)
      dataflowOptions.getFilesToStage().stream()
          .forEach(
              stagedFileSpec -> {
                File localFile;
                if (stagedFileSpec.contains("=")) {
                  String[] components = stagedFileSpec.split("=", 2);
                  localFile = new File(components[1]);
                } else {
                  localFile = new File(stagedFileSpec);
                }
                if (!localFile.exists()) {
                  // should be FileNotFoundException, but for build-time backwards compatibility
                  // cannot add checked exception
                  throw new RuntimeException(
                      String.format("Non-existent files specified in filesToStage: %s", localFile));
                }
              });
    } else {
      dataflowOptions.setFilesToStage(
          detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options));
      if (dataflowOptions.getFilesToStage().isEmpty()) {
        throw new IllegalArgumentException("No files to stage has been found.");
      } else {
        LOG.info(
            "PipelineOptions.filesToStage was not specified. "
                + "Defaulting to files from the classpath: will stage {} files. "
                + "Enable logging at DEBUG level to see which files will be staged.",
            dataflowOptions.getFilesToStage().size());
        LOG.debug("Classpath elements: {}", dataflowOptions.getFilesToStage());
      }
    }

    // Verify jobName according to service requirements, truncating converting to lowercase if
    // necessary.
    String jobName = dataflowOptions.getJobName().toLowerCase();
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
      throw new IllegalArgumentException(
          "Project ID '"
              + project
              + "' invalid. Please make sure you specified the Project ID, not project number.");
    } else if (!project.matches(PROJECT_ID_REGEXP)) {
      throw new IllegalArgumentException(
          "Project ID '"
              + project
              + "' invalid. Please make sure you specified the Project ID, not project description.");
    }

    DataflowPipelineDebugOptions debugOptions =
        dataflowOptions.as(DataflowPipelineDebugOptions.class);
    // Verify the number of worker threads is a valid value
    if (debugOptions.getNumberOfWorkerHarnessThreads() < 0) {
      throw new IllegalArgumentException(
          "Number of worker harness threads '"
              + debugOptions.getNumberOfWorkerHarnessThreads()
              + "' invalid. Please make sure the value is non-negative.");
    }

    if (dataflowOptions.isStreaming() && dataflowOptions.getGcsUploadBufferSizeBytes() == null) {
      dataflowOptions.setGcsUploadBufferSizeBytes(GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT);
    }

    // Adding the Java version to the SDK name for user's and support convenience.
    String javaVersion =
        Float.parseFloat(System.getProperty("java.specification.version")) >= 9
            ? "(JDK 11 environment)"
            : "(JRE 8 environment)";

    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String userAgent =
        String.format(
                "%s/%s%s",
                dataflowRunnerInfo.getName(), dataflowRunnerInfo.getVersion(), javaVersion)
            .replace(" ", "_");
    dataflowOptions.setUserAgent(userAgent);

    return new DataflowRunner(dataflowOptions);
  }

  static boolean isServiceEndpoint(String endpoint) {
    return Strings.isNullOrEmpty(endpoint) || Pattern.matches(ENDPOINT_REGEXP, endpoint);
  }

  @VisibleForTesting
  static void validateWorkerSettings(GcpOptions gcpOptions) {
    Preconditions.checkArgument(
        gcpOptions.getZone() == null || gcpOptions.getWorkerRegion() == null,
        "Cannot use option zone with workerRegion. Prefer either workerZone or workerRegion.");
    Preconditions.checkArgument(
        gcpOptions.getZone() == null || gcpOptions.getWorkerZone() == null,
        "Cannot use option zone with workerZone. Prefer workerZone.");
    Preconditions.checkArgument(
        gcpOptions.getWorkerRegion() == null || gcpOptions.getWorkerZone() == null,
        "workerRegion and workerZone options are mutually exclusive.");

    DataflowPipelineOptions dataflowOptions = gcpOptions.as(DataflowPipelineOptions.class);
    boolean hasExperimentWorkerRegion = false;
    if (dataflowOptions.getExperiments() != null) {
      for (String experiment : dataflowOptions.getExperiments()) {
        if (experiment.startsWith("worker_region")) {
          hasExperimentWorkerRegion = true;
          break;
        }
      }
    }
    Preconditions.checkArgument(
        !hasExperimentWorkerRegion || gcpOptions.getWorkerRegion() == null,
        "Experiment worker_region and option workerRegion are mutually exclusive.");
    Preconditions.checkArgument(
        !hasExperimentWorkerRegion || gcpOptions.getWorkerZone() == null,
        "Experiment worker_region and option workerZone are mutually exclusive.");

    if (gcpOptions.getZone() != null) {
      LOG.warn("Option --zone is deprecated. Please use --workerZone instead.");
      gcpOptions.setWorkerZone(gcpOptions.getZone());
      gcpOptions.setZone(null);
    }
  }

  @VisibleForTesting
  protected DataflowRunner(DataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = DataflowClient.create(options);
    this.translator = DataflowPipelineTranslator.fromOptions(options);
    this.pcollectionsRequiringIndexedFormat = new HashSet<>();
    this.pcollectionsRequiringAutoSharding = new HashSet<>();
    this.ptransformViewsWithNonDeterministicKeyCoders = new HashSet<>();
  }

  private List<PTransformOverride> getOverrides(boolean streaming) {
    boolean fnApiEnabled = hasExperiment(options, "beam_fn_api");
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
    if (!fnApiEnabled) {
      // By default Dataflow runner replaces single-output ParDo with a ParDoSingle override.
      // However, we want a different expansion for single-output splittable ParDo.
      overridesBuilder
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDoSingle(),
                  new ReflectiveOneToOneOverrideFactory(
                      SplittableParDoOverrides.ParDoSingleViaMulti.class, this)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDoMulti(),
                  new SplittableParDoOverrides.SplittableParDoOverrideFactory()));
    }
    if (streaming) {
      if (!hasExperiment(options, "enable_custom_pubsub_source")) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(PubsubUnboundedSource.class),
                new StreamingPubsubIOReadOverrideFactory()));
      }
      if (!hasExperiment(options, "enable_custom_pubsub_sink")) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(PubsubUnboundedSink.class),
                new StreamingPubsubIOWriteOverrideFactory(this)));
      }
      if (fnApiEnabled) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(Create.Values.class),
                new StreamingFnApiCreateOverrideFactory()));
      }
      overridesBuilder.add(
          PTransformOverride.of(
              PTransformMatchers.writeWithRunnerDeterminedSharding(),
              new StreamingShardedWriteFactory(options)));

      overridesBuilder.add(
          PTransformOverride.of(
              PTransformMatchers.groupWithShardableStates(),
              new GroupIntoBatchesOverride.StreamingGroupIntoBatchesOverrideFactory(this)));

      if (!fnApiEnabled
          || ExperimentalOptions.hasExperiment(options, "beam_fn_api_use_deprecated_read")) {
        overridesBuilder
            .add(
                // Streaming Bounded Read is implemented in terms of Streaming Unbounded Read, and
                // must precede it
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(Read.Bounded.class),
                    new StreamingBoundedReadOverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(Read.Unbounded.class),
                    new StreamingUnboundedReadOverrideFactory()));
      }

      if (!fnApiEnabled) {
        overridesBuilder.add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.CreatePCollectionView.class),
                new StreamingCreatePCollectionViewFactory()));
      }
      // Dataflow Streaming runner overrides the SPLITTABLE_PROCESS_KEYED transform
      // natively in the Dataflow service.
    } else {
      overridesBuilder
          // Replace GroupIntoBatches before the state/timer replacements below since
          // GroupIntoBatches internally uses a stateful DoFn.
          .add(
          PTransformOverride.of(
              PTransformMatchers.classEqualTo(GroupIntoBatches.class),
              new GroupIntoBatchesOverride.BatchGroupIntoBatchesOverrideFactory()));

      overridesBuilder
          // State and timer pardos are implemented by expansion to GBK-then-ParDo
          .add(
              PTransformOverride.of(
                  PTransformMatchers.stateOrTimerParDoMulti(),
                  BatchStatefulParDoOverrides.multiOutputOverrideFactory(options)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.stateOrTimerParDoSingle(),
                  BatchStatefulParDoOverrides.singleOutputOverrideFactory(options)));
      // Dataflow Batch runner uses the naive override of the SPLITTABLE_PROCESS_KEYED transform
      // for now, but eventually (when liquid sharding is implemented) will also override it
      // natively in the Dataflow service.
      overridesBuilder.add(
          PTransformOverride.of(
              PTransformMatchers.splittableProcessKeyedBounded(),
              new SplittableParDoNaiveBounded.OverrideFactory()));
      if (!fnApiEnabled) {
        overridesBuilder
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsMap.class),
                    new ReflectiveViewOverrideFactory(
                        BatchViewOverrides.BatchViewAsMap.class, this)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsMultimap.class),
                    new ReflectiveViewOverrideFactory(
                        BatchViewOverrides.BatchViewAsMultimap.class, this)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                    new CombineGloballyAsSingletonViewOverrideFactory(this)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsList.class),
                    new ReflectiveViewOverrideFactory(
                        BatchViewOverrides.BatchViewAsList.class, this)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsIterable.class),
                    new ReflectiveViewOverrideFactory(
                        BatchViewOverrides.BatchViewAsIterable.class, this)));
      }
    }
    /* TODO[Beam-4684]: Support @RequiresStableInput on Dataflow in a more intelligent way
    // Uses Reshuffle, so has to be before the Reshuffle override
    overridesBuilder.add(
        PTransformOverride.of(
            PTransformMatchers.requiresStableInputParDoSingle(),
            RequiresStableInputParDoOverrides.singleOutputOverrideFactory()));
    // Uses Reshuffle, so has to be before the Reshuffle override
    overridesBuilder.add(
        PTransformOverride.of(
            PTransformMatchers.requiresStableInputParDoMulti(),
            RequiresStableInputParDoOverrides.multiOutputOverrideFactory()));
    */
    overridesBuilder
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(Reshuffle.class), new ReshuffleOverrideFactory()))
        // Order is important. Streaming views almost all use Combine internally.
        .add(
            PTransformOverride.of(
                combineValuesTranslation(fnApiEnabled),
                new PrimitiveCombineGroupedValuesOverrideFactory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(ParDo.SingleOutput.class),
                new PrimitiveParDoSingleFactory()));
    return overridesBuilder.build();
  }

  /**
   * Replace the {@link Combine.GloballyAsSingletonView} transform with a specialization which
   * re-applies the {@link CombineFn} and adds a specialization specific to the Dataflow runner.
   */
  private static class CombineGloballyAsSingletonViewOverrideFactory<InputT, ViewT>
      extends ReflectiveViewOverrideFactory<InputT, ViewT> {

    private CombineGloballyAsSingletonViewOverrideFactory(DataflowRunner runner) {
      super((Class) BatchViewOverrides.BatchViewAsSingleton.class, runner);
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PValue> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PValue, PTransform<PCollection<InputT>, PValue>>
            transform) {
      Combine.GloballyAsSingletonView<?, ?> combineTransform =
          (Combine.GloballyAsSingletonView) transform.getTransform();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchViewOverrides.BatchViewAsSingleton(
              runner,
              findCreatePCollectionView(transform),
              (CombineFn) combineTransform.getCombineFn(),
              combineTransform.getFanout()));
    }
  }

  /**
   * Replace the View.AsYYY transform with specialized view overrides for Dataflow. It is required
   * that the new replacement transform uses the supplied PCollectionView and does not create
   * another instance.
   */
  private static class ReflectiveViewOverrideFactory<InputT, ViewT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PValue, PTransform<PCollection<InputT>, PValue>> {

    final Class<PTransform<PCollection<InputT>, PValue>> replacement;
    final DataflowRunner runner;

    private ReflectiveViewOverrideFactory(
        Class<PTransform<PCollection<InputT>, PValue>> replacement, DataflowRunner runner) {
      this.replacement = replacement;
      this.runner = runner;
    }

    CreatePCollectionView<ViewT, ViewT> findCreatePCollectionView(
        final AppliedPTransform<
                PCollection<InputT>, PValue, PTransform<PCollection<InputT>, PValue>>
            transform) {
      final AtomicReference<CreatePCollectionView> viewTransformRef = new AtomicReference<>();
      transform
          .getPipeline()
          .traverseTopologically(
              new PipelineVisitor.Defaults() {
                // Stores whether we have entered the expected composite view transform.
                private boolean tracking = false;

                @Override
                public CompositeBehavior enterCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    tracking = true;
                  }
                  return super.enterCompositeTransform(node);
                }

                @Override
                public void visitPrimitiveTransform(Node node) {
                  if (tracking && node.getTransform() instanceof CreatePCollectionView) {
                    checkState(
                        viewTransformRef.compareAndSet(
                            null, (CreatePCollectionView) node.getTransform()),
                        "Found more than one instance of a CreatePCollectionView when "
                            + "attempting to replace %s, found [%s, %s]",
                        replacement,
                        viewTransformRef.get(),
                        node.getTransform());
                  }
                }

                @Override
                public void leaveCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    tracking = false;
                  }
                }
              });

      checkState(
          viewTransformRef.get() != null,
          "Expected to find CreatePCollectionView contained within %s",
          transform.getTransform());
      return viewTransformRef.get();
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PValue> getReplacementTransform(
        final AppliedPTransform<
                PCollection<InputT>, PValue, PTransform<PCollection<InputT>, PValue>>
            transform) {

      PTransform<PCollection<InputT>, PValue> rep =
          InstanceBuilder.ofType(replacement)
              .withArg(DataflowRunner.class, runner)
              .withArg(CreatePCollectionView.class, findCreatePCollectionView(transform))
              .build();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform), (PTransform) rep);
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PValue newOutput) {
      // We do not replace any of the outputs because we expect that the new PTransform will
      // re-use the original PCollectionView that was returned.
      return ImmutableMap.of();
    }
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

  /**
   * Returns a {@link PTransformMatcher} that matches {@link PTransform}s of class {@link
   * Combine.GroupedValues} that will be translated into CombineValues transforms in Dataflow's Job
   * API and skips those that should be expanded into ParDos.
   *
   * @param fnApiEnabled Flag indicating whether this matcher is being retrieved for a fnapi or
   *     non-fnapi pipeline.
   */
  private static PTransformMatcher combineValuesTranslation(boolean fnApiEnabled) {
    if (fnApiEnabled) {
      return new DataflowPTransformMatchers.CombineValuesWithParentCheckPTransformMatcher();
    } else {
      return new DataflowPTransformMatchers.CombineValuesWithoutSideInputsPTransformMatcher();
    }
  }

  private String debuggerMessage(String projectId, String uniquifier) {
    return String.format(
        "To debug your job, visit Google Cloud Debugger at: "
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

    CloudDebugger debuggerClient = DataflowTransport.newClouddebuggerClient(options).build();
    Debuggee debuggee = registerDebuggee(debuggerClient, uniquifier);
    options.setDebuggee(debuggee);

    System.out.println(debuggerMessage(options.getProject(), debuggee.getUniquifier()));
  }

  private Debuggee registerDebuggee(CloudDebugger debuggerClient, String uniquifier) {
    RegisterDebuggeeRequest registerReq = new RegisterDebuggeeRequest();
    registerReq.setDebuggee(
        new Debuggee()
            .setProject(options.getProject())
            .setUniquifier(uniquifier)
            .setDescription(uniquifier)
            .setAgentVersion("google.com/cloud-dataflow-java/v1"));

    try {
      RegisterDebuggeeResponse registerResponse =
          debuggerClient.controller().debuggees().register(registerReq).execute();
      Debuggee debuggee = registerResponse.getDebuggee();
      if (debuggee.getStatus() != null && debuggee.getStatus().getIsError()) {
        throw new RuntimeException(
            "Unable to register with the debugger: "
                + debuggee.getStatus().getDescription().getFormat());
      }

      return debuggee;
    } catch (IOException e) {
      throw new RuntimeException("Unable to register with the debugger: ", e);
    }
  }

  private List<DataflowPackage> stageArtifacts(RunnerApi.Pipeline pipeline) {
    ImmutableList.Builder<StagedFile> filesToStageBuilder = ImmutableList.builder();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      for (RunnerApi.ArtifactInformation info : entry.getValue().getDependenciesList()) {
        if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE).equals(info.getTypeUrn())) {
          throw new RuntimeException(
              String.format("unsupported artifact type %s", info.getTypeUrn()));
        }
        RunnerApi.ArtifactFilePayload filePayload;
        try {
          filePayload = RunnerApi.ArtifactFilePayload.parseFrom(info.getTypePayload());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Error parsing artifact file payload.", e);
        }
        if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO)
            .equals(info.getRoleUrn())) {
          throw new RuntimeException(
              String.format("unsupported artifact role %s", info.getRoleUrn()));
        }
        RunnerApi.ArtifactStagingToRolePayload stagingPayload;
        try {
          stagingPayload = RunnerApi.ArtifactStagingToRolePayload.parseFrom(info.getRolePayload());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Error parsing artifact staging_to role payload.", e);
        }
        filesToStageBuilder.add(
            StagedFile.of(
                filePayload.getPath(), filePayload.getSha256(), stagingPayload.getStagedName()));
      }
    }
    return options.getStager().stageFiles(filesToStageBuilder.build());
  }

  private List<RunnerApi.ArtifactInformation> getDefaultArtifacts() {
    ImmutableList.Builder<String> pathsToStageBuilder = ImmutableList.builder();
    String windmillBinary =
        options.as(DataflowPipelineDebugOptions.class).getOverrideWindmillBinary();
    String dataflowWorkerJar = options.getDataflowWorkerJar();
    if (dataflowWorkerJar != null && !dataflowWorkerJar.isEmpty()) {
      // Put the user specified worker jar at the start of the classpath, to be consistent with the
      // built in worker order.
      pathsToStageBuilder.add("dataflow-worker.jar=" + dataflowWorkerJar);
    }
    pathsToStageBuilder.addAll(options.getFilesToStage());
    if (windmillBinary != null) {
      pathsToStageBuilder.add("windmill_main=" + windmillBinary);
    }
    return Environments.getArtifacts(pathsToStageBuilder.build());
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    logWarningIfPCollectionViewHasNonDeterministicKeyCoder(pipeline);
    if (containsUnboundedPCollection(pipeline)) {
      options.setStreaming(true);
    }
    replaceTransforms(pipeline);

    LOG.info(
        "Executing pipeline on the Dataflow Service, which will have billing implications "
            + "related to Google Compute Engine usage and other Google Cloud Services.");

    // Capture the sdkComponents for look up during step translations
    SdkComponents sdkComponents = SdkComponents.create();

    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    String workerHarnessContainerImageURL = DataflowRunner.getContainerImageForJob(dataflowOptions);
    RunnerApi.Environment defaultEnvironmentForDataflow =
        Environments.createDockerEnvironment(workerHarnessContainerImageURL);

    sdkComponents.registerEnvironment(
        defaultEnvironmentForDataflow
            .toBuilder()
            .addAllDependencies(getDefaultArtifacts())
            .addAllCapabilities(Environments.getJavaCapabilities())
            .build());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents, true);

    LOG.debug("Portable pipeline proto:\n{}", TextFormat.printToString(pipelineProto));

    List<DataflowPackage> packages = stageArtifacts(pipelineProto);

    // Set a unique client_request_id in the CreateJob request.
    // This is used to ensure idempotence of job creation across retried
    // attempts to create a job. Specifically, if the service returns a job with
    // a different client_request_id, it means the returned one is a different
    // job previously created with the same job name, and that the job creation
    // has been effectively rejected. The SDK should return
    // Error::Already_Exists to user in that case.
    int randomNum = new Random().nextInt(9000) + 1000;
    String requestId =
        DateTimeFormat.forPattern("YYYYMMddHHmmssmmm")
                .withZone(DateTimeZone.UTC)
                .print(DateTimeUtils.currentTimeMillis())
            + "_"
            + randomNum;

    // Try to create a debuggee ID. This must happen before the job is translated since it may
    // update the options.
    maybeRegisterDebuggee(dataflowOptions, requestId);

    JobSpecification jobSpecification =
        translator.translate(pipeline, pipelineProto, sdkComponents, this, packages);

    // Stage the pipeline, retrieving the staged pipeline path, then update
    // the options on the new job
    // TODO: add an explicit `pipeline` parameter to the submission instead of pipeline options
    LOG.info("Staging pipeline description to {}", options.getStagingLocation());
    byte[] serializedProtoPipeline = jobSpecification.getPipelineProto().toByteArray();
    DataflowPackage stagedPipeline =
        options.getStager().stageToFile(serializedProtoPipeline, PIPELINE_FILE_NAME);
    dataflowOptions.setPipelineUrl(stagedPipeline.getLocation());

    if (!isNullOrEmpty(dataflowOptions.getDataflowWorkerJar())) {
      List<String> experiments =
          dataflowOptions.getExperiments() == null
              ? new ArrayList<>()
              : new ArrayList<>(dataflowOptions.getExperiments());
      experiments.add("use_staged_dataflow_worker_jar");
      dataflowOptions.setExperiments(experiments);
    }

    Job newJob = jobSpecification.getJob();
    try {
      newJob
          .getEnvironment()
          .setSdkPipelineOptions(
              MAPPER.readValue(MAPPER_WITH_MODULES.writeValueAsBytes(options), Map.class));
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "PipelineOptions specified failed to serialize to JSON.", e);
    }
    newJob.setClientRequestId(requestId);

    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String version = dataflowRunnerInfo.getVersion();
    checkState(
        !"${pom.version}".equals(version),
        "Unable to submit a job to the Dataflow service with unset version ${pom.version}");
    LOG.info("Dataflow SDK version: {}", version);

    newJob.getEnvironment().setUserAgent((Map) dataflowRunnerInfo.getProperties());
    // The Dataflow Service may write to the temporary directory directly, so
    // must be verified.
    if (!isNullOrEmpty(options.getGcpTempLocation())) {
      newJob
          .getEnvironment()
          .setTempStoragePrefix(
              dataflowOptions.getPathValidator().verifyPath(options.getGcpTempLocation()));
    }
    newJob.getEnvironment().setDataset(options.getTempDatasetId());

    if (options.getWorkerRegion() != null) {
      newJob.getEnvironment().setWorkerRegion(options.getWorkerRegion());
    }
    if (options.getWorkerZone() != null) {
      newJob.getEnvironment().setWorkerZone(options.getWorkerZone());
    }

    if (options.getFlexRSGoal()
        == DataflowPipelineOptions.FlexResourceSchedulingGoal.COST_OPTIMIZED) {
      newJob.getEnvironment().setFlexResourceSchedulingGoal("FLEXRS_COST_OPTIMIZED");
    } else if (options.getFlexRSGoal()
        == DataflowPipelineOptions.FlexResourceSchedulingGoal.SPEED_OPTIMIZED) {
      newJob.getEnvironment().setFlexResourceSchedulingGoal("FLEXRS_SPEED_OPTIMIZED");
    }

    // Represent the minCpuPlatform pipeline option as an experiment, if not already present.
    List<String> experiments =
        firstNonNull(dataflowOptions.getExperiments(), new ArrayList<String>());
    if (!isNullOrEmpty(dataflowOptions.getMinCpuPlatform())) {

      List<String> minCpuFlags =
          experiments.stream()
              .filter(p -> p.startsWith("min_cpu_platform"))
              .collect(Collectors.toList());

      if (minCpuFlags.isEmpty()) {
        experiments.add("min_cpu_platform=" + dataflowOptions.getMinCpuPlatform());
      } else {
        LOG.warn(
            "Flag min_cpu_platform is defined in both top level PipelineOption, "
                + "as well as under experiments. Proceed using {}.",
            minCpuFlags.get(0));
      }
    }

    newJob.getEnvironment().setExperiments(experiments);

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

    // Upload the job to GCS and remove the graph object from the API call.  The graph
    // will be downloaded from GCS by the service.
    if (hasExperiment(options, "upload_graph")) {
      DataflowPackage stagedGraph =
          options
              .getStager()
              .stageToFile(
                  DataflowPipelineTranslator.jobToString(newJob).getBytes(UTF_8),
                  DATAFLOW_GRAPH_FILE_NAME);
      newJob.getSteps().clear();
      newJob.setStepsLocation(stagedGraph.getLocation());
    }

    if (!isNullOrEmpty(options.getDataflowJobFile())
        || !isNullOrEmpty(options.getTemplateLocation())) {
      boolean isTemplate = !isNullOrEmpty(options.getTemplateLocation());
      if (isTemplate) {
        checkArgument(
            isNullOrEmpty(options.getDataflowJobFile()),
            "--dataflowJobFile and --templateLocation are mutually exclusive.");
      }
      String fileLocation =
          firstNonNull(options.getTemplateLocation(), options.getDataflowJobFile());
      checkArgument(
          fileLocation.startsWith("/") || fileLocation.startsWith("gs://"),
          "Location must be local or on Cloud Storage, got %s.",
          fileLocation);
      ResourceId fileResource = FileSystems.matchNewResource(fileLocation, false /* isDirectory */);
      String workSpecJson = DataflowPipelineTranslator.jobToString(newJob);
      try (PrintWriter printWriter =
          new PrintWriter(
              new BufferedWriter(
                  new OutputStreamWriter(
                      Channels.newOutputStream(FileSystems.create(fileResource, MimeTypes.TEXT)),
                      UTF_8)))) {
        printWriter.print(workSpecJson);
        LOG.info("Printed job specification to {}", fileLocation);
      } catch (IOException ex) {
        String error = String.format("Cannot create output file at %s", fileLocation);
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
    if (options.getCreateFromSnapshot() != null && !options.getCreateFromSnapshot().isEmpty()) {
      newJob.setCreatedFromSnapshotId(options.getCreateFromSnapshot());
    }

    Job jobResult;
    try {
      jobResult = dataflowClient.createJob(newJob);
    } catch (GoogleJsonResponseException e) {
      String errorMessages = "Unexpected errors";
      if (e.getDetails() != null) {
        if (Utf8.encodedLength(newJob.toString()) >= CREATE_JOB_REQUEST_LIMIT_BYTES) {
          errorMessages =
              "The size of the serialized JSON representation of the pipeline "
                  + "exceeds the allowable limit. "
                  + "For more information, please see the documentation on job submission:\n"
                  + "https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#jobs";
        } else {
          errorMessages = e.getDetails().getMessage();
        }
      }
      throw new RuntimeException("Failed to create a workflow job: " + errorMessages, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a workflow job", e);
    }

    // Use a raw client for post-launch monitoring, as status calls may fail
    // regularly and need not be retried automatically.
    DataflowPipelineJob dataflowPipelineJob =
        new DataflowPipelineJob(
            DataflowClient.create(options),
            jobResult.getId(),
            options,
            jobSpecification.getStepNames());

    // If the service returned client request id, the SDK needs to compare it
    // with the original id generated in the request, if they are not the same
    // (i.e., the returned job is not created by this request), throw
    // DataflowJobAlreadyExistsException or DataflowJobAlreadyUpdatedException
    // depending on whether this is a reload or not.
    if (jobResult.getClientRequestId() != null
        && !jobResult.getClientRequestId().isEmpty()
        && !jobResult.getClientRequestId().equals(requestId)) {
      // If updating a job.
      if (options.isUpdate()) {
        throw new DataflowJobAlreadyUpdatedException(
            dataflowPipelineJob,
            String.format(
                "The job named %s with id: %s has already been updated into job id: %s "
                    + "and cannot be updated again.",
                newJob.getName(), jobIdToUpdate, jobResult.getId()));
      } else {
        throw new DataflowJobAlreadyExistsException(
            dataflowPipelineJob,
            String.format(
                "There is already an active job named %s with id: %s. If you want "
                    + "to submit a second job, try again by setting a different name using --jobName.",
                newJob.getName(), jobResult.getId()));
      }
    }

    LOG.info(
        "To access the Dataflow monitoring console, please navigate to {}",
        MonitoringUtil.getJobMonitoringPageURL(
            options.getProject(), options.getRegion(), jobResult.getId()));
    LOG.info("Submitted job: {}", jobResult.getId());

    LOG.info(
        "To cancel the job using the 'gcloud' tool, run:\n> {}",
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
    return ImmutableMap.of(
        PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, majorVersion,
        PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);
  }

  // This method is protected to allow a Google internal subclass to properly
  // setup overrides.
  @VisibleForTesting
  protected void replaceTransforms(Pipeline pipeline) {
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

  /** Returns the DataflowPipelineTranslator associated with this object. */
  public DataflowPipelineTranslator getTranslator() {
    return translator;
  }

  /** Sets callbacks to invoke during execution see {@code DataflowRunnerHooks}. */
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
          new PipelineVisitor.Defaults() {
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

      LOG.warn(
          "Unable to use indexed implementation for View.AsMap and View.AsMultimap for {} "
              + "because the key coder is not deterministic. Falling back to singleton implementation "
              + "which may cause memory and/or performance problems. Future major versions of "
              + "Dataflow will require deterministic key coders.",
          ptransformViewNamesWithNonDeterministicKeyCoders);
    }
  }

  /**
   * Returns true if the passed in {@link PCollection} needs to be materialiazed using an indexed
   * format.
   */
  boolean doesPCollectionRequireIndexedFormat(PCollection<?> pcol) {
    return pcollectionsRequiringIndexedFormat.contains(pcol);
  }

  /**
   * Marks the passed in {@link PCollection} as requiring to be materialized using an indexed
   * format.
   */
  void addPCollectionRequiringIndexedFormat(PCollection<?> pcol) {
    pcollectionsRequiringIndexedFormat.add(pcol);
  }

  void maybeRecordPCollectionWithAutoSharding(PCollection<?> pcol) {
    if (hasExperiment(options, "enable_streaming_auto_sharding")
        && !hasExperiment(options, "beam_fn_api")) {
      pcollectionsRequiringAutoSharding.add(pcol);
    }
  }

  boolean doesPCollectionRequireAutoSharding(PCollection<?> pcol) {
    return pcollectionsRequiringAutoSharding.contains(pcol);
  }

  /** A set of {@link View}s with non-deterministic key coders. */
  private Set<PTransform<?, ?>> ptransformViewsWithNonDeterministicKeyCoders;

  /** Records that the {@link PTransform} requires a deterministic key coder. */
  void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    ptransformViewsWithNonDeterministicKeyCoders.add(ptransform);
  }

  // ================================================================================
  // PubsubIO translations
  // ================================================================================

  private static class StreamingPubsubIOReadOverrideFactory
      implements PTransformOverrideFactory<
          PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> {

    @Override
    public PTransformReplacement<PBegin, PCollection<PubsubMessage>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingPubsubIORead(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<PubsubMessage> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Suppress application of {@link PubsubUnboundedSource#expand} in streaming mode so that we can
   * instead defer to Windmill's implementation.
   */
  private static class StreamingPubsubIORead
      extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private final PubsubUnboundedSource transform;

    public StreamingPubsubIORead(PubsubUnboundedSource transform) {
      this.transform = transform;
    }

    PubsubUnboundedSource getOverriddenTransform() {
      return transform;
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
      Coder coder =
          transform.getNeedsMessageId()
              ? new PubsubMessageWithAttributesAndMessageIdCoder()
              : new PubsubMessageWithAttributesCoder();
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, coder);
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

  /** Rewrite {@link StreamingPubsubIORead} to the appropriate internal node. */
  private static class StreamingPubsubIOReadTranslator
      implements TransformTranslator<StreamingPubsubIORead> {

    @Override
    public void translate(StreamingPubsubIORead transform, TranslationContext context) {
      checkArgument(
          context.getPipelineOptions().isStreaming(),
          "StreamingPubsubIORead is only for streaming pipelines.");
      PubsubUnboundedSource overriddenTransform = transform.getOverriddenTransform();
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
      if (overriddenTransform.getTimestampAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, overriddenTransform.getTimestampAttribute());
      }
      if (overriddenTransform.getIdAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_ID_ATTRIBUTE, overriddenTransform.getIdAttribute());
      }
      // In both cases, the transform needs to read PubsubMessage. However, in case it needs
      // the attributes or messageId, we supply an identity "parse fn" so the worker will
      // read PubsubMessage's from Windmill and simply pass them around; and in case it
      // doesn't need attributes, we're already implicitly using a "Coder" that interprets
      // the data as a PubsubMessage's payload.
      if (overriddenTransform.getNeedsAttributes() || overriddenTransform.getNeedsMessageId()) {
        stepContext.addInput(
            PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
            byteArrayToJsonString(serializeToByteArray(new IdentityMessageFn())));
      }
      stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
  }

  private static class IdentityMessageFn extends SimpleFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage input) {
      return input;
    }
  }

  /**
   * Suppress application of {@link PubsubUnboundedSink#expand} in streaming mode so that we can
   * instead defer to Windmill's implementation.
   */
  private static class StreamingPubsubIOWrite
      extends PTransform<PCollection<PubsubMessage>, PDone> {

    private final PubsubUnboundedSink transform;

    /** Builds an instance of this class from the overridden transform. */
    public StreamingPubsubIOWrite(DataflowRunner runner, PubsubUnboundedSink transform) {
      this.transform = transform;
    }

    PubsubUnboundedSink getOverriddenTransform() {
      return transform;
    }

    @Override
    public PDone expand(PCollection<PubsubMessage> input) {
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

  /** Rewrite {@link StreamingPubsubIOWrite} to the appropriate internal node. */
  private static class StreamingPubsubIOWriteTranslator
      implements TransformTranslator<StreamingPubsubIOWrite> {

    @Override
    public void translate(StreamingPubsubIOWrite transform, TranslationContext context) {
      checkArgument(
          context.getPipelineOptions().isStreaming(),
          "StreamingPubsubIOWrite is only for streaming pipelines.");
      PubsubUnboundedSink overriddenTransform = transform.getOverriddenTransform();
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
      if (overriddenTransform.getTimestampAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, overriddenTransform.getTimestampAttribute());
      }
      if (overriddenTransform.getIdAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_ID_ATTRIBUTE, overriddenTransform.getIdAttribute());
      }
      stepContext.addInput(
          PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
          byteArrayToJsonString(serializeToByteArray(new IdentityMessageFn())));
      // No coder is needed in this case since the collection being written is already of
      // PubsubMessage, however the Dataflow backend require a coder to be set.
      stepContext.addEncodingInput(WindowedValue.getValueOnlyCoder(VoidCoder.of()));
      stepContext.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));
    }
  }

  // ================================================================================

  /**
   * A PTranform override factory which maps Create.Values PTransforms for streaming pipelines into
   * a Dataflow specific variant.
   */
  private static class StreamingFnApiCreateOverrideFactory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Create.Values<T>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Create.Values<T>> transform) {
      Create.Values<T> original = transform.getTransform();
      PCollection<T> output =
          (PCollection) Iterables.getOnlyElement(transform.getOutputs().values());
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingFnApiCreate<>(original, output));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.Create.Values
   * Create.Values} for the Dataflow runner in streaming mode over the Fn API.
   */
  private static class StreamingFnApiCreate<T> extends PTransform<PBegin, PCollection<T>> {

    private final Create.Values<T> transform;
    private final transient PCollection<T> originalOutput;

    private StreamingFnApiCreate(Create.Values<T> transform, PCollection<T> originalOutput) {
      this.transform = transform;
      this.originalOutput = originalOutput;
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      try {
        PCollection<T> pc =
            Pipeline.applyTransform(input, Impulse.create())
                .apply(
                    ParDo.of(
                        DecodeAndEmitDoFn.fromIterable(
                            transform.getElements(), originalOutput.getCoder())));
        pc.setCoder(originalOutput.getCoder());
        return pc;
      } catch (IOException e) {
        throw new IllegalStateException("Unable to encode elements.", e);
      }
    }

    /**
     * A DoFn which stores encoded versions of elements and a representation of a Coder capable of
     * decoding those elements.
     *
     * <p>TODO: BEAM-2422 - Make this a SplittableDoFn.
     */
    private static class DecodeAndEmitDoFn<T> extends DoFn<byte[], T> {

      public static <T> DecodeAndEmitDoFn<T> fromIterable(Iterable<T> elements, Coder<T> elemCoder)
          throws IOException {
        ImmutableList.Builder<byte[]> allElementsBytes = ImmutableList.builder();
        for (T element : elements) {
          byte[] bytes = encodeToByteArray(elemCoder, element);
          allElementsBytes.add(bytes);
        }
        return new DecodeAndEmitDoFn<>(allElementsBytes.build(), elemCoder);
      }

      private final Collection<byte[]> elements;
      private final RunnerApi.MessageWithComponents coderSpec;

      // lazily initialized by parsing coderSpec
      private transient Coder<T> coder;

      private Coder<T> getCoder() throws IOException {
        if (coder == null) {
          coder =
              (Coder)
                  CoderTranslation.fromProto(
                      coderSpec.getCoder(),
                      RehydratedComponents.forComponents(coderSpec.getComponents()),
                      TranslationContext.DEFAULT);
        }
        return coder;
      }

      private DecodeAndEmitDoFn(Collection<byte[]> elements, Coder<T> coder) throws IOException {
        this.elements = elements;
        this.coderSpec = CoderTranslation.toProto(coder);
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws IOException {
        for (byte[] element : elements) {
          context.output(CoderUtils.decodeFromByteArray(getCoder(), element));
        }
      }
    }
  }

  private static class ImpulseTranslator implements TransformTranslator<Impulse> {

    @Override
    public void translate(Impulse transform, TranslationContext context) {
      if (context.getPipelineOptions().isStreaming()
          && (!context.isFnApi() || !context.isStreamingEngine())) {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        stepContext.addInput(PropertyNames.FORMAT, "pubsub");
        stepContext.addInput(PropertyNames.PUBSUB_SUBSCRIPTION, "_starting_signal/");
        stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
      } else {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        stepContext.addInput(PropertyNames.FORMAT, "impulse");
        WindowedValue.FullWindowedValueCoder<byte[]> coder =
            WindowedValue.getFullCoder(
                context.getOutput(transform).getCoder(), GlobalWindow.Coder.INSTANCE);
        byte[] encodedImpulse;
        try {
          encodedImpulse = encodeToByteArray(coder, WindowedValue.valueInGlobalWindow(new byte[0]));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        stepContext.addInput(PropertyNames.IMPULSE_ELEMENT, byteArrayToJsonString(encodedImpulse));
        stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
      }
    }
  }

  static {
    DataflowPipelineTranslator.registerTransformTranslator(Impulse.class, new ImpulseTranslator());
  }

  private static class StreamingUnboundedReadOverrideFactory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Read.Unbounded<T>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Read.Unbounded<T>> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingUnboundedRead<>(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.io.Read.Unbounded Read.Unbounded} for
   * the Dataflow runner in streaming mode.
   *
   * <p>In particular, if an UnboundedSource requires deduplication, then features of WindmillSink
   * are leveraged to do the deduplication.
   */
  private static class StreamingUnboundedRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final UnboundedSource<T, ?> source;

    public StreamingUnboundedRead(Read.Unbounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      if (source.requiresDeduping()) {
        return Pipeline.applyTransform(input, new ReadWithIds<>(source)).apply(new Deduplicate<>());
      } else {
        return Pipeline.applyTransform(input, new ReadWithIds<>(source))
            .apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn<>()));
      }
    }

    /**
     * {@link PTransform} that reads {@code (record,recordId)} pairs from an {@link
     * UnboundedSource}.
     */
    private static class ReadWithIds<T>
        extends PTransform<PInput, PCollection<ValueWithRecordId<T>>> {

      private final UnboundedSource<T, ?> source;

      private ReadWithIds(UnboundedSource<T, ?> source) {
        this.source = source;
      }

      @Override
      public final PCollection<ValueWithRecordId<T>> expand(PInput input) {
        return PCollection.createPrimitiveOutputInternal(
            input.getPipeline(),
            WindowingStrategy.globalDefault(),
            IsBounded.UNBOUNDED,
            ValueWithRecordId.ValueWithRecordIdCoder.of(source.getOutputCoder()));
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

    private static class ReadWithIdsTranslator implements TransformTranslator<ReadWithIds<?>> {

      @Override
      public void translate(ReadWithIds<?> transform, TranslationContext context) {
        ReadTranslator.translateReadHelper(transform.getSource(), transform, context);
      }
    }
  }

  /** Remove values with duplicate ids. */
  private static class Deduplicate<T>
      extends PTransform<PCollection<ValueWithRecordId<T>>, PCollection<T>> {

    // Use a finite set of keys to improve bundling.  Without this, the key space
    // will be the space of ids which is potentially very large, which results in much
    // more per-key overhead.
    private static final int NUM_RESHARD_KEYS = 10000;

    @Override
    public PCollection<T> expand(PCollection<ValueWithRecordId<T>> input) {
      return input
          .apply(
              WithKeys.of(
                      (ValueWithRecordId<T> value) ->
                          Arrays.hashCode(value.getId()) % NUM_RESHARD_KEYS)
                  .withKeyType(TypeDescriptors.integers()))
          // Reshuffle will dedup based on ids in ValueWithRecordId by passing the data through
          // WindmillSink.
          .apply(Reshuffle.of())
          .apply(
              "StripIds",
              ParDo.of(
                  new DoFn<KV<Integer, ValueWithRecordId<T>>, T>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      c.output(c.element().getValue().getValue());
                    }
                  }));
    }
  }

  private static class StreamingBoundedReadOverrideFactory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Read.Bounded<T>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Read.Bounded<T>> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingBoundedRead<>(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.io.Read.Bounded Read.Bounded} for the
   * Dataflow runner in streaming mode.
   */
  private static class StreamingBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final BoundedSource<T> source;

    public StreamingBoundedRead(Read.Bounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      return Pipeline.applyTransform(input, new UnboundedReadFromBoundedSource<>(source))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
  }

  /**
   * A marker {@link DoFn} for writing the contents of a {@link PCollection} to a streaming {@link
   * PCollectionView} backend implementation.
   */
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

  /** Finds the id for the running job of the given name. */
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
              + (e.getDetails() != null ? e.getDetails().getMessage() : e),
          e);
    } catch (IOException e) {
      throw new RuntimeException("Got error while looking up jobs: ", e);
    }

    throw new IllegalArgumentException("Could not find running job named " + jobName);
  }

  static class CombineGroupedValues<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final Combine.GroupedValues<K, InputT, OutputT> original;
    private final Coder<KV<K, OutputT>> outputCoder;

    CombineGroupedValues(
        GroupedValues<K, InputT, OutputT> original, Coder<KV<K, OutputT>> outputCoder) {
      this.original = original;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<InputT>>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), outputCoder);
    }

    public Combine.GroupedValues<K, InputT, OutputT> getOriginalCombine() {
      return original;
    }
  }

  private static class PrimitiveCombineGroupedValuesOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, Iterable<InputT>>>,
          PCollection<KV<K, OutputT>>,
          Combine.GroupedValues<K, InputT, OutputT>> {

    @Override
    public PTransformReplacement<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, Iterable<InputT>>>,
                    PCollection<KV<K, OutputT>>,
                    GroupedValues<K, InputT, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new CombineGroupedValues<>(
              transform.getTransform(),
              PTransformReplacements.getSingletonMainOutput(transform).getCoder()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<KV<K, OutputT>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class StreamingPubsubIOWriteOverrideFactory
      implements PTransformOverrideFactory<PCollection<PubsubMessage>, PDone, PubsubUnboundedSink> {

    private final DataflowRunner runner;

    private StreamingPubsubIOWriteOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<PubsubMessage>, PDone> getReplacementTransform(
        AppliedPTransform<PCollection<PubsubMessage>, PDone, PubsubUnboundedSink> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingPubsubIOWrite(runner, transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PDone newOutput) {
      return Collections.emptyMap();
    }
  }

  @VisibleForTesting
  static class StreamingShardedWriteFactory<UserT, DestinationT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<UserT>,
          WriteFilesResult<DestinationT>,
          WriteFiles<UserT, DestinationT, OutputT>> {

    // We pick 10 as a a default, as it works well with the default number of workers started
    // by Dataflow.
    static final int DEFAULT_NUM_SHARDS = 10;
    DataflowPipelineWorkerPoolOptions options;

    StreamingShardedWriteFactory(PipelineOptions options) {
      this.options = options.as(DataflowPipelineWorkerPoolOptions.class);
    }

    @Override
    public PTransformReplacement<PCollection<UserT>, WriteFilesResult<DestinationT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<UserT>,
                    WriteFilesResult<DestinationT>,
                    WriteFiles<UserT, DestinationT, OutputT>>
                transform) {
      // By default, if numShards is not set WriteFiles will produce one file per bundle. In
      // streaming, there are large numbers of small bundles, resulting in many tiny files.
      // Instead we pick max workers * 2 to ensure full parallelism, but prevent too-many files.
      // (current_num_workers * 2 might be a better choice, but that value is not easily available
      // today).
      // If the user does not set either numWorkers or maxNumWorkers, default to 10 shards.
      int numShards;
      if (options.getMaxNumWorkers() > 0) {
        numShards = options.getMaxNumWorkers() * 2;
      } else if (options.getNumWorkers() > 0) {
        numShards = options.getNumWorkers() * 2;
      } else {
        numShards = DEFAULT_NUM_SHARDS;
      }

      try {
        List<PCollectionView<?>> sideInputs =
            WriteFilesTranslation.getDynamicDestinationSideInputs(transform);
        FileBasedSink sink = WriteFilesTranslation.getSink(transform);
        WriteFiles<UserT, DestinationT, OutputT> replacement =
            WriteFiles.to(sink).withSideInputs(sideInputs);
        if (WriteFilesTranslation.isWindowedWrites(transform)) {
          replacement = replacement.withWindowedWrites();
        }
        return PTransformReplacement.of(
            PTransformReplacements.getSingletonMainInput(transform),
            replacement.withNumShards(numShards));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, WriteFilesResult<DestinationT> newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  @VisibleForTesting
  static String getContainerImageForJob(DataflowPipelineOptions options) {
    String workerHarnessContainerImage = options.getWorkerHarnessContainerImage();

    String javaVersionId =
        Float.parseFloat(System.getProperty("java.specification.version")) >= 9 ? "java11" : "java";
    if (!workerHarnessContainerImage.contains("IMAGE")) {
      return workerHarnessContainerImage;
    } else if (hasExperiment(options, "beam_fn_api")) {
      return workerHarnessContainerImage.replace("IMAGE", "java");
    } else if (options.isStreaming()) {
      return workerHarnessContainerImage.replace(
          "IMAGE", String.format("beam-%s-streaming", javaVersionId));
    } else {
      return workerHarnessContainerImage.replace(
          "IMAGE", String.format("beam-%s-batch", javaVersionId));
    }
  }

  static void verifyDoFnSupportedBatch(DoFn<?, ?> fn) {
    verifyDoFnSupported(fn, false);
  }

  static void verifyDoFnSupportedStreaming(DoFn<?, ?> fn) {
    verifyDoFnSupported(fn, true);
  }

  static void verifyDoFnSupported(DoFn<?, ?> fn, boolean streaming) {
    if (DoFnSignatures.usesSetState(fn)) {
      // https://issues.apache.org/jira/browse/BEAM-1479
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s",
              DataflowRunner.class.getSimpleName(), SetState.class.getSimpleName()));
    }
    if (DoFnSignatures.usesMapState(fn)) {
      // https://issues.apache.org/jira/browse/BEAM-1474
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s",
              DataflowRunner.class.getSimpleName(), MapState.class.getSimpleName()));
    }
    if (DoFnSignatures.usesOrderedListState(fn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s",
              DataflowRunner.class.getSimpleName(), OrderedListState.class.getSimpleName()));
    }
    if (streaming && DoFnSignatures.requiresTimeSortedInput(fn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support @RequiresTimeSortedInput in streaming mode.",
              DataflowRunner.class.getSimpleName()));
    }
  }

  static void verifyStateSupportForWindowingStrategy(WindowingStrategy strategy) {
    // https://issues.apache.org/jira/browse/BEAM-2507
    if (!strategy.getWindowFn().isNonMerging()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support state or timers with merging windows",
              DataflowRunner.class.getSimpleName()));
    }
  }
}
