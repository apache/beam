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

import static org.apache.beam.runners.dataflow.util.Structs.addBoolean;
import static org.apache.beam.runners.dataflow.util.Structs.addDictionary;
import static org.apache.beam.runners.dataflow.util.Structs.addList;
import static org.apache.beam.runners.dataflow.util.Structs.addLong;
import static org.apache.beam.runners.dataflow.util.Structs.addObject;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.model.AutoscalingSettings;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.DebugOptions;
import com.google.api.services.dataflow.model.Disk;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.Step;
import com.google.api.services.dataflow.model.WorkerPool;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.BatchViewOverrides.GroupByKeyAndSortValuesOnly;
import org.apache.beam.runners.dataflow.DataflowRunner.CombineGroupedValues;
import org.apache.beam.runners.dataflow.PrimitiveParDoSingleFactory.ParDoSingle;
import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.internal.DataflowGroupByKey;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.OutputReference;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHint;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.PercentCodec;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataflowPipelineTranslator} knows how to translate {@link Pipeline} objects into Cloud
 * Dataflow Service API {@link Job}s.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@VisibleForTesting
public class DataflowPipelineTranslator {

  // Must be kept in sync with their internal counterparts.
  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineTranslator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static byte[] serializeWindowingStrategy(
      WindowingStrategy<?, ?> windowingStrategy, PipelineOptions options) {
    try {
      SdkComponents sdkComponents = SdkComponents.create();

      String workerHarnessContainerImageURL =
          DataflowRunner.getContainerImageForJob(options.as(DataflowPipelineOptions.class));
      RunnerApi.Environment defaultEnvironmentForDataflow =
          Environments.createDockerEnvironment(workerHarnessContainerImageURL);
      sdkComponents.registerEnvironment(defaultEnvironmentForDataflow);

      return WindowingStrategyTranslation.toMessageProto(windowingStrategy, sdkComponents)
          .toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Unable to format windowing strategy %s as bytes", windowingStrategy), e);
    }
  }

  /**
   * A map from {@link PTransform} subclass to the corresponding {@link TransformTranslator} to use
   * to translate that transform.
   *
   * <p>A static map that contains system-wide defaults.
   */
  private static Map<Class, TransformTranslator> transformTranslators = new HashMap<>();

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /**
   * Constructs a translator from the provided options.
   *
   * @param options Properties that configure the translator.
   * @return The newly created translator.
   */
  public static DataflowPipelineTranslator fromOptions(DataflowPipelineOptions options) {
    return new DataflowPipelineTranslator(options);
  }

  private DataflowPipelineTranslator(DataflowPipelineOptions options) {
    this.options = options;
  }

  /** Translates a {@link Pipeline} into a {@code JobSpecification}. */
  public JobSpecification translate(
      Pipeline pipeline,
      RunnerApi.Pipeline pipelineProto,
      SdkComponents sdkComponents,
      DataflowRunner runner,
      List<DataflowPackage> packages) {
    Translator translator = new Translator(pipeline, runner, sdkComponents);
    Job result = translator.translate(packages);
    return new JobSpecification(
        result, pipelineProto, Collections.unmodifiableMap(translator.stepNames));
  }

  /**
   * The result of a job translation.
   *
   * <p>Used to pass the result {@link Job} and any state that was used to construct the job that
   * may be of use to other classes (eg the {@link PTransform} to StepName mapping).
   */
  public static class JobSpecification {

    private final Job job;
    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;
    private final RunnerApi.Pipeline pipelineProto;

    public JobSpecification(
        Job job,
        RunnerApi.Pipeline pipelineProto,
        Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
      this.job = job;
      this.pipelineProto = pipelineProto;
      this.stepNames = stepNames;
    }

    public Job getJob() {
      return job;
    }

    public RunnerApi.Pipeline getPipelineProto() {
      return pipelineProto;
    }

    /**
     * Returns the mapping of {@link AppliedPTransform AppliedPTransforms} to the internal step name
     * for that {@code AppliedPTransform}.
     */
    public Map<AppliedPTransform<?, ?, ?>, String> getStepNames() {
      return stepNames;
    }
  }

  /** Renders a {@link Job} as a string. */
  public static String jobToString(Job job) {
    try {
      return MAPPER.writeValueAsString(job);
    } catch (JsonProcessingException exc) {
      throw new IllegalStateException("Failed to render Job as String.", exc);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Records that instances of the specified PTransform class should be translated by default by the
   * corresponding {@link TransformTranslator}.
   */
  public static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }

  /**
   * Returns the {@link TransformTranslator} to use for instances of the specified PTransform class,
   * or null if none registered.
   */
  public <TransformT extends PTransform> TransformTranslator<TransformT> getTransformTranslator(
      Class<TransformT> transformClass) {
    return transformTranslators.get(transformClass);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Translates a Pipeline into the Dataflow representation.
   *
   * <p>For internal use only.
   */
  class Translator extends PipelineVisitor.Defaults implements TranslationContext {

    /** The Pipeline to translate. */
    private final Pipeline pipeline;

    /** The runner which will execute the pipeline. */
    private final DataflowRunner runner;

    /** The Cloud Dataflow Job representation. */
    private final Job job = new Job();

    /** A Map from AppliedPTransform to their unique Dataflow step names. */
    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap<>();

    /**
     * A Map from {@link PValue} to the {@link AppliedPTransform} that produces that {@link PValue}.
     */
    private final Map<PValue, AppliedPTransform<?, ?, ?>> producers = new HashMap<>();

    /** A Map from PValues to their output names used by their producer Dataflow steps. */
    private final Map<PValue, String> outputNames = new HashMap<>();

    /** A Map from PValues to the Coders used for them. */
    private final Map<PValue, Coder<?>> outputCoders = new HashMap<>();

    /**
     * The component maps of the portable pipeline, so they can be referred to by id in the output
     * of translation.
     */
    private final SdkComponents sdkComponents;

    /** The transform currently being applied. */
    private AppliedPTransform<?, ?, ?> currentTransform;

    /** A stack of all composite PTransforms encompassing the current transform. */
    private ArrayDeque<TransformHierarchy.Node> parents = new ArrayDeque<>();

    /** Constructs a Translator that will translate the specified Pipeline into Dataflow objects. */
    public Translator(Pipeline pipeline, DataflowRunner runner, SdkComponents sdkComponents) {
      this.pipeline = pipeline;
      this.runner = runner;
      this.sdkComponents = sdkComponents;
    }

    /**
     * Translates this Translator's pipeline onto its writer.
     *
     * @return a Job definition filled in with the type of job, the environment, and the job steps.
     */
    public Job translate(List<DataflowPackage> packages) {
      job.setName(options.getJobName().toLowerCase());

      Environment environment = new Environment();
      job.setEnvironment(environment);
      job.getEnvironment().setServiceOptions(options.getDataflowServiceOptions());

      WorkerPool workerPool = new WorkerPool();

      // If streaming engine is enabled set the proper experiments so that it is enabled on the
      // back end as well.  If streaming engine is not enabled make sure the experiments are also
      // not enabled.
      if (options.isEnableStreamingEngine()) {
        List<String> experiments = options.getExperiments();
        if (experiments == null) {
          experiments = new ArrayList<String>();
        } else {
          experiments = new ArrayList<String>(experiments);
        }
        if (!experiments.contains(GcpOptions.STREAMING_ENGINE_EXPERIMENT)) {
          experiments.add(GcpOptions.STREAMING_ENGINE_EXPERIMENT);
        }
        if (!experiments.contains(GcpOptions.WINDMILL_SERVICE_EXPERIMENT)) {
          experiments.add(GcpOptions.WINDMILL_SERVICE_EXPERIMENT);
        }
        options.setExperiments(experiments);
      } else {
        List<String> experiments = options.getExperiments();
        if (experiments != null) {
          if (experiments.contains(GcpOptions.STREAMING_ENGINE_EXPERIMENT)
              || experiments.contains(GcpOptions.WINDMILL_SERVICE_EXPERIMENT)) {
            throw new IllegalArgumentException(
                "Streaming engine both disabled and enabled: enableStreamingEngine is set to"
                    + " false, but enable_windmill_service and/or enable_streaming_engine are"
                    + " present. It is recommended you only set enableStreamingEngine.");
          }
        }
      }

      if (options.isStreaming()) {
        job.setType("JOB_TYPE_STREAMING");
      } else {
        job.setType("JOB_TYPE_BATCH");
        workerPool.setDiskType(options.getWorkerDiskType());
      }

      if (options.getWorkerMachineType() != null) {
        workerPool.setMachineType(options.getWorkerMachineType());
      }

      if (options.getUsePublicIps() != null) {
        if (options.getUsePublicIps()) {
          workerPool.setIpConfiguration("WORKER_IP_PUBLIC");
        } else {
          workerPool.setIpConfiguration("WORKER_IP_PRIVATE");
        }
      }
      workerPool.setPackages(packages);
      workerPool.setNumWorkers(options.getNumWorkers());

      if (options.getMaxNumWorkers() != 0 && options.getNumWorkers() > options.getMaxNumWorkers()) {
        throw new IllegalArgumentException(
            String.format(
                "numWorkers (%d) cannot exceed maxNumWorkers (%d).",
                options.getNumWorkers(), options.getMaxNumWorkers()));
      }

      if (options.getLabels() != null) {
        job.setLabels(options.getLabels());
      }
      if (options.isStreaming() && !hasExperiment(options, "enable_windmill_service")) {
        // Use separate data disk for streaming.
        Disk disk = new Disk();
        disk.setDiskType(options.getWorkerDiskType());
        workerPool.setDataDisks(Collections.singletonList(disk));
      }
      if (!isNullOrEmpty(options.getZone())) {
        workerPool.setZone(options.getZone());
      }
      if (!isNullOrEmpty(options.getNetwork())) {
        workerPool.setNetwork(options.getNetwork());
      }
      if (!isNullOrEmpty(options.getSubnetwork())) {
        workerPool.setSubnetwork(options.getSubnetwork());
      }
      if (options.getDiskSizeGb() > 0) {
        workerPool.setDiskSizeGb(options.getDiskSizeGb());
      }
      AutoscalingSettings settings = new AutoscalingSettings();
      if (options.getAutoscalingAlgorithm() != null) {
        settings.setAlgorithm(options.getAutoscalingAlgorithm().getAlgorithm());
      }
      settings.setMaxNumWorkers(options.getMaxNumWorkers());
      workerPool.setAutoscalingSettings(settings);

      List<WorkerPool> workerPools = new ArrayList<>();

      workerPools.add(workerPool);
      environment.setWorkerPools(workerPools);

      if (options.getServiceAccount() != null) {
        environment.setServiceAccountEmail(options.getServiceAccount());
      }
      if (options.getDataflowKmsKey() != null) {
        environment.setServiceKmsKeyName(options.getDataflowKmsKey());
      }
      if (options.isHotKeyLoggingEnabled() || hasExperiment(options, "enable_hot_key_logging")) {
        DebugOptions debugOptions = new DebugOptions();
        debugOptions.setEnableHotKeyLogging(true);
        environment.setDebugOptions(debugOptions);
      }

      if (DataflowRunner.useUnifiedWorker(options)) {
        LOG.info("Skipping v1 pipeline translation since this job will run on v2.");
      } else {
        pipeline.traverseTopologically(this);
      }

      return job;
    }

    @Override
    public DataflowPipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public <InputT extends PInput> Map<TupleTag<?>, PCollection<?>> getInputs(
        PTransform<InputT, ?> transform) {
      return getCurrentTransform(transform).getInputs();
    }

    @Override
    public <InputT extends PValue> InputT getInput(PTransform<InputT, ?> transform) {
      return (InputT)
          Iterables.getOnlyElement(
              TransformInputs.nonAdditionalInputs(getCurrentTransform(transform)));
    }

    @Override
    public <OutputT extends POutput> Map<TupleTag<?>, PCollection<?>> getOutputs(
        PTransform<?, OutputT> transform) {
      return getCurrentTransform(transform).getOutputs();
    }

    @Override
    public <OutputT extends PValue> OutputT getOutput(PTransform<?, OutputT> transform) {
      return (OutputT) Iterables.getOnlyElement(getOutputs(transform).values());
    }

    @Override
    public String getFullName(PTransform<?, ?> transform) {
      return getCurrentTransform(transform).getFullName();
    }

    @Override
    public AppliedPTransform<?, ?, ?> getCurrentTransform() {
      return currentTransform;
    }

    private AppliedPTransform<?, ?, ?> getCurrentTransform(PTransform<?, ?> transform) {
      checkArgument(
          currentTransform != null && currentTransform.getTransform() == transform,
          "can only be called with current transform");
      return currentTransform;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator = getTransformTranslator(transform.getClass());
      checkState(
          translator != null,
          "no translator registered for primitive transform %s at node %s",
          transform,
          node.getFullName());
      LOG.debug("Translating {}", transform);
      currentTransform = node.toAppliedPTransform(getPipeline());
      ResourceHints hints = transform.getResourceHints();
      // AppliedPTransform instance stores resource hints of current transform merged with outer
      // hints (e.g. set on outer composites).
      // Translation reads resource hints from PTransform objects, so update the hints.
      transform.setResourceHints(currentTransform.getResourceHints());
      translator.translate(transform, this);
      // Avoid side-effects in case the same transform is applied in multiple places in the
      // pipeline.
      transform.setResourceHints(hints);
      // translator.translate(node, this);
      currentTransform = null;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      LOG.debug("Checking translation of {}", value);
      // Primitive transforms are the only ones assigned step names.
      if (producer.getTransform() instanceof CreateDataflowView) {
        // CreateDataflowView produces a dummy output (as it must be a primitive transform)
        // but in the Dataflow Job graph produces only the view and not the output PCollection.
        asOutputReference(
            ((CreateDataflowView) producer.getTransform()).getView(),
            producer.toAppliedPTransform(getPipeline()));
        return;
      }
      asOutputReference(value, producer.toAppliedPTransform(getPipeline()));
    }

    @Override
    public StepTranslator addStep(PTransform<?, ?> transform, String type) {
      String stepName = genStepName();
      if (stepNames.put(getCurrentTransform(transform), stepName) != null) {
        throw new IllegalArgumentException(transform + " already has a name specified");
      }
      // Start the next "steps" list item.
      List<Step> steps = job.getSteps();
      if (steps == null) {
        steps = new ArrayList<>();
        job.setSteps(steps);
      }

      Step step = new Step();
      step.setName(stepName);
      step.setKind(type);
      steps.add(step);

      StepTranslator stepContext = new StepTranslator(this, step);
      stepContext.addInput(PropertyNames.USER_NAME, getFullName(transform));
      stepContext.addDisplayData(transform);
      stepContext.addResourceHints(transform.getResourceHints());
      LOG.info("Adding {} as step {}", getCurrentTransform(transform).getFullName(), stepName);
      return stepContext;
    }

    @Override
    public OutputReference asOutputReference(PValue value, AppliedPTransform<?, ?, ?> producer) {
      String stepName = stepNames.get(producer);
      checkArgument(stepName != null, "%s doesn't have a name specified", producer);

      String outputName = outputNames.get(value);
      checkArgument(outputName != null, "output %s doesn't have a name specified", value);

      return new OutputReference(stepName, outputName);
    }

    @Override
    public SdkComponents getSdkComponents() {
      return sdkComponents;
    }

    @Override
    public AppliedPTransform<?, ?, ?> getProducer(PValue value) {
      return checkNotNull(
          producers.get(value),
          "Unknown producer for value %s while translating step %s",
          value,
          currentTransform.getFullName());
    }

    /** Returns a fresh Dataflow step name. */
    private String genStepName() {
      return "s" + (stepNames.size() + 1);
    }

    /** Records the name of the given output PValue, within its producing transform. */
    private void registerOutputName(PValue value, String name) {
      if (outputNames.put(value, name) != null) {
        throw new IllegalArgumentException("output " + value + " already has a name specified");
      }
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      if (!node.isRootNode()) {
        parents.addFirst(node);
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
      if (!node.isRootNode()) {
        parents.removeFirst();
      }
    }

    @Override
    public AppliedPTransform<?, ?, ?> getCurrentParent() {
      if (parents.isEmpty()) {
        return null;
      } else {
        return parents.peekFirst().toAppliedPTransform(getPipeline());
      }
    }
  }

  static class StepTranslator implements StepTranslationContext {

    private final Translator translator;
    private final Step step;
    // For compatibility with URL encoding implementations that represent space as +,
    // always encode + as %2b even though we don't encode space as +.
    private final PercentCodec percentCodec =
        new PercentCodec("+".getBytes(StandardCharsets.US_ASCII), false);

    private StepTranslator(Translator translator, Step step) {
      this.translator = translator;
      this.step = step;
    }

    private Map<String, Object> getProperties() {
      return DataflowPipelineTranslator.getProperties(step);
    }

    @Override
    public void addEncodingInput(Coder<?> coder) {
      CloudObject encoding = translateCoder(coder);
      addObject(getProperties(), PropertyNames.ENCODING, encoding);
    }

    @Override
    public void addInput(String name, Boolean value) {
      addBoolean(getProperties(), name, value);
    }

    @Override
    public void addInput(String name, String value) {
      addString(getProperties(), name, value);
    }

    @Override
    public void addInput(String name, Long value) {
      addLong(getProperties(), name, value);
    }

    @Override
    public void addInput(String name, Map<String, Object> elements) {
      addDictionary(getProperties(), name, elements);
    }

    @Override
    public void addInput(String name, List<? extends Map<String, Object>> elements) {
      addList(getProperties(), name, elements);
    }

    @Override
    public void addInput(String name, PInput value) {
      if (value instanceof PValue) {
        PValue pvalue = (PValue) value;
        addInput(name, translator.asOutputReference(pvalue, translator.getProducer(pvalue)));
      } else {
        throw new IllegalStateException("Input must be a PValue");
      }
    }

    @Override
    public void addOutput(String name, PCollection<?> value) {
      translator.producers.put(value, translator.currentTransform);
      // Wrap the PCollection element Coder inside a WindowedValueCoder.
      Coder<?> coder =
          WindowedValue.getFullCoder(
              value.getCoder(), value.getWindowingStrategy().getWindowFn().windowCoder());
      addOutput(name, value, coder);
    }

    @Override
    public void addCollectionToSingletonOutput(
        PCollection<?> inputValue, String outputName, PCollectionView<?> outputValue) {
      translator.producers.put(outputValue, translator.currentTransform);
      Coder<?> inputValueCoder = checkNotNull(translator.outputCoders.get(inputValue));
      // The inputValueCoder for the input PCollection should be some
      // WindowedValueCoder of the input PCollection's element
      // coder.
      checkState(inputValueCoder instanceof WindowedValue.WindowedValueCoder);
      // The outputValueCoder for the output should be an
      // IterableCoder of the inputValueCoder. This is a property
      // of the backend "CollectionToSingleton" step.
      Coder<?> outputValueCoder = IterableCoder.of(inputValueCoder);
      addOutput(outputName, outputValue, outputValueCoder);
    }

    /**
     * Adds an output with the given name to the previously added Dataflow step, producing the
     * specified output {@code PValue} with the given {@code Coder} (if not {@code null}).
     */
    private void addOutput(String name, PValue value, Coder<?> valueCoder) {
      translator.registerOutputName(value, name);

      // If the output requires runner determined sharding, also append necessary input properties.
      if (value instanceof PCollection) {
        if (translator.runner.doesPCollectionRequireAutoSharding((PCollection<?>) value)) {
          addInput(PropertyNames.ALLOWS_SHARDABLE_STATE, "true");
        }
        if (translator.runner.doesPCollectionPreserveKeys((PCollection<?>) value)) {
          addInput(PropertyNames.PRESERVES_KEYS, "true");
        }
      }

      Map<String, Object> properties = getProperties();
      @Nullable List<Map<String, Object>> outputInfoList = null;
      try {
        // TODO: This should be done via a Structs accessor.
        outputInfoList = (List<Map<String, Object>>) properties.get(PropertyNames.OUTPUT_INFO);
      } catch (Exception e) {
        throw new RuntimeException("Inconsistent dataflow pipeline translation", e);
      }
      if (outputInfoList == null) {
        outputInfoList = new ArrayList<>();
        // TODO: This should be done via a Structs accessor.
        properties.put(PropertyNames.OUTPUT_INFO, outputInfoList);
      }

      Map<String, Object> outputInfo = new HashMap<>();
      addString(outputInfo, PropertyNames.OUTPUT_NAME, name);

      String stepName = getString(properties, PropertyNames.USER_NAME);
      String generatedName = String.format("%s.out%d", stepName, outputInfoList.size());

      addString(outputInfo, PropertyNames.USER_NAME, generatedName);
      if ((value instanceof PCollection
              && translator.runner.doesPCollectionRequireIndexedFormat((PCollection<?>) value))
          || ((value instanceof PCollectionView)
              && Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
                  ((PCollectionView) value).getViewFn().getMaterialization().getUrn()))) {
        addBoolean(outputInfo, PropertyNames.USE_INDEXED_FORMAT, true);
      }
      if (valueCoder != null) {
        // Verify that encoding can be decoded, in order to catch serialization
        // failures as early as possible.
        CloudObject encoding = translateCoder(valueCoder);
        addObject(outputInfo, PropertyNames.ENCODING, encoding);
        translator.outputCoders.put(value, valueCoder);
      }

      outputInfoList.add(outputInfo);
    }

    private void addDisplayData(HasDisplayData hasDisplayData) {
      DisplayData displayData = DisplayData.from(hasDisplayData);
      List<Map<String, Object>> list = MAPPER.convertValue(displayData, List.class);
      addList(getProperties(), PropertyNames.DISPLAY_DATA, list);
    }

    private void addResourceHints(ResourceHints hints) {
      Map<String, Object> urlEncodedHints = new HashMap<>();
      for (Entry<String, ResourceHint> entry : hints.hints().entrySet()) {
        try {
          urlEncodedHints.put(
              entry.getKey(),
              new String(
                  percentCodec.encode(entry.getValue().toBytes()), StandardCharsets.US_ASCII));
        } catch (EncoderException e) {
          // Should never happen.
          throw new RuntimeException("Invalid value for resource hint: " + entry.getKey(), e);
        }
      }
      if (urlEncodedHints.size() > 0) {
        addDictionary(getProperties(), PropertyNames.RESOURCE_HINTS, urlEncodedHints);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  @Override
  public String toString() {
    return "DataflowPipelineTranslator#" + hashCode();
  }

  private static Map<String, Object> getProperties(Step step) {
    Map<String, Object> properties = step.getProperties();
    if (properties == null) {
      properties = new HashMap<>();
      step.setProperties(properties);
    }
    return properties;
  }

  ///////////////////////////////////////////////////////////////////////////

  static {
    registerTransformTranslator(
        View.CreatePCollectionView.class,
        new TransformTranslator<View.CreatePCollectionView>() {
          @Override
          public void translate(View.CreatePCollectionView transform, TranslationContext context) {
            translateTyped(transform, context);
          }

          private <ElemT, ViewT> void translateTyped(
              View.CreatePCollectionView<ElemT, ViewT> transform, TranslationContext context) {
            StepTranslationContext stepContext =
                context.addStep(transform, "CollectionToSingleton");
            PCollection<ElemT> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
            stepContext.addInput(
                PropertyNames.WINDOWING_STRATEGY,
                byteArrayToJsonString(
                    serializeWindowingStrategy(windowingStrategy, context.getPipelineOptions())));
            stepContext.addInput(
                PropertyNames.IS_MERGING_WINDOW_FN, windowingStrategy.needsMerge());
            stepContext.addCollectionToSingletonOutput(
                input, PropertyNames.OUTPUT, transform.getView());
          }
        });

    registerTransformTranslator(
        CreateDataflowView.class,
        new TransformTranslator<CreateDataflowView>() {
          @Override
          public void translate(CreateDataflowView transform, TranslationContext context) {
            translateTyped(transform, context);
          }

          private <ElemT, ViewT> void translateTyped(
              CreateDataflowView<ElemT, ViewT> transform, TranslationContext context) {
            StepTranslationContext stepContext =
                context.addStep(transform, "CollectionToSingleton");
            PCollection<ElemT> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            stepContext.addCollectionToSingletonOutput(
                input, PropertyNames.OUTPUT, transform.getView());
          }
        });

    DataflowPipelineTranslator.registerTransformTranslator(
        DataflowRunner.CombineGroupedValues.class,
        new TransformTranslator<CombineGroupedValues>() {
          @Override
          public void translate(CombineGroupedValues transform, TranslationContext context) {
            translateHelper(transform, context);
          }

          private <K, InputT, OutputT> void translateHelper(
              final CombineGroupedValues<K, InputT, OutputT> primitiveTransform,
              TranslationContext context) {
            Combine.GroupedValues<K, InputT, OutputT> originalTransform =
                primitiveTransform.getOriginalCombine();
            StepTranslationContext stepContext =
                context.addStep(primitiveTransform, "CombineValues");
            translateInputs(
                stepContext,
                context.getInput(primitiveTransform),
                originalTransform.getSideInputs(),
                context);

            AppliedCombineFn<? super K, ? super InputT, ?, OutputT> fn =
                originalTransform.getAppliedFn(
                    context.getInput(primitiveTransform).getPipeline().getCoderRegistry(),
                    context.getInput(primitiveTransform).getCoder(),
                    context.getInput(primitiveTransform).getWindowingStrategy());

            stepContext.addEncodingInput(fn.getAccumulatorCoder());

            stepContext.addInput(
                PropertyNames.SERIALIZED_FN, byteArrayToJsonString(serializeToByteArray(fn)));

            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(primitiveTransform));
          }
        });

    registerTransformTranslator(
        Flatten.PCollections.class,
        new TransformTranslator<Flatten.PCollections>() {
          @Override
          public void translate(Flatten.PCollections transform, TranslationContext context) {
            flattenHelper(transform, context);
          }

          private <T> void flattenHelper(
              Flatten.PCollections<T> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "Flatten");

            List<OutputReference> inputs = new ArrayList<>();
            for (PValue input : context.getInputs(transform).values()) {
              inputs.add(context.asOutputReference(input, context.getProducer(input)));
            }
            stepContext.addInput(PropertyNames.INPUTS, inputs);
            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
          }
        });

    registerTransformTranslator(
        GroupByKeyAndSortValuesOnly.class,
        new TransformTranslator<GroupByKeyAndSortValuesOnly>() {
          @Override
          public void translate(GroupByKeyAndSortValuesOnly transform, TranslationContext context) {
            groupByKeyAndSortValuesHelper(transform, context);
          }

          private <K1, K2, V> void groupByKeyAndSortValuesHelper(
              GroupByKeyAndSortValuesOnly<K1, K2, V> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "GroupByKey");
            PCollection<KV<K1, KV<K2, V>>> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
            stepContext.addInput(PropertyNames.SORT_VALUES, true);

            // TODO: Add support for combiner lifting once the need arises.
            stepContext.addInput(PropertyNames.DISALLOW_COMBINER_LIFTING, true);
          }
        });

    registerTransformTranslator(
        DataflowGroupByKey.class,
        new TransformTranslator<DataflowGroupByKey>() {
          @Override
          public void translate(DataflowGroupByKey transform, TranslationContext context) {
            dataflowGroupByKeyHelper(transform, context);
          }

          private <K, V> void dataflowGroupByKeyHelper(
              DataflowGroupByKey<K, V> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "GroupByKey");
            PCollection<KV<K, V>> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));

            WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
            stepContext.addInput(PropertyNames.DISALLOW_COMBINER_LIFTING, true);
            stepContext.addInput(
                PropertyNames.SERIALIZED_FN,
                byteArrayToJsonString(
                    serializeWindowingStrategy(windowingStrategy, context.getPipelineOptions())));
            stepContext.addInput(
                PropertyNames.IS_MERGING_WINDOW_FN,
                !windowingStrategy.getWindowFn().isNonMerging());
            stepContext.addInput(PropertyNames.ALLOW_DUPLICATES, transform.allowDuplicates());
          }
        });

    registerTransformTranslator(
        GroupByKey.class,
        new TransformTranslator<GroupByKey>() {
          @Override
          public void translate(GroupByKey transform, TranslationContext context) {
            groupByKeyHelper(transform, context);
          }

          private <K, V> void groupByKeyHelper(
              GroupByKey<K, V> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "GroupByKey");
            PCollection<KV<K, V>> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));

            WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
            boolean isStreaming =
                context.getPipelineOptions().as(StreamingOptions.class).isStreaming();
            boolean allowCombinerLifting =
                !windowingStrategy.needsMerge()
                    && windowingStrategy.getWindowFn().assignsToOneWindow();
            if (isStreaming) {
              allowCombinerLifting &= transform.fewKeys();
              // TODO: Allow combiner lifting on the non-default trigger, as appropriate.
              allowCombinerLifting &= (windowingStrategy.getTrigger() instanceof DefaultTrigger);
            }
            stepContext.addInput(PropertyNames.DISALLOW_COMBINER_LIFTING, !allowCombinerLifting);
            stepContext.addInput(
                PropertyNames.SERIALIZED_FN,
                byteArrayToJsonString(
                    serializeWindowingStrategy(windowingStrategy, context.getPipelineOptions())));
            stepContext.addInput(
                PropertyNames.IS_MERGING_WINDOW_FN,
                !windowingStrategy.getWindowFn().isNonMerging());
          }
        });

    registerTransformTranslator(
        ParDo.MultiOutput.class,
        new TransformTranslator<ParDo.MultiOutput>() {
          @Override
          public void translate(ParDo.MultiOutput transform, TranslationContext context) {
            translateMultiHelper(transform, context);
          }

          private <InputT, OutputT> void translateMultiHelper(
              ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "ParallelDo");
            DoFnSchemaInformation doFnSchemaInformation;
            doFnSchemaInformation =
                ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
            Map<String, PCollectionView<?>> sideInputMapping =
                ParDoTranslation.getSideInputMapping(context.getCurrentTransform());
            Map<TupleTag<?>, Coder<?>> outputCoders =
                context.getOutputs(transform).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
            translateInputs(
                stepContext,
                context.getInput(transform),
                transform.getSideInputs().values(),
                context);
            translateOutputs(context.getOutputs(transform), stepContext);
            translateFn(
                stepContext,
                transform.getFn(),
                context.getInput(transform).getWindowingStrategy(),
                transform.getSideInputs().values(),
                context.getInput(transform).getCoder(),
                context,
                transform.getMainOutputTag(),
                outputCoders,
                doFnSchemaInformation,
                sideInputMapping);
          }
        });

    registerTransformTranslator(
        ParDoSingle.class,
        new TransformTranslator<ParDoSingle>() {
          @Override
          public void translate(ParDoSingle transform, TranslationContext context) {
            translateSingleHelper(transform, context);
          }

          private <InputT, OutputT> void translateSingleHelper(
              ParDoSingle<InputT, OutputT> transform, TranslationContext context) {

            DoFnSchemaInformation doFnSchemaInformation;
            doFnSchemaInformation =
                ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
            Map<String, PCollectionView<?>> sideInputMapping =
                ParDoTranslation.getSideInputMapping(context.getCurrentTransform());
            StepTranslationContext stepContext = context.addStep(transform, "ParallelDo");
            Map<TupleTag<?>, Coder<?>> outputCoders =
                context.getOutputs(transform).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));

            translateInputs(
                stepContext,
                context.getInput(transform),
                transform.getSideInputs().values(),
                context);
            stepContext.addOutput(
                transform.getMainOutputTag().getId(), context.getOutput(transform));
            translateFn(
                stepContext,
                transform.getFn(),
                context.getInput(transform).getWindowingStrategy(),
                transform.getSideInputs().values(),
                context.getInput(transform).getCoder(),
                context,
                transform.getMainOutputTag(),
                outputCoders,
                doFnSchemaInformation,
                sideInputMapping);
          }
        });

    registerTransformTranslator(
        Window.Assign.class,
        new TransformTranslator<Window.Assign>() {
          @Override
          public void translate(Window.Assign transform, TranslationContext context) {
            translateHelper(transform, context);
          }

          private <T> void translateHelper(Window.Assign<T> transform, TranslationContext context) {
            StepTranslationContext stepContext = context.addStep(transform, "Bucket");
            PCollection<T> input = context.getInput(transform);
            stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
            stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));

            WindowingStrategy<?, ?> strategy = context.getOutput(transform).getWindowingStrategy();
            byte[] serializedBytes =
                serializeWindowingStrategy(strategy, context.getPipelineOptions());
            String serializedJson = byteArrayToJsonString(serializedBytes);
            stepContext.addInput(PropertyNames.SERIALIZED_FN, serializedJson);
          }
        });

    ///////////////////////////////////////////////////////////////////////////
    // IO Translation.

    registerTransformTranslator(SplittableParDo.PrimitiveBoundedRead.class, new ReadTranslator());

    registerTransformTranslator(
        TestStream.class,
        new TransformTranslator<TestStream>() {
          @Override
          public void translate(TestStream transform, TranslationContext context) {
            translateTyped(transform, context);
          }

          private <T> void translateTyped(TestStream<T> transform, TranslationContext context) {
            try {
              StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
              String ptransformId =
                  context.getSdkComponents().getPTransformIdOrThrow(context.getCurrentTransform());
              stepContext.addInput(PropertyNames.SERIALIZED_FN, ptransformId);
              stepContext.addInput(PropertyNames.FORMAT, "test_stream");
              RunnerApi.TestStreamPayload.Builder payloadBuilder =
                  RunnerApi.TestStreamPayload.newBuilder();
              for (TestStream.Event event : transform.getEvents()) {
                if (event instanceof TestStream.ElementEvent) {
                  RunnerApi.TestStreamPayload.Event.AddElements.Builder addElementsBuilder =
                      RunnerApi.TestStreamPayload.Event.AddElements.newBuilder();
                  Iterable<TimestampedValue<T>> elements =
                      ((TestStream.ElementEvent) event).getElements();
                  for (TimestampedValue<T> element : elements) {
                    addElementsBuilder.addElements(
                        RunnerApi.TestStreamPayload.TimestampedElement.newBuilder()
                            .setEncodedElement(
                                ByteString.copyFrom(
                                    CoderUtils.encodeToByteArray(
                                        transform.getValueCoder(), element.getValue())))
                            .setTimestamp(element.getTimestamp().getMillis() * 1000));
                  }
                  payloadBuilder.addEventsBuilder().setElementEvent(addElementsBuilder);
                } else if (event instanceof TestStream.WatermarkEvent) {
                  payloadBuilder
                      .addEventsBuilder()
                      .setWatermarkEvent(
                          RunnerApi.TestStreamPayload.Event.AdvanceWatermark.newBuilder()
                              .setNewWatermark(
                                  ((TestStream.WatermarkEvent) event).getWatermark().getMillis()
                                      * 1000));
                } else if (event instanceof TestStream.ProcessingTimeEvent) {
                  payloadBuilder
                      .addEventsBuilder()
                      .setProcessingTimeEvent(
                          RunnerApi.TestStreamPayload.Event.AdvanceProcessingTime.newBuilder()
                              .setAdvanceDuration(
                                  ((TestStream.ProcessingTimeEvent) event)
                                          .getProcessingTimeAdvance()
                                          .getMillis()
                                      * 1000));
                }
              }
              stepContext.addInput(
                  PropertyNames.SERIALIZED_TEST_STREAM,
                  byteArrayToJsonString(payloadBuilder.build().toByteArray()));
              stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });

    ///////////////////////////////////////////////////////////////////////////
    // Legacy Splittable DoFn translation.

    registerTransformTranslator(
        SplittableParDo.ProcessKeyedElements.class,
        new TransformTranslator<SplittableParDo.ProcessKeyedElements>() {
          @Override
          public void translate(
              SplittableParDo.ProcessKeyedElements transform, TranslationContext context) {
            translateTyped(transform, context);
          }

          private <InputT, OutputT, RestrictionT, WatermarkEstimatorStateT> void translateTyped(
              SplittableParDo.ProcessKeyedElements<
                      InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
                  transform,
              TranslationContext context) {
            DoFnSchemaInformation doFnSchemaInformation;
            doFnSchemaInformation =
                ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
            Map<String, PCollectionView<?>> sideInputMapping =
                ParDoTranslation.getSideInputMapping(context.getCurrentTransform());
            StepTranslationContext stepContext =
                context.addStep(transform, "SplittableProcessKeyed");
            Map<TupleTag<?>, Coder<?>> outputCoders =
                context.getOutputs(transform).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
            translateInputs(
                stepContext, context.getInput(transform), transform.getSideInputs(), context);
            translateOutputs(context.getOutputs(transform), stepContext);
            translateFn(
                stepContext,
                transform.getFn(),
                transform.getInputWindowingStrategy(),
                transform.getSideInputs(),
                transform.getElementCoder(),
                context,
                transform.getMainOutputTag(),
                outputCoders,
                doFnSchemaInformation,
                sideInputMapping);

            stepContext.addInput(
                PropertyNames.RESTRICTION_CODER,
                translateCoder(
                    KvCoder.of(
                        transform.getRestrictionCoder(),
                        transform.getWatermarkEstimatorStateCoder())));
          }
        });
  }

  private static void translateInputs(
      StepTranslationContext stepContext,
      PCollection<?> input,
      Iterable<PCollectionView<?>> sideInputs,
      TranslationContext context) {
    stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
    translateSideInputs(stepContext, sideInputs, context);
  }

  // Used for ParDo
  private static void translateSideInputs(
      StepTranslationContext stepContext,
      Iterable<PCollectionView<?>> sideInputs,
      TranslationContext context) {
    Map<String, Object> nonParInputs = new HashMap<>();

    for (PCollectionView<?> view : sideInputs) {
      nonParInputs.put(
          view.getTagInternal().getId(),
          context.asOutputReference(view, context.getProducer(view)));
    }

    stepContext.addInput(PropertyNames.NON_PARALLEL_INPUTS, nonParInputs);
  }

  private static void translateFn(
      StepTranslationContext stepContext,
      DoFn fn,
      WindowingStrategy windowingStrategy,
      Iterable<PCollectionView<?>> sideInputs,
      Coder inputCoder,
      TranslationContext context,
      TupleTag<?> mainOutput,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {

    boolean isStateful = DoFnSignatures.isStateful(fn);
    if (isStateful) {
      DataflowPipelineOptions options = context.getPipelineOptions();
      DataflowRunner.verifyDoFnSupported(fn, options.isStreaming(), options);
      DataflowRunner.verifyStateSupportForWindowingStrategy(windowingStrategy);
    }

    stepContext.addInput(PropertyNames.USER_FN, fn.getClass().getName());

    // Fn API does not need the additional metadata in the wrapper, and it is Java-only serializable
    // hence not suitable for portable execution
    stepContext.addInput(
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(
            serializeToByteArray(
                DoFnInfo.forFn(
                    fn,
                    windowingStrategy,
                    sideInputs,
                    inputCoder,
                    outputCoders,
                    mainOutput,
                    doFnSchemaInformation,
                    sideInputMapping))));

    // Setting USES_KEYED_STATE will cause an ungrouped shuffle, which works
    // in streaming but does not work in batch
    if (context.getPipelineOptions().isStreaming() && isStateful) {
      stepContext.addInput(PropertyNames.USES_KEYED_STATE, "true");
    }
  }

  private static void translateOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, StepTranslationContext stepContext) {
    for (Map.Entry<TupleTag<?>, PCollection<?>> taggedOutput : outputs.entrySet()) {
      TupleTag<?> tag = taggedOutput.getKey();
      stepContext.addOutput(tag.getId(), taggedOutput.getValue());
    }
  }

  private static CloudObject translateCoder(Coder<?> coder) {
    return CloudObjects.asCloudObject(coder, null);
  }
}
