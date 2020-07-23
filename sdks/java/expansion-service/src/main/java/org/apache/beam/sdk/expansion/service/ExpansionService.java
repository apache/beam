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
package org.apache.beam.sdk.expansion.service;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Converter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A service that allows pipeline expand transforms from a remote SDK. */
public class ExpansionService extends ExpansionServiceGrpc.ExpansionServiceImplBase
    implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ExpansionService.class);

  /**
   * A registrar that creates {@link TransformProvider} instances from {@link
   * RunnerApi.FunctionSpec}s.
   *
   * <p>Transform authors have the ability to provide a registrar by creating a {@link
   * ServiceLoader} entry and a concrete implementation of this interface.
   *
   * <p>It is optional but recommended to use one of the many build time tools such as {@link
   * AutoService} to generate the necessary META-INF files automatically.
   */
  public interface ExpansionServiceRegistrar {
    Map<String, TransformProvider> knownTransforms();
  }

  /**
   * Exposes Java transforms via {@link org.apache.beam.sdk.expansion.ExternalTransformRegistrar}.
   */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class ExternalTransformRegistrarLoader
      implements ExpansionService.ExpansionServiceRegistrar {

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      for (ExternalTransformRegistrar registrar :
          ServiceLoader.load(ExternalTransformRegistrar.class)) {
        for (Map.Entry<String, Class<? extends ExternalTransformBuilder>> entry :
            registrar.knownBuilders().entrySet()) {
          String urn = entry.getKey();
          Class<? extends ExternalTransformBuilder> builderClass = entry.getValue();
          builder.put(
              urn,
              spec -> {
                try {
                  ExternalTransforms.ExternalConfigurationPayload payload =
                      ExternalTransforms.ExternalConfigurationPayload.parseFrom(spec.getPayload());
                  return translate(payload, builderClass);
                } catch (Exception e) {
                  throw new RuntimeException(
                      String.format("Failed to build transform %s from spec %s", urn, spec), e);
                }
              });
        }
      }
      return builder.build();
    }

    private static PTransform<?, ?> translate(
        ExternalTransforms.ExternalConfigurationPayload payload,
        Class<? extends ExternalTransformBuilder> builderClass)
        throws Exception {
      Preconditions.checkState(
          ExternalTransformBuilder.class.isAssignableFrom(builderClass),
          "Provided identifier %s is not an ExternalTransformBuilder.",
          builderClass.getName());

      Object configObject = initConfiguration(builderClass);
      populateConfiguration(configObject, payload);
      return buildTransform(builderClass, configObject);
    }

    private static Object initConfiguration(Class<? extends ExternalTransformBuilder> builderClass)
        throws Exception {
      for (Method method : builderClass.getMethods()) {
        if (method.getName().equals("buildExternal")) {
          Preconditions.checkState(
              method.getParameterCount() == 1,
              "Build method for ExternalTransformBuilder %s must have exactly one parameter, but"
                  + " had %s parameters.",
              builderClass.getSimpleName(),
              method.getParameterCount());
          Class<?> configurationClass = method.getParameterTypes()[0];
          if (Object.class.equals(configurationClass)) {
            continue;
          }
          return configurationClass.getDeclaredConstructor().newInstance();
        }
      }
      throw new RuntimeException("Couldn't find build method on ExternalTransformBuilder.");
    }

    @VisibleForTesting
    static void populateConfiguration(
        Object config, ExternalTransforms.ExternalConfigurationPayload payload) throws Exception {
      Converter<String, String> camelCaseConverter =
          CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL);
      for (Map.Entry<String, ExternalTransforms.ConfigValue> entry :
          payload.getConfigurationMap().entrySet()) {
        String key = entry.getKey();
        ExternalTransforms.ConfigValue value = entry.getValue();

        String fieldName = camelCaseConverter.convert(key);
        assert fieldName != null
            : "@AssumeAssertion(nullness): converter type is imprecise; it is nullness-preserving";
        List<String> coderUrns = value.getCoderUrnList();
        Preconditions.checkArgument(coderUrns.size() > 0, "No Coder URN provided.");
        Coder coder = resolveCoder(coderUrns);
        Class type = coder.getEncodedTypeDescriptor().getRawType();

        String setterName =
            "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Method method;
        try {
          // retrieve the setter for this field
          method = config.getClass().getMethod(setterName, type);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(
              String.format(
                  "The configuration class %s is missing a setter %s for %s with type %s",
                  config.getClass(),
                  setterName,
                  fieldName,
                  coder.getEncodedTypeDescriptor().getType().getTypeName()),
              e);
        }
        method.invoke(
            config, coder.decode(entry.getValue().getPayload().newInput(), Coder.Context.NESTED));
      }
    }

    private static Coder resolveCoder(List<String> coderUrns) throws Exception {
      Preconditions.checkArgument(coderUrns.size() > 0, "No Coder URN provided.");
      RunnerApi.Components.Builder componentsBuilder = RunnerApi.Components.newBuilder();
      Deque<String> coderQueue = new ArrayDeque<>(coderUrns);
      RunnerApi.Coder coder = buildProto(coderQueue, componentsBuilder);

      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(componentsBuilder.build());
      return CoderTranslation.fromProto(coder, rehydratedComponents, TranslationContext.DEFAULT);
    }

    private static RunnerApi.Coder buildProto(
        Deque<String> coderUrns, RunnerApi.Components.Builder componentsBuilder) {
      Preconditions.checkArgument(coderUrns.size() > 0, "No URNs left to construct coder from");

      final String coderUrn = coderUrns.pop();
      RunnerApi.Coder.Builder coderBuilder =
          RunnerApi.Coder.newBuilder()
              .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(coderUrn).build());

      if (coderUrn.equals(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.ITERABLE))) {
        RunnerApi.Coder elementCoder = buildProto(coderUrns, componentsBuilder);
        String coderId = UUID.randomUUID().toString();
        componentsBuilder.putCoders(coderId, elementCoder);
        coderBuilder.addComponentCoderIds(coderId);
      } else if (coderUrn.equals(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.KV))) {
        RunnerApi.Coder element1Coder = buildProto(coderUrns, componentsBuilder);
        RunnerApi.Coder element2Coder = buildProto(coderUrns, componentsBuilder);
        String coderId1 = UUID.randomUUID().toString();
        String coderId2 = UUID.randomUUID().toString();
        componentsBuilder.putCoders(coderId1, element1Coder);
        componentsBuilder.putCoders(coderId2, element2Coder);
        coderBuilder.addComponentCoderIds(coderId1);
        coderBuilder.addComponentCoderIds(coderId2);
      }

      return coderBuilder.build();
    }

    private static PTransform<?, ?> buildTransform(
        Class<? extends ExternalTransformBuilder> builderClass, Object configObject)
        throws Exception {
      Constructor<? extends ExternalTransformBuilder> constructor =
          builderClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      ExternalTransformBuilder<?, ?, ?> externalTransformBuilder = constructor.newInstance();
      Method buildMethod = builderClass.getMethod("buildExternal", configObject.getClass());
      buildMethod.setAccessible(true);

      PTransform<?, ?> transform =
          (PTransform<?, ?>)
              checkArgumentNotNull(
                  buildMethod.invoke(externalTransformBuilder, configObject),
                  "Invoking %s.%s(%s) returned null, violating its type.",
                  builderClass.getCanonicalName(),
                  "buildExternal",
                  configObject);

      return transform;
    }
  }

  /**
   * Provides a mapping of {@link RunnerApi.FunctionSpec} to a {@link PTransform}, together with
   * mappings of its inputs and outputs to maps of PCollections.
   *
   * @param <InputT> input {@link PInput} type of the transform
   * @param <OutputT> output {@link POutput} type of the transform
   */
  public interface TransformProvider<InputT extends PInput, OutputT extends POutput> {

    default InputT createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
      inputs =
          checkArgumentNotNull(
              inputs); // spotbugs claims incorrectly that it is annotated @Nullable
      if (inputs.size() == 0) {
        return (InputT) p.begin();
      }
      if (inputs.size() == 1) {
        return (InputT) Iterables.getOnlyElement(inputs.values());
      } else {
        PCollectionTuple inputTuple = PCollectionTuple.empty(p);
        for (Map.Entry<String, PCollection<?>> entry : inputs.entrySet()) {
          inputTuple = inputTuple.and(new TupleTag(entry.getKey()), entry.getValue());
        }
        return (InputT) inputTuple;
      }
    }

    PTransform<InputT, OutputT> getTransform(RunnerApi.FunctionSpec spec);

    default Map<String, PCollection<?>> extractOutputs(OutputT output) {
      if (output instanceof PDone) {
        return Collections.emptyMap();
      } else if (output instanceof PCollection) {
        return ImmutableMap.of("output", (PCollection<?>) output);
      } else if (output instanceof PCollectionTuple) {
        return ((PCollectionTuple) output)
            .getAll().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getId(), Map.Entry::getValue));
      } else if (output instanceof PCollectionList<?>) {
        PCollectionList<?> listOutput = (PCollectionList<?>) output;
        ImmutableMap.Builder<String, PCollection<?>> indexToPCollection = ImmutableMap.builder();
        int i = 0;
        for (PCollection pc : listOutput.getAll()) {
          indexToPCollection.put(Integer.toString(i), pc);
          i++;
        }
        return indexToPCollection.build();
      } else {
        throw new UnsupportedOperationException("Unknown output type: " + output.getClass());
      }
    }

    default Map<String, PCollection<?>> apply(
        Pipeline p, String name, RunnerApi.FunctionSpec spec, Map<String, PCollection<?>> inputs) {
      return extractOutputs(
          Pipeline.applyTransform(name, createInput(p, inputs), getTransform(spec)));
    }
  }

  private @MonotonicNonNull Map<String, TransformProvider> registeredTransforms;

  private Map<String, TransformProvider> getRegisteredTransforms() {
    if (registeredTransforms == null) {
      registeredTransforms = loadRegisteredTransforms();
    }
    return registeredTransforms;
  }

  private Map<String, TransformProvider> loadRegisteredTransforms() {
    ImmutableMap.Builder<String, TransformProvider> registeredTransformsBuilder =
        ImmutableMap.builder();
    for (ExpansionServiceRegistrar registrar :
        ServiceLoader.load(ExpansionServiceRegistrar.class)) {
      registeredTransformsBuilder.putAll(registrar.knownTransforms());
    }
    ImmutableMap<String, TransformProvider> registeredTransforms =
        registeredTransformsBuilder.build();
    LOG.info("Registering external transforms: {}", registeredTransforms.keySet());
    return registeredTransforms;
  }

  @VisibleForTesting
  /*package*/ ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
    LOG.info(
        "Expanding '{}' with URN '{}'",
        request.getTransform().getUniqueName(),
        request.getTransform().getSpec().getUrn());
    LOG.debug("Full transform: {}", request.getTransform());
    Set<String> existingTransformIds = request.getComponents().getTransformsMap().keySet();
    Pipeline pipeline = Pipeline.create();
    ExperimentalOptions.addExperiment(
        pipeline.getOptions().as(ExperimentalOptions.class), "beam_fn_api");

    ClassLoader classLoader = Environments.class.getClassLoader();
    if (classLoader == null) {
      throw new RuntimeException(
          "Cannot detect classpath: classload is null (is it the bootstrap classloader?)");
    }

    List<String> classpathResources =
        detectClassPathResourcesToStage(classLoader, pipeline.getOptions());
    if (classpathResources.isEmpty()) {
      throw new IllegalArgumentException("No classpath elements found.");
    }
    LOG.debug("Staging to files from the classpath: {}", classpathResources.size());
    pipeline.getOptions().as(PortablePipelineOptions.class).setFilesToStage(classpathResources);

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(request.getComponents()).withPipeline(pipeline);

    Map<String, PCollection<?>> inputs =
        request.getTransform().getInputsMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    input -> {
                      try {
                        return rehydratedComponents.getPCollection(input.getValue());
                      } catch (IOException exn) {
                        throw new RuntimeException(exn);
                      }
                    }));

    @Nullable
    TransformProvider transformProvider =
        getRegisteredTransforms().get(request.getTransform().getSpec().getUrn());
    if (transformProvider == null) {
      throw new UnsupportedOperationException(
          "Unknown urn: " + request.getTransform().getSpec().getUrn());
    }
    Map<String, PCollection<?>> outputs =
        transformProvider.apply(
            pipeline,
            request.getTransform().getUniqueName(),
            request.getTransform().getSpec(),
            inputs);

    // Needed to find which transform was new...
    SdkComponents sdkComponents =
        rehydratedComponents
            .getSdkComponents(Collections.emptyList())
            .withNewIdPrefix(request.getNamespace());
    sdkComponents.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(
            pipeline.getOptions().as(PortablePipelineOptions.class)));
    Map<String, String> outputMap =
        outputs.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    output -> {
                      try {
                        return sdkComponents.registerPCollection(output.getValue());
                      } catch (IOException exn) {
                        throw new RuntimeException(exn);
                      }
                    }));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, sdkComponents);
    String expandedTransformId =
        Iterables.getOnlyElement(
            pipelineProto.getRootTransformIdsList().stream()
                .filter(id -> !existingTransformIds.contains(id))
                .collect(Collectors.toList()));
    RunnerApi.Components components = pipelineProto.getComponents();
    RunnerApi.PTransform expandedTransform =
        components
            .getTransformsOrThrow(expandedTransformId)
            .toBuilder()
            .setUniqueName(expandedTransformId)
            .clearOutputs()
            .putAllOutputs(outputMap)
            .build();
    LOG.debug("Expanded to {}", expandedTransform);

    return ExpansionApi.ExpansionResponse.newBuilder()
        .setComponents(components.toBuilder().removeTransforms(expandedTransformId))
        .setTransform(expandedTransform)
        .addAllRequirements(pipelineProto.getRequirementsList())
        .build();
  }

  @Override
  public void expand(
      ExpansionApi.ExpansionRequest request,
      StreamObserver<ExpansionApi.ExpansionResponse> responseObserver) {
    try {
      responseObserver.onNext(expand(request));
      responseObserver.onCompleted();
    } catch (RuntimeException exn) {
      responseObserver.onNext(
          ExpansionApi.ExpansionResponse.newBuilder()
              .setError(Throwables.getStackTraceAsString(exn))
              .build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing to do because the expansion service is stateless.
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    ExpansionService service = new ExpansionService();
    for (Map.Entry<String, TransformProvider> entry :
        service.getRegisteredTransforms().entrySet()) {
      System.out.println("\t" + entry.getKey() + ": " + entry.getValue());
    }

    Server server =
        ServerBuilder.forPort(port)
            .addService(service)
            .addService(new ArtifactRetrievalService())
            .build();
    server.start();
    server.awaitTermination();
  }
}
