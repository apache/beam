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
package org.apache.beam.runners.core.construction.expansion;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Converter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
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
  public static class ExternalTransformRegistrarLoader<ConfigT>
      implements ExpansionService.ExpansionServiceRegistrar {

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      for (ExternalTransformRegistrar registrar :
          ServiceLoader.load(ExternalTransformRegistrar.class)) {
        for (Map.Entry<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> entry :
            registrar.knownBuilders().entrySet()) {
          String urn = entry.getKey();
          Class<? extends ExternalTransformBuilder<?, ?, ?>> builderClass = entry.getValue();
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

    private static PTransform translate(
        ExternalTransforms.ExternalConfigurationPayload payload,
        Class<? extends ExternalTransformBuilder<?, ?, ?>> builderClass)
        throws Exception {
      Preconditions.checkState(
          ExternalTransformBuilder.class.isAssignableFrom(builderClass),
          "Provided identifier %s is not an ExternalTransformBuilder.",
          builderClass.getName());

      Object configObject = initConfiguration(builderClass);
      populateConfiguration(configObject, payload);
      return buildTransform(builderClass, configObject);
    }

    private static Object initConfiguration(
        Class<? extends ExternalTransformBuilder<?, ?, ?>> builderClass) throws Exception {
      for (Method method : builderClass.getMethods()) {
        if (method.getName().equals("buildExternal")) {
          Preconditions.checkState(
              method.getParameterCount() == 1,
              "Build method for ExternalTransformBuilder %s must have exactly one parameter, but had %s parameters.",
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

    private static void populateConfiguration(
        Object config, ExternalTransforms.ExternalConfigurationPayload payload) throws Exception {
      Converter<String, String> camelCaseConverter =
          CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL);
      for (Map.Entry<String, ExternalTransforms.ConfigValue> entry :
          payload.getConfigurationMap().entrySet()) {
        String fieldName = camelCaseConverter.convert(entry.getKey());
        String coderUrn = entry.getValue().getCoderUrn();

        final Coder coder;
        final Class type;
        if (BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT).equals(coderUrn)) {
          coder = VarLongCoder.of();
          type = Long.class;
        } else {
          // TODO Use RehydratedComponents with coder ids instead
          throw new RuntimeException("Unsupported coder urn " + coderUrn);
        }
        String setterName =
            "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Method method;
        try {
          // retrieve the setter for this field
          method = config.getClass().getMethod(setterName, type);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(
              String.format(
                  "The configuration class %s is missing a setter %s for %s",
                  config.getClass(), setterName, fieldName),
              e);
        }
        method.invoke(config, coder.decode(entry.getValue().getPayload().newInput()));
      }
    }

    private static PTransform buildTransform(
        Class<? extends ExternalTransformBuilder<?, ?, ?>> builderClass, Object configObject)
        throws Exception {
      Constructor<? extends ExternalTransformBuilder<?, ?, ?>> constructor =
          builderClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      ExternalTransformBuilder<?, ?, ?> externalTransformBuilder = constructor.newInstance();
      Method buildMethod = builderClass.getMethod("buildExternal", configObject.getClass());
      buildMethod.setAccessible(true);
      return (PTransform) buildMethod.invoke(externalTransformBuilder, configObject);
    }
  }

  /**
   * Provides a mapping of {@link RunnerApi.FunctionSpec} to a {@link PTransform}, together with
   * mappings of its inputs and outputs to maps of PCollections.
   *
   * @param <InputT> input {@link PValue} type of the transform
   * @param <OutputT> output {@link PValue} type of the transform
   */
  public interface TransformProvider<InputT extends PInput, OutputT extends PValue> {

    default InputT createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
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
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
      } else if (output instanceof PCollectionList<?>) {
        PCollectionList<?> listOutput = (PCollectionList<?>) output;
        return IntStream.range(0, listOutput.size())
            .boxed()
            .collect(Collectors.toMap(index -> "output_" + index, listOutput::get));
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

  private Map<String, TransformProvider> registeredTransforms = loadRegisteredTransforms();

  private Map<String, TransformProvider> loadRegisteredTransforms() {
    ImmutableMap.Builder<String, TransformProvider> registeredTransforms = ImmutableMap.builder();
    for (ExpansionServiceRegistrar registrar :
        ServiceLoader.load(ExpansionServiceRegistrar.class)) {
      registeredTransforms.putAll(registrar.knownTransforms());
    }
    return registeredTransforms.build();
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
    if (!registeredTransforms.containsKey(request.getTransform().getSpec().getUrn())) {
      throw new UnsupportedOperationException(
          "Unknown urn: " + request.getTransform().getSpec().getUrn());
    }
    registeredTransforms
        .get(request.getTransform().getSpec().getUrn())
        .apply(
            pipeline,
            request.getTransform().getUniqueName(),
            request.getTransform().getSpec(),
            inputs);

    // Needed to find which transform was new...
    SdkComponents sdkComponents =
        rehydratedComponents.getSdkComponents().withNewIdPrefix(request.getNamespace());
    sdkComponents.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT);
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
            .build();
    LOG.debug("Expanded to {}", expandedTransform);

    return ExpansionApi.ExpansionResponse.newBuilder()
        .setComponents(components.toBuilder().removeTransforms(expandedTransformId))
        .setTransform(expandedTransform)
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
      responseObserver.onError(exn);
      throw exn;
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing to do because the expansion service is stateless.
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    Server server = ServerBuilder.forPort(port).addService(new ExpansionService()).build();
    server.start();
    server.awaitTermination();
  }
}
