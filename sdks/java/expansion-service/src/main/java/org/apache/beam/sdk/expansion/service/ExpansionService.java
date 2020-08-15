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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExternalConfigurationPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
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

    private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      for (ExternalTransformRegistrar registrar :
          ServiceLoader.load(ExternalTransformRegistrar.class)) {
        for (Map.Entry<String, ExternalTransformBuilder<?, ?, ?>> entry :
            registrar.knownBuilderInstances().entrySet()) {
          String urn = entry.getKey();
          ExternalTransformBuilder builderInstance = entry.getValue();
          builder.put(
              urn,
              spec -> {
                try {
                  Class configClass = getConfigClass(builderInstance);
                  return builderInstance.buildExternal(
                      payloadToConfig(
                          ExternalConfigurationPayload.parseFrom(spec.getPayload()), configClass));
                } catch (Exception e) {
                  throw new RuntimeException(
                      String.format("Failed to build transform %s from spec %s", urn, spec), e);
                }
              });
        }
      }

      return builder.build();
    }

    private static <ConfigT> Class<ConfigT> getConfigClass(
        ExternalTransformBuilder<ConfigT, ?, ?> transformBuilder) {
      Class<ConfigT> configurationClass = null;
      for (Method method : transformBuilder.getClass().getMethods()) {
        if (method.getName().equals("buildExternal")) {
          Preconditions.checkState(
              method.getParameterCount() == 1,
              "Build method for ExternalTransformBuilder %s must have exactly one parameter, but"
                  + " had %s parameters.",
              transformBuilder.getClass().getSimpleName(),
              method.getParameterCount());
          configurationClass = (Class<ConfigT>) method.getParameterTypes()[0];
          if (Object.class.equals(configurationClass)) {
            continue;
          }
          break;
        }
      }

      if (configurationClass == null) {
        throw new AssertionError("Failed to find buildExternal method.");
      }

      return configurationClass;
    }

    private static <ConfigT> Row decodeRow(ExternalConfigurationPayload payload) {
      Schema payloadSchema = SchemaTranslation.schemaFromProto(payload.getSchema());

      if (payloadSchema.getFieldCount() == 0) {
        return Row.withSchema(Schema.of()).build();
      }

      // Coerce field names to camel-case
      Converter<String, String> camelCaseConverter =
          CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL);
      payloadSchema =
          payloadSchema.getFields().stream()
              .map(
                  (field) -> {
                    Preconditions.checkNotNull(field.getName());
                    if (field.getName().contains("_")) {
                      @Nullable String newName = camelCaseConverter.convert(field.getName());
                      assert newName != null
                          : "@AssumeAssertion(nullness): converter type is imprecise; it is nullness-preserving";
                      return field.withName(newName);
                    } else {
                      return field;
                    }
                  })
              .collect(Schema.toSchema());

      Row configRow;
      try {
        configRow = RowCoder.of(payloadSchema).decode(payload.getPayload().newInput());
      } catch (IOException e) {
        throw new RuntimeException("Error decoding payload", e);
      }
      return configRow;
    }

    /**
     * Attempt to create an instance of {@link ConfigT} from an {@link
     * ExternalConfigurationPayload}. If a schema is registered for {@link ConfigT} this method will
     * attempt to ise it. Throws an {@link IllegalArgumentException} if the schema in {@code
     * payload} is not {@link Schema#assignableTo(Schema) assignable to} the registered schema.
     *
     * <p>If no Schema is registered, {@link ConfigT} must have a zero-argument constructor and
     * setters corresponding to each field in the row encoded by {@code payload}. Note {@link
     * ConfigT} may have additional setters not represented in the {@ocde payload} schema.
     *
     * <p>Exposed for testing only. No backwards compatibility guarantees.
     */
    @VisibleForTesting
    public static <ConfigT> ConfigT payloadToConfig(
        ExternalConfigurationPayload payload, Class<ConfigT> configurationClass) {
      try {
        return payloadToConfigSchema(payload, configurationClass);
      } catch (NoSuchSchemaException schemaException) {
        LOG.warn(
            "Configuration class '{}' has no schema registered. Attempting to construct with setter approach.",
            configurationClass.getName());
        try {
          return payloadToConfigSetters(payload, configurationClass);
        } catch (ReflectiveOperationException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Failed to construct instance of configuration class '%s'",
                  configurationClass.getName()),
              e);
        }
      }
    }

    private static <ConfigT> ConfigT payloadToConfigSchema(
        ExternalConfigurationPayload payload, Class<ConfigT> configurationClass)
        throws NoSuchSchemaException {
      Schema configSchema = SCHEMA_REGISTRY.getSchema(configurationClass);
      SerializableFunction<Row, ConfigT> fromRowFunc =
          SCHEMA_REGISTRY.getFromRowFunction(configurationClass);

      Row payloadRow = decodeRow(payload);

      if (!payloadRow.getSchema().assignableTo(configSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "Schema in expansion request payload is not assignable to the schema for the "
                    + "configuration object.%n%nPayload Schema: %s%n%nConfiguration Schema: %s",
                payloadRow.getSchema(), configSchema));
      }

      return fromRowFunc.apply(payloadRow);
    }

    private static <ConfigT> ConfigT payloadToConfigSetters(
        ExternalConfigurationPayload payload, Class<ConfigT> configurationClass)
        throws ReflectiveOperationException {
      Row configRow = decodeRow(payload);

      Constructor<ConfigT> constructor = configurationClass.getDeclaredConstructor();
      constructor.setAccessible(true);

      ConfigT config = constructor.newInstance();
      for (Field field : configRow.getSchema().getFields()) {
        String key = field.getName();
        @Nullable Object value = configRow.getValue(field.getName());

        String fieldName = key;

        Coder coder = SchemaCoder.coderForFieldType(field.getType());
        Class type = coder.getEncodedTypeDescriptor().getRawType();

        String setterName =
            "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Method method;
        try {
          // retrieve the setter for this field
          method = config.getClass().getMethod(setterName, type);
        } catch (NoSuchMethodException e) {
          throw new IllegalArgumentException(
              String.format(
                  "The configuration class %s is missing a setter %s for %s with type %s",
                  config.getClass(),
                  setterName,
                  fieldName,
                  coder.getEncodedTypeDescriptor().getType().getTypeName()),
              e);
        }
        invokeSetter(config, value, method);
      }
      return config;
    }

    // Checker framework is conservative for Method#invoke, args are NonNull
    // See https://checkerframework.org/manual/#reflection-resolution
    @SuppressWarnings("nullness")
    private static <ConfigT> void invokeSetter(
        ConfigT config, @Nullable Object value, Method method)
        throws IllegalAccessException, InvocationTargetException {
      method.invoke(config, value);
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
