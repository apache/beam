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

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslation.SchemaTransformPayloadTranslator;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.READ_TRANSFORM_URN;

import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.fn.harness.ExternalWorkerService;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest;
import org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse;
import org.apache.beam.model.expansion.v1.ExpansionApi.SchemaTransformConfig;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExternalConfigurationPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProvider.AllowList;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.alts.AltsServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Converter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A service that allows pipeline expand transforms from a remote SDK. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ExpansionService extends ExpansionServiceGrpc.ExpansionServiceImplBase
    implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ExpansionService.class);

  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();

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
    public Map<String, TransformProvider> knownTransforms() {
      Map<String, TransformProvider> providers = new HashMap<>();

      // First check and register ExternalTransformBuilder in ServiceLoader style, converting
      // to TransformProvider after validation.
      Map<String, ExternalTransformBuilder> registeredBuilders = loadTransformBuilders();
      for (Map.Entry<String, ExternalTransformBuilder> registeredBuilder :
          registeredBuilders.entrySet()) {
        providers.put(
            registeredBuilder.getKey(),
            new TransformProviderForBuilder(registeredBuilder.getValue()));
      }

      // Now load payload translators and convert them to TransformProviders after validating for
      // duplicates
      List<String> deprecatedTransformURNs = ImmutableList.of(READ_TRANSFORM_URN);
      Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          registeredPayloadTranslators = PTransformTranslation.getKnownPayloadTranslators();

      for (Map.Entry<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          entry : registeredPayloadTranslators.entrySet()) {

        @Initialized TransformPayloadTranslator translator = entry.getValue();

        if (translator == null) {
          continue;
        }

        String urn;
        try {
          urn = translator.getUrn();
          if (urn == null) {
            LOG.debug(
                "Could not load the TransformPayloadTranslator "
                    + translator
                    + " to the Expansion Service since it did not produce a unique URN.");
            continue;
          } else if (urn.equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))
              && translator instanceof SchemaTransformPayloadTranslator) {
            urn = ((SchemaTransformPayloadTranslator) translator).provider().identifier();
          }
        } catch (Exception e) {
          LOG.info(
              "Could not load the TransformPayloadTranslator "
                  + translator
                  + " to the Expansion Service.",
              e);
          continue;
        }

        if (deprecatedTransformURNs.contains(urn)) {
          continue;
        }
        final String finalUrn = urn;
        TransformProvider transformProvider = new TransformProviderForPayloadTranslator(translator);
        providers.put(finalUrn, transformProvider);
      }
      return providers;
    }

    /**
     * Returns all {@link ExternalTransformBuilder} associations from any registrar set up via the
     * service loader interface.
     */
    private static Map<String, ExternalTransformBuilder> loadTransformBuilders() {

      Map<String, ExternalTransformBuilder> registeredBuilders = new HashMap<>();

      ImmutableSet.Builder<String> conflictingRegistrations = new ImmutableSet.Builder<>();

      for (ExternalTransformRegistrar registrar :
          ServiceLoader.load(ExternalTransformRegistrar.class)) {

        for (Map.Entry<String, ExternalTransformBuilder<?, ?, ?>> entry :
            registrar.knownBuilderInstances().entrySet()) {

          String urn = entry.getKey();
          ExternalTransformBuilder newBuilder = entry.getValue();
          @Nullable ExternalTransformBuilder existingBuilder = registeredBuilders.get(urn);

          if (existingBuilder == null) {
            registeredBuilders.put(urn, newBuilder);
          } else {
            LOG.error(
                "Conflicting registrations for {}: {} and {}", urn, existingBuilder, newBuilder);
            conflictingRegistrations.add(urn);
          }
        }
      }

      Set<String> conflictingRegistrationSet = conflictingRegistrations.build();
      if (!conflictingRegistrationSet.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Conflicting registrations for: %s",
                Joiner.on(", ").join(conflictingRegistrationSet)));
      }
      return registeredBuilders;
    }
  }

  private static class TransformProviderForPayloadTranslator<
          InputT extends PInput, OutputT extends POutput>
      implements TransformProvider<InputT, OutputT> {

    private final TransformPayloadTranslator<PTransform<InputT, OutputT>> payloadTranslator;

    // Returns true if the underlying transform represented by this is a schema-aware transform.
    private boolean isSchemaTransform() {
      return (payloadTranslator instanceof SchemaTransformPayloadTranslator);
    }

    private TransformProviderForPayloadTranslator(
        TransformPayloadTranslator<PTransform<InputT, OutputT>> payloadTranslator) {
      this.payloadTranslator = payloadTranslator;
    }

    @Override
    public PTransform<InputT, OutputT> getTransform(
        RunnerApi.FunctionSpec spec, PipelineOptions options) {
      if (isSchemaTransform()) {
        return ExpansionServiceSchemaTransformProvider.of().getTransform(spec, options);
      } else {
        try {
          ExternalConfigurationPayload payload =
              ExternalConfigurationPayload.parseFrom(spec.getPayload());
          Row configRow =
              RowCoder.of(SchemaTranslation.schemaFromProto(payload.getSchema()))
                  .decode(new ByteArrayInputStream(payload.getPayload().toByteArray()));
          PTransform transformFromRow = payloadTranslator.fromConfigRow(configRow, options);
          if (transformFromRow != null) {
            return transformFromRow;
          } else {
            throw new RuntimeException(
                String.format(
                    "A transform cannot be initiated using the provided config row %s and the"
                        + " TransformPayloadTranslator %s",
                    configRow, payloadTranslator));
          }
        } catch (Exception e) {
          throw new RuntimeException(
              String.format(
                  "Failed to build transform %s from spec %s: %s",
                  spec.getUrn(), spec, e.getMessage()),
              e);
        }
      }
    }

    @Override
    public InputT createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
      if (isSchemaTransform()) {
        return (InputT) ExpansionServiceSchemaTransformProvider.of().createInput(p, inputs);
      } else {
        return TransformProvider.super.createInput(p, inputs);
      }
    }

    @Override
    public Map<String, PCollection<?>> extractOutputs(OutputT output) {
      if (isSchemaTransform()) {
        return ExpansionServiceSchemaTransformProvider.of()
            .extractOutputs((PCollectionRowTuple) output);
      } else {
        return TransformProvider.super.extractOutputs(output);
      }
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof TransformProviderForPayloadTranslator
          && Objects.equals(
              payloadTranslator, ((TransformProviderForPayloadTranslator) other).payloadTranslator);
    }

    @Override
    public int hashCode() {
      return payloadTranslator.hashCode();
    }
  }

  private static class TransformProviderForBuilder implements TransformProvider {

    private final ExternalTransformBuilder transformBuilder;

    private TransformProviderForBuilder(ExternalTransformBuilder transformBuilder) {
      this.transformBuilder = transformBuilder;
    }

    @Override
    public PTransform getTransform(RunnerApi.FunctionSpec spec, PipelineOptions options) {
      try {
        Class configClass = getConfigClass(transformBuilder);
        return transformBuilder.buildExternal(
            payloadToConfig(
                ExternalConfigurationPayload.parseFrom(spec.getPayload()), configClass));
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to build transform from spec %s: %s", spec, e.getMessage()), e);
      }
    }

    @Override
    public List<String> getDependencies(RunnerApi.FunctionSpec spec, PipelineOptions options) {
      try {
        Class configClass = getConfigClass(transformBuilder);
        Optional<List<String>> dependencies =
            transformBuilder.getDependencies(
                payloadToConfig(
                    ExternalConfigurationPayload.parseFrom(spec.getPayload()), configClass),
                options);
        return dependencies.orElseGet(() -> TransformProvider.super.getDependencies(spec, options));
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to get dependencies for spec %s", spec), e);
      }
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof TransformProviderForBuilder
          && Objects.equals(
              transformBuilder, ((TransformProviderForBuilder) other).transformBuilder);
    }

    @Override
    public int hashCode() {
      return transformBuilder.hashCode();
    }
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

  static <ConfigT> Row decodeConfigObjectRow(SchemaApi.Schema schema, ByteString payload) {
    Schema payloadSchema = SchemaTranslation.schemaFromProto(schema);

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
                        : "@AssumeAssertion(nullness): converter type is imprecise; it is"
                            + " nullness-preserving";
                    return field.withName(newName);
                  } else {
                    return field;
                  }
                })
            .collect(Schema.toSchema());

    Row configRow;
    try {
      configRow = RowCoder.of(payloadSchema).decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException("Error decoding payload", e);
    }
    return configRow;
  }

  /**
   * Attempt to create an instance of {@link ConfigT} from an {@link ExternalConfigurationPayload}.
   * If a schema is registered for {@link ConfigT} this method will attempt to ise it. Throws an
   * {@link IllegalArgumentException} if the schema in {@code payload} is not {@link
   * Schema#assignableTo(Schema) assignable to} the registered schema.
   *
   * <p>If no Schema is registered, {@link ConfigT} must have a zero-argument constructor and
   * setters corresponding to each field in the row encoded by {@code payload}. Note {@link ConfigT}
   * may have additional setters not represented in the {@ocde payload} schema.
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
          "Configuration class '{}' has no schema registered. Attempting to construct with setter"
              + " approach.",
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

    Row payloadRow = decodeConfigObjectRow(payload.getSchema(), payload.getPayload());

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
    Row configRow = decodeConfigObjectRow(payload.getSchema(), payload.getPayload());

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
  private static <ConfigT> void invokeSetter(ConfigT config, @Nullable Object value, Method method)
      throws IllegalAccessException, InvocationTargetException {
    method.invoke(config, value);
  }

  private @MonotonicNonNull Map<String, TransformProvider> registeredTransforms;
  private final PipelineOptions commandLineOptions;
  private final @Nullable String loopbackAddress;

  public ExpansionService() {
    this(new String[] {});
  }

  public ExpansionService(String[] args) {
    this(PipelineOptionsFactory.fromArgs(args).create());
  }

  public ExpansionService(PipelineOptions opts) {
    this(opts, null);
  }

  public ExpansionService(PipelineOptions opts, @Nullable String loopbackAddress) {
    this.commandLineOptions = opts;
    this.loopbackAddress = loopbackAddress;
  }

  private Map<String, TransformProvider> getRegisteredTransforms() {
    if (registeredTransforms == null) {
      registeredTransforms = loadRegisteredTransforms();
    }
    return registeredTransforms;
  }

  private Iterable<SchemaTransformProvider> getRegisteredSchemaTransforms() {
    return ExpansionServiceSchemaTransformProvider.of().getAllProviders();
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

    PipelineOptions pipelineOptionsFromRequest =
        PipelineOptionsTranslation.fromProto(request.getPipelineOptions());
    Pipeline pipeline = createPipeline(pipelineOptionsFromRequest);

    boolean isUseDeprecatedRead =
        ExperimentalOptions.hasExperiment(commandLineOptions, "use_deprecated_read")
            || ExperimentalOptions.hasExperiment(
                commandLineOptions, "beam_fn_api_use_deprecated_read");
    if (!isUseDeprecatedRead) {
      ExperimentalOptions.addExperiment(
          pipeline.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
      // TODO(https://github.com/apache/beam/issues/20530): Remove this when we address performance
      // issue.
      ExperimentalOptions.addExperiment(
          pipeline.getOptions().as(ExperimentalOptions.class), "use_sdf_read");
    } else {
      LOG.warn(
          "Using use_depreacted_read in portable runners is runner-dependent. The "
              + "ExpansionService will respect that, but if your runner does not have support for "
              + "native Read transform, your Pipeline will fail during Pipeline submission.");
    }

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

    String urn = request.getTransform().getSpec().getUrn();

    TransformProvider transformProvider = getRegisteredTransforms().get(urn);
    if (transformProvider == null) {
      if (getUrn(ExpansionMethods.Enum.JAVA_CLASS_LOOKUP).equals(urn)) {
        AllowList allowList =
            commandLineOptions.as(ExpansionServiceOptions.class).getJavaClassLookupAllowlist();
        assert allowList != null;
        transformProvider = new JavaClassLookupTransformProvider(allowList);
      } else if (getUrn(SCHEMA_TRANSFORM).equals(urn)) {
        try {
          String underlyingIdentifier =
              ExternalTransforms.SchemaTransformPayload.parseFrom(
                      request.getTransform().getSpec().getPayload())
                  .getIdentifier();
          transformProvider = getRegisteredTransforms().get(underlyingIdentifier);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
        transformProvider =
            transformProvider != null
                ? transformProvider
                : ExpansionServiceSchemaTransformProvider.of();
      } else {
        throw new UnsupportedOperationException(
            "Unknown urn: " + request.getTransform().getSpec().getUrn());
      }
    }

    List<String> classpathResources =
        transformProvider.getDependencies(request.getTransform().getSpec(), pipeline.getOptions());
    pipeline.getOptions().as(PortablePipelineOptions.class).setFilesToStage(classpathResources);

    Map<String, PCollection<?>> outputs =
        transformProvider.apply(
            pipeline,
            request.getTransform().getUniqueName(),
            request.getTransform().getSpec(),
            inputs);

    // Needed to find which transform was new...
    SdkComponents sdkComponents =
        rehydratedComponents
            .getSdkComponents(request.getRequirementsList())
            .withNewIdPrefix(request.getNamespace());
    RunnerApi.Environment defaultEnvironment =
        Environments.createOrGetDefaultEnvironment(
            pipeline.getOptions().as(PortablePipelineOptions.class));
    if (commandLineOptions.as(ExpansionServiceOptions.class).getAlsoStartLoopbackWorker()) {
      PortablePipelineOptions externalOptions =
          PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
      externalOptions.setDefaultEnvironmentType(Environments.ENVIRONMENT_EXTERNAL);
      externalOptions.setDefaultEnvironmentConfig(loopbackAddress);
      defaultEnvironment =
          Environments.createAnyOfEnvironment(
              defaultEnvironment, Environments.createOrGetDefaultEnvironment(externalOptions));
    }
    sdkComponents.registerEnvironment(defaultEnvironment);
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

    if (isUseDeprecatedRead) {
      SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReadsIfNecessary(pipeline);
    }

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

  protected Pipeline createPipeline(PipelineOptions requestOptions) {
    // We expect the ExpansionRequest to contain a valid set of options to be used for this
    // expansion.
    // Additionally, we override selected options using options values set via command line or
    // ExpansionService wide overrides.

    PortablePipelineOptions requestPortablePipelineOptions =
        requestOptions.as(PortablePipelineOptions.class);
    PortablePipelineOptions commandLinePortablePipelineOptions =
        commandLineOptions.as(PortablePipelineOptions.class);
    Optional.ofNullable(commandLinePortablePipelineOptions.getDefaultEnvironmentType())
        .ifPresent(requestPortablePipelineOptions::setDefaultEnvironmentType);
    Optional.ofNullable(commandLinePortablePipelineOptions.getDefaultEnvironmentConfig())
        .ifPresent(requestPortablePipelineOptions::setDefaultEnvironmentConfig);
    List<String> filesToStage = commandLinePortablePipelineOptions.getFilesToStage();
    if (filesToStage != null) {
      requestPortablePipelineOptions
          .as(PortablePipelineOptions.class)
          .setFilesToStage(filesToStage);
    }
    requestPortablePipelineOptions
        .as(ExperimentalOptions.class)
        .setExperiments(commandLineOptions.as(ExperimentalOptions.class).getExperiments());
    requestPortablePipelineOptions.setRunner(NotRunnableRunner.class);
    requestPortablePipelineOptions
        .as(ExpansionServiceOptions.class)
        .setExpansionServiceConfig(
            commandLineOptions.as(ExpansionServiceOptions.class).getExpansionServiceConfig());
    return Pipeline.create(requestOptions);
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

  DiscoverSchemaTransformResponse discover(DiscoverSchemaTransformRequest request) {
    ExpansionServiceSchemaTransformProvider transformProvider =
        ExpansionServiceSchemaTransformProvider.of();
    DiscoverSchemaTransformResponse.Builder responseBuilder =
        DiscoverSchemaTransformResponse.newBuilder();
    for (org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider provider :
        transformProvider.getAllProviders()) {
      SchemaTransformConfig.Builder schemaTransformConfigBuilder =
          SchemaTransformConfig.newBuilder();
      schemaTransformConfigBuilder.setDescription(provider.description());
      schemaTransformConfigBuilder.setConfigSchema(
          SchemaTranslation.schemaToProto(provider.configurationSchema(), true));
      schemaTransformConfigBuilder.addAllInputPcollectionNames(provider.inputCollectionNames());
      schemaTransformConfigBuilder.addAllOutputPcollectionNames(provider.outputCollectionNames());
      responseBuilder.putSchemaTransformConfigs(
          provider.identifier(), schemaTransformConfigBuilder.build());
    }

    return responseBuilder.build();
  }

  @Override
  public void discoverSchemaTransform(
      DiscoverSchemaTransformRequest request,
      StreamObserver<DiscoverSchemaTransformResponse> responseObserver) {
    try {
      responseObserver.onNext(discover(request));
      responseObserver.onCompleted();
    } catch (RuntimeException exn) {
      responseObserver.onNext(
          ExpansionApi.DiscoverSchemaTransformResponse.newBuilder()
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

    // Register the options class used by the expansion service.
    PipelineOptionsFactory.register(ExpansionServiceOptions.class);
    @SuppressWarnings({"nullness"})
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(Arrays.copyOfRange(args, 1, args.length)).create();

    @SuppressWarnings("nullness")
    ExpansionService service = new ExpansionService(options, "localhost:" + port);

    StringBuilder registeredTransformsLog = new StringBuilder();
    boolean registeredTransformsFound = false;
    registeredTransformsLog.append("\n");
    registeredTransformsLog.append("Registered transforms:");

    for (Map.Entry<String, TransformProvider> entry :
        service.getRegisteredTransforms().entrySet()) {
      registeredTransformsFound = true;
      registeredTransformsLog.append("\n\t" + entry.getKey() + ": " + entry.getValue());
    }

    StringBuilder registeredSchemaTransformProvidersLog = new StringBuilder();
    boolean registeredSchemaTransformProvidersFound = false;
    registeredSchemaTransformProvidersLog.append("\n");
    registeredSchemaTransformProvidersLog.append("Registered SchemaTransformProviders:");

    for (SchemaTransformProvider provider : service.getRegisteredSchemaTransforms()) {
      registeredSchemaTransformProvidersFound = true;
      registeredSchemaTransformProvidersLog.append("\n\t" + provider.identifier());
    }

    if (registeredTransformsFound) {
      System.out.println(registeredTransformsLog.toString());
    }
    if (registeredSchemaTransformProvidersFound) {
      System.out.println(registeredSchemaTransformProvidersLog.toString());
    }
    if (!registeredTransformsFound && !registeredSchemaTransformProvidersFound) {
      System.out.println("\nDid not find any registered transforms or SchemaTransforms.\n");
    }

    boolean useAlts = options.as(ExpansionServiceOptions.class).getUseAltsServer();
    ServerBuilder serverBuilder =
        useAlts ? AltsServerBuilder.forPort(port) : ServerBuilder.forPort(port);

    if (useAlts) {
      LOG.info("Running with gRPC ALTS authentication.");
    }

    serverBuilder.addService(service).addService(new ArtifactRetrievalService());
    if (options.as(ExpansionServiceOptions.class).getAlsoStartLoopbackWorker()) {
      serverBuilder.addService(new ExternalWorkerService(options));
    }
    Server server = serverBuilder.build();
    server.start();
    server.awaitTermination();
  }

  private static class NotRunnableRunner extends PipelineRunner<PipelineResult> {
    public static NotRunnableRunner fromOptions(PipelineOptions opts) {
      return new NotRunnableRunner();
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
      throw new UnsupportedOperationException();
    }
  }
}
