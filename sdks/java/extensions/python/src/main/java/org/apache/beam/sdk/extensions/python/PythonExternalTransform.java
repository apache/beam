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
package org.apache.beam.sdk.extensions.python;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ClassUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.logicaltypes.PythonCallable;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transformservice.launcher.TransformServiceLauncher;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper for invoking external Python transforms. */
public class PythonExternalTransform<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {

  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();
  private String fullyQualifiedName;

  private String expansionService;
  private List<String> extraPackages;

  // We preseve the order here since Schema's care about order of fields but the order will not
  // matter when applying kwargs at the Python side.
  private SortedMap<String, Object> kwargsMap;
  private Map<java.lang.Class<?>, Schema.FieldType> typeHints;

  private @Nullable Object @NonNull [] argsArray;
  private @Nullable Row providedKwargsRow;

  Map<String, Coder<?>> outputCoders;

  private static final Logger LOG = LoggerFactory.getLogger(PythonExternalTransform.class);

  private PythonExternalTransform(String fullyQualifiedName, String expansionService) {
    this.fullyQualifiedName = fullyQualifiedName;
    this.expansionService = expansionService;
    this.extraPackages = new ArrayList<>();
    this.kwargsMap = new TreeMap<>();
    this.typeHints = new HashMap<>();
    // TODO(https://github.com/apache/beam/issues/21567): remove a default type hint for
    // PythonCallableSource when https://github.com/apache/beam/issues/21567 is
    // resolved
    this.typeHints.put(
        PythonCallableSource.class, Schema.FieldType.logicalType(new PythonCallable()));
    argsArray = new Object[] {};
    this.outputCoders = new HashMap<>();
  }

  /**
   * Instantiates a cross-language wrapper for a Python transform with a given transform name.
   *
   * <p>The given fully qualified name will be imported and called to instantiate the transform.
   * Often this is the fully qualified name of a Python {@code PTransform} class, in which case the
   * arguments will be passed to its constructor, but any callable will do.
   *
   * <p>Two special names, {@code __callable__} and {@code __constructor__} can be used to define a
   * suitable transform inline if none exists.
   *
   * <p>When {@code __callable__} is provided, the first argument (or {@code source} keyword
   * argument) should be a {@link PythonCallableSource} which represents the expand method of the
   * {@link PTransform} accepting and returning a {@code PValue} (and may also take additional
   * arguments and keyword arguments). For example, one might write
   *
   * <pre>
   * PythonExternalTransform
   *     .from("__callable__")
   *     .withArgs(
   *         PythonCallable.of("def expand(pcoll, x, y): return pcoll | ..."),
   *         valueForX,
   *         valueForY);
   * </pre>
   *
   * <p>When {@code __constructor__} is provided, the first argument (or {@code source} keyword
   * argument) should be a {@link PythonCallableSource} which will return the desired PTransform
   * when called with the remaining arguments and keyword arguments. Often this will be a {@link
   * PythonCallableSource} representing a PTransform class, for example
   *
   * <pre>
   * PythonExternalTransform
   *     .from("__constructor__")
   *     .withArgs(
   *         PythonCallable.of("class MyPTransform(beam.PTransform): ..."),
   *         ...valuesForMyPTransformConstructorIfAny);
   * </pre>
   *
   * @param transformName fully qualified transform name.
   * @param <InputT> Input {@link PCollection} type
   * @param <OutputT> Output {@link PCollection} type
   * @return A {@link PythonExternalTransform} for the given transform name.
   */
  public static <InputT extends PInput, OutputT extends POutput>
      PythonExternalTransform<InputT, OutputT> from(String transformName) {
    return new PythonExternalTransform<>(transformName, "");
  }

  /**
   * Instantiates a cross-language wrapper for a Python transform with a given transform name.
   *
   * <p>See {@link PythonExternalTransform#from(String)} for the meaning of transformName.
   *
   * @param transformName fully qualified transform name.
   * @param expansionService address and port number for externally launched expansion service
   * @param <InputT> Input {@link PCollection} type
   * @param <OutputT> Output {@link PCollection} type
   * @return A {@link PythonExternalTransform} for the given transform name.
   */
  public static <InputT extends PInput, OutputT extends POutput>
      PythonExternalTransform<InputT, OutputT> from(String transformName, String expansionService) {
    return new PythonExternalTransform<>(transformName, expansionService);
  }

  /**
   * Positional arguments for the Python cross-language transform. If invoked more than once, new
   * arguments will be appended to the previously specified arguments.
   *
   * @param args list of arguments.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withArgs(@NonNull Object... args) {
    @Nullable
    Object @NonNull [] result = Arrays.copyOf(this.argsArray, this.argsArray.length + args.length);
    System.arraycopy(args, 0, result, this.argsArray.length, args.length);
    this.argsArray = result;
    return this;
  }

  /**
   * Specifies a single keyword argument for the Python cross-language transform. This may be
   * invoked multiple times to add more than one keyword argument.
   *
   * @param name argument name.
   * @param value argument value
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withKwarg(String name, Object value) {
    if (providedKwargsRow != null) {
      throw new IllegalArgumentException("Kwargs were specified both directly and as a Row object");
    }
    kwargsMap.put(name, value);
    return this;
  }

  /**
   * Specifies keyword arguments for the Python cross-language transform. If invoked more than once,
   * new keyword arguments map will be added to the previously prided keyword arguments.
   *
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withKwargs(Map<String, Object> kwargs) {
    if (providedKwargsRow != null) {
      throw new IllegalArgumentException("Kwargs were specified both directly and as a Row object");
    }
    kwargsMap.putAll(kwargs);
    return this;
  }

  /**
   * Specifies keyword arguments as a Row objects.
   *
   * @param kwargs keyword arguments as a {@link Row} objects. An empty Row represents zero keyword
   *     arguments.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withKwargs(Row kwargs) {
    if (this.kwargsMap.size() > 0) {
      throw new IllegalArgumentException("Kwargs were specified both directly and as a Row object");
    }
    this.providedKwargsRow = kwargs;
    return this;
  }

  /**
   * Specifies the field type of arguments.
   *
   * <p>Type hints are especially useful for logical types since type inference does not work well
   * for logical types.
   *
   * @param argType A class object for the argument type.
   * @param fieldType A schema field type for the argument.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withTypeHint(
      java.lang.Class<?> argType, Schema.FieldType fieldType) {
    if (typeHints.containsKey(argType)) {
      throw new IllegalArgumentException(
          String.format("typehint for arg type %s already exists", argType));
    }
    typeHints.put(argType, fieldType);
    return this;
  }

  /**
   * Specifies the keys and {@link Coder}s of the output {@link PCollection}s produced by this
   * transform.
   *
   * @param outputCoders a mapping from output keys to {@link Coder}s.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withOutputCoders(
      Map<String, Coder<?>> outputCoders) {
    if (this.outputCoders.size() > 0) {
      throw new IllegalArgumentException("Output coders were already specified");
    }
    this.outputCoders.putAll(outputCoders);
    return this;
  }

  /**
   * Specifies the {@link Coder} of the output {@link PCollection}s produced by this transform.
   * Should only be used if this transform produces a single output.
   *
   * @param outputCoder output {@link Coder} of the transform.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withOutputCoder(Coder<?> outputCoder) {
    if (this.outputCoders.size() > 0) {
      throw new IllegalArgumentException("Output coders were already specified");
    }

    // Output key should not matter when only specifying a single output.
    this.outputCoders.put("random_output_key", outputCoder);
    return this;
  }

  /**
   * Specifies that the given Python packages are required for this transform, which will cause them
   * to be installed in both the construction-time and execution time environment.
   *
   * @param extraPackages a list of pip-installable package specifications, such as would be found
   *     in a requirements file.
   * @return updated wrapper for the cross-language transform.
   */
  public PythonExternalTransform<InputT, OutputT> withExtraPackages(List<String> extraPackages) {
    if (extraPackages.isEmpty()) {
      return this;
    }
    Preconditions.checkState(
        Strings.isNullOrEmpty(expansionService),
        "Extra packages only apply to auto-started expansion service.");
    this.extraPackages = extraPackages;
    return this;
  }

  @VisibleForTesting
  Row buildOrGetKwargsRow() {
    if (providedKwargsRow != null) {
      return providedKwargsRow;
    } else {
      Schema schema =
          generateSchemaFromFieldValues(
              kwargsMap.values().toArray(), kwargsMap.keySet().toArray(new String[] {}));
      return Row.withSchema(schema)
          .addValues(convertComplexTypesToRows(kwargsMap.values().toArray()))
          .build();
    }
  }

  // Types that are not one of following are considered custom types.
  // * Java primitives
  // * Type String
  // * Any Type explicitly annotated by withTypeHint()
  // * Type Row
  private boolean isCustomType(java.lang.Class<?> type) {
    boolean val =
        !(ClassUtils.isPrimitiveOrWrapper(type)
            || type == String.class
            || typeHints.containsKey(type)
            || Row.class.isAssignableFrom(type));
    return val;
  }

  // If the custom type has a registered schema, we use that. Otherwise, we try to register it using
  // 'JavaFieldSchema'.
  private Row convertCustomValue(Object value) {
    SerializableFunction<Object, Row> toRowFunc;
    try {
      toRowFunc =
          (SerializableFunction<Object, Row>) SCHEMA_REGISTRY.getToRowFunction(value.getClass());
    } catch (NoSuchSchemaException e) {
      SCHEMA_REGISTRY.registerSchemaProvider(value.getClass(), new JavaFieldSchema());
      try {
        toRowFunc =
            (SerializableFunction<Object, Row>) SCHEMA_REGISTRY.getToRowFunction(value.getClass());
      } catch (NoSuchSchemaException e1) {
        throw new RuntimeException(e1);
      }
    }
    return toRowFunc.apply(value);
  }

  private Object[] convertComplexTypesToRows(@Nullable Object @NonNull [] values) {
    Object[] converted = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if (value != null) {
        converted[i] = isCustomType(value.getClass()) ? convertCustomValue(value) : value;
      } else {
        throw new RuntimeException("Null values are not supported");
      }
    }
    return converted;
  }

  @VisibleForTesting
  Row buildOrGetArgsRow() {
    Schema schema = generateSchemaFromFieldValues(argsArray, null);
    Object[] convertedValues = convertComplexTypesToRows(argsArray);
    return Row.withSchema(schema).addValues(convertedValues).build();
  }

  private Schema generateSchemaDirectly(
      @Nullable Object @NonNull [] fieldValues, @NonNull String @Nullable [] fieldNames) {
    Schema.Builder builder = Schema.builder();
    int counter = 0;
    for (Object field : fieldValues) {
      if (field == null) {
        throw new RuntimeException("Null field values are not supported");
      }
      String fieldName = (fieldNames != null) ? fieldNames[counter] : "field" + counter;
      if (field instanceof Row) {
        // Rows are used as is but other types are converted to proper field types.
        builder.addRowField(fieldName, ((Row) field).getSchema());
      } else if (typeHints.containsKey(field.getClass())) {
        builder.addField(fieldName, typeHints.get(field.getClass()));
      } else {
        builder.addField(
            fieldName,
            StaticSchemaInference.fieldFromType(
                TypeDescriptor.of(field.getClass()),
                JavaFieldSchema.JavaFieldTypeSupplier.INSTANCE,
                Collections.emptyMap()));
      }

      counter++;
    }

    Schema schema = builder.build();
    return schema;
  }

  // We generate the Schema from the provided field names and values. If field names are
  // not provided, we generate them.
  private Schema generateSchemaFromFieldValues(
      @Nullable Object @NonNull [] fieldValues, @NonNull String @Nullable [] fieldNames) {
    return generateSchemaDirectly(fieldValues, fieldNames);
  }

  @VisibleForTesting
  ExternalTransforms.ExternalConfigurationPayload generatePayload() {
    Row argsRow = buildOrGetArgsRow();
    Row kwargsRow = buildOrGetKwargsRow();
    Schema.Builder schemaBuilder = Schema.builder();
    schemaBuilder.addStringField("constructor");
    if (argsRow.getValues().size() > 0) {
      schemaBuilder.addRowField("args", argsRow.getSchema());
    }
    if (kwargsRow.getValues().size() > 0) {
      schemaBuilder.addRowField("kwargs", kwargsRow.getSchema());
    }
    Schema payloadSchema = schemaBuilder.build();
    Row.Builder payloadRowBuilder = Row.withSchema(payloadSchema);
    payloadRowBuilder.addValue(fullyQualifiedName);
    if (argsRow.getValues().size() > 0) {
      payloadRowBuilder.addValue(argsRow);
    }
    if (kwargsRow.getValues().size() > 0) {
      payloadRowBuilder.addValue(kwargsRow);
    }
    try {
      return ExternalTransforms.ExternalConfigurationPayload.newBuilder()
          .setSchema(SchemaTranslation.schemaToProto(payloadSchema, true))
          .setPayload(
              ByteString.copyFrom(
                  CoderUtils.encodeToByteArray(
                      RowCoder.of(payloadSchema), payloadRowBuilder.build())))
          .build();
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isPythonAvailable() {
    for (String executable : ImmutableList.of("python3", "python")) {
      try {
        new ProcessBuilder(executable, "--version").start().waitFor();
        return true;
      } catch (IOException | InterruptedException exn) {
        // Ignore.
      }
    }
    return false;
  }

  private boolean isDockerAvailable() {
    String executable = "docker";
    try {
      new ProcessBuilder(executable, "--version").start().waitFor();
      return true;
    } catch (IOException | InterruptedException exn) {
      // Ignore.
    }
    return false;
  }

  @Override
  public OutputT expand(InputT input) {
    try {
      ExternalTransforms.ExternalConfigurationPayload payload = generatePayload();
      if (!Strings.isNullOrEmpty(expansionService)) {
        int portIndex = expansionService.lastIndexOf(':');
        if (portIndex <= 0) {
          throw new IllegalArgumentException(
              "Unexpected expansion service address. Expected to be in the "
                  + "format \"<host>:<port>\"");
        }
        PythonService.waitForPort(
            expansionService.substring(0, portIndex),
            Integer.parseInt(expansionService.substring(portIndex + 1, expansionService.length())),
            15000);
        return apply(input, expansionService, payload);
      } else {
        OutputT output = null;
        int port = PythonService.findAvailablePort();
        PipelineOptionsFactory.register(PythonExternalTransformOptions.class);
        PythonExternalTransformOptions options =
            input.getPipeline().getOptions().as(PythonExternalTransformOptions.class);
        boolean useTransformService = options.getUseTransformService();
        @Nullable String customBeamRequirement = options.getCustomBeamRequirement();
        boolean pythonAvailable = isPythonAvailable();
        boolean dockerAvailable = isDockerAvailable();

        File requirementsFile = null;
        if (!extraPackages.isEmpty()) {
          requirementsFile = File.createTempFile("requirements", ".txt");
          requirementsFile.deleteOnExit();
          try (Writer fout =
              new OutputStreamWriter(
                  new FileOutputStream(requirementsFile.getAbsolutePath()),
                  StandardCharsets.UTF_8)) {
            for (String pkg : extraPackages) {
              fout.write(pkg);
              fout.write('\n');
            }
          }
        }

        // We use the transform service if either of the following is true.
        // * It was explicitly requested.
        // * Python executable is not available in the system but Docker is available.
        if (useTransformService || (!pythonAvailable && dockerAvailable)) {
          // A unique project name ensures that this expansion gets a dedicated instance of the
          // transform service.
          String projectName = UUID.randomUUID().toString();

          String messageAppend =
              useTransformService
                  ? "it was explicitly requested"
                  : "a Python executable is not available in the system";
          LOG.info(
              "Using the Docker Compose based transform service since {}. Service will have the "
                  + "project name {} and will be made available at the port {}",
              messageAppend,
              projectName,
              port);

          String pythonRequirementsFile =
              requirementsFile != null ? requirementsFile.getAbsolutePath() : null;
          TransformServiceLauncher service =
              TransformServiceLauncher.forProject(projectName, port, pythonRequirementsFile);
          service.setBeamVersion(ReleaseInfo.getReleaseInfo().getSdkVersion());
          try {
            // Starting the transform service.
            service.start();
            // Waiting the service to be ready.
            service.waitTillUp(-1);
            // Expanding the transform.
            output = apply(input, String.format("localhost:%s", port), payload);
          } finally {
            // Shutting down the transform service.
            service.shutdown();
          }
          return output;
        } else {

          ImmutableList.Builder<String> args = ImmutableList.builder();
          args.add(
              "--port=" + port, "--fully_qualified_name_glob=*", "--pickle_library=cloudpickle");
          if (requirementsFile != null) {
            args.add("--requirements_file=" + requirementsFile.getAbsolutePath());
          }
          PythonService service =
              new PythonService(
                      "apache_beam.runners.portability.expansion_service_main", args.build())
                  .withExtraPackages(extraPackages);
          if (!Strings.isNullOrEmpty(customBeamRequirement)) {
            service = service.withCustomBeamRequirement(customBeamRequirement);
          }
          try (AutoCloseable p = service.start()) {
            // allow more time waiting for the port ready for transient expansion service setup.
            PythonService.waitForPort("localhost", port, 60000);
            return apply(input, String.format("localhost:%s", port), payload);
          }
        }
      }
    } catch (RuntimeException exn) {
      throw exn;
    } catch (Exception exn) {
      throw new RuntimeException(exn);
    }
  }

  private OutputT apply(
      InputT input,
      String expansionService,
      ExternalTransforms.ExternalConfigurationPayload payload) {
    PTransform<PInput, PCollectionTuple> transform =
        External.of(
                "beam:transforms:python:fully_qualified_named",
                payload.toByteArray(),
                expansionService)
            .withMultiOutputs()
            .withOutputCoder(this.outputCoders);
    PCollectionTuple outputs;
    if (input instanceof PCollection) {
      outputs = ((PCollection<?>) input).apply(transform);
    } else if (input instanceof PCollectionTuple) {
      outputs = ((PCollectionTuple) input).apply(transform);
    } else if (input instanceof PBegin) {
      outputs = ((PBegin) input).apply(transform);
    } else {
      throw new RuntimeException("Unhandled input type " + input.getClass());
    }
    Set<TupleTag<?>> tags = outputs.getAll().keySet();
    if (tags.size() == 1) {
      return (OutputT) outputs.get(Iterables.getOnlyElement(tags));
    } else {
      return (OutputT) outputs;
    }
  }
}
