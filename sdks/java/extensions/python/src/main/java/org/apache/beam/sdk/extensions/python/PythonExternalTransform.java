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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ClassUtils;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Wrapper for invoking external Python transforms. */
public class PythonExternalTransform<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {

  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();
  private String fullyQualifiedName;
  private String expansionService;

  // We preseve the order here since Schema's care about order of fields but the order will not
  // matter when applying kwargs at the Python side.
  private SortedMap<String, Object> kwargsMap;

  private @Nullable Object @NonNull [] argsArray;
  private @Nullable Row providedKwargsRow;

  private PythonExternalTransform(String fullyQualifiedName, String expansionService) {
    this.fullyQualifiedName = fullyQualifiedName;
    this.expansionService = expansionService;
    this.kwargsMap = new TreeMap<>();
    argsArray = new Object[] {};
  }

  /**
   * Instantiates a cross-language wrapper for a Python transform with a given transform name.
   *
   * @param tranformName fully qualified transform name.
   * @param <InputT> Input {@link PCollection} type
   * @param <OutputT> Output {@link PCollection} type
   * @return A {@link PythonExternalTransform} for the given transform name.
   */
  public static <InputT extends PInput, OutputT extends POutput>
      PythonExternalTransform<InputT, OutputT> from(String tranformName) {
    return new PythonExternalTransform<>(tranformName, "");
  }

  /**
   * Instantiates a cross-language wrapper for a Python transform with a given transform name.
   *
   * @param tranformName fully qualified transform name.
   * @param expansionService address and port number for externally launched expansion service
   * @param <InputT> Input {@link PCollection} type
   * @param <OutputT> Output {@link PCollection} type
   * @return A {@link PythonExternalTransform} for the given transform name.
   */
  public static <InputT extends PInput, OutputT extends POutput>
      PythonExternalTransform<InputT, OutputT> from(String tranformName, String expansionService) {
    return new PythonExternalTransform<>(tranformName, expansionService);
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
  // * Type Row
  private static boolean isCustomType(java.lang.Class<?> type) {
    boolean val =
        !(ClassUtils.isPrimitiveOrWrapper(type)
            || type == String.class
            || Row.class.isAssignableFrom(type));
    return val;
  }

  // If the custom type has a registered schema, we use that. OTherwise we try to register it using
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
      } else {
        builder.addField(
            fieldName,
            StaticSchemaInference.fieldFromType(
                TypeDescriptor.of(field.getClass()),
                JavaFieldSchema.JavaFieldTypeSupplier.INSTANCE));
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
    payloadSchema.setUUID(UUID.randomUUID());
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

  @Override
  public OutputT expand(InputT input) {
    try {
      ExternalTransforms.ExternalConfigurationPayload payload = generatePayload();
      if (!Strings.isNullOrEmpty(expansionService)) {
        PythonService.waitForPort(
            Iterables.get(Splitter.on(':').split(expansionService), 0),
            Integer.parseInt(Iterables.get(Splitter.on(':').split(expansionService), 1)),
            15000);
        return apply(input, expansionService, payload);
      } else {
        int port = PythonService.findAvailablePort();
        PythonService service =
            new PythonService(
                "apache_beam.runners.portability.expansion_service_main",
                "--port",
                "" + port,
                "--fully_qualified_name_glob",
                "*");
        try (AutoCloseable p = service.start()) {
          PythonService.waitForPort("localhost", port, 15000);
          return apply(input, String.format("localhost:%s", port), payload);
        }
      }
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
            .withMultiOutputs();
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
