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
package org.apache.beam.sdk.managed;

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Top-level {@link org.apache.beam.sdk.transforms.PTransform}s that build and instantiate turnkey
 * transforms.
 *
 * <h3>Available transforms</h3>
 *
 * <p>This API currently supports two operations: {@link Managed#read} and {@link Managed#write}.
 * Each one enumerates the available transforms in a {@code TRANSFORMS} map.
 *
 * <h3>Building a Managed turnkey transform</h3>
 *
 * <p>Turnkey transforms are represented as {@link SchemaTransform}s, which means each one has a
 * defined configuration. A given transform can be built with a {@code Map<String, Object>} that
 * specifies arguments using like so:
 *
 * <pre>{@code
 * PCollection<Row> rows = pipeline.apply(
 *       Managed.read(ICEBERG)
 *           .withConfig(ImmutableMap.<String, Object>.builder()
 *               .put("foo", "abc")
 *               .put("bar", 123)
 *               .build()))
 *       .getOutput();
 * }</pre>
 *
 * <p>Instead of specifying configuration arguments directly in the code, one can provide the
 * location to a YAML file that contains this information. Say we have the following {@code
 * config.yaml} file:
 *
 * <pre>{@code
 * foo: "abc"
 * bar: 123
 * }</pre>
 *
 * <p>The file's path can be passed in to the Managed API like so:
 *
 * <pre>{@code
 * PCollection<Row> inputRows = pipeline.apply(Create.of(...));
 *
 * inputRows.apply(Managed.write(ICEBERG).withConfigUrl("path/to/config.yaml"));
 * }</pre>
 */
public class Managed {

  // TODO: Dynamically generate a list of supported transforms
  public static final String ICEBERG = "iceberg";
  public static final String KAFKA = "kafka";
  public static final String BIGQUERY = "bigquery";

  // Supported SchemaTransforms
  public static final Map<String, String> READ_TRANSFORMS =
      ImmutableMap.<String, String>builder()
          .put(ICEBERG, getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_READ))
          .put(KAFKA, getUrn(ExternalTransforms.ManagedTransforms.Urns.KAFKA_READ))
          .put(BIGQUERY, getUrn(ExternalTransforms.ManagedTransforms.Urns.BIGQUERY_READ))
          .build();
  public static final Map<String, String> WRITE_TRANSFORMS =
      ImmutableMap.<String, String>builder()
          .put(ICEBERG, getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_WRITE))
          .put(KAFKA, getUrn(ExternalTransforms.ManagedTransforms.Urns.KAFKA_WRITE))
          .put(BIGQUERY, getUrn(ExternalTransforms.ManagedTransforms.Urns.BIGQUERY_WRITE))
          .build();

  /**
   * Instantiates a {@link Managed.ManagedTransform} transform for the specified source. The
   * supported managed sources are:
   *
   * <ul>
   *   <li>{@link Managed#ICEBERG} : Read from Apache Iceberg
   * </ul>
   */
  public static ManagedTransform read(String source) {
    return new AutoValue_Managed_ManagedTransform.Builder()
        .setIdentifier(
            Preconditions.checkNotNull(
                READ_TRANSFORMS.get(source.toLowerCase()),
                "An unsupported source was specified: '%s'. Please specify one of the following sources: %s",
                source,
                READ_TRANSFORMS.keySet()))
        .setSupportedIdentifiers(new ArrayList<>(READ_TRANSFORMS.values()))
        .build();
  }

  /**
   * Instantiates a {@link Managed.ManagedTransform} transform for the specified sink. The supported
   * managed sinks are:
   *
   * <ul>
   *   <li>{@link Managed#ICEBERG} : Write to Apache Iceberg
   * </ul>
   */
  public static ManagedTransform write(String sink) {
    return new AutoValue_Managed_ManagedTransform.Builder()
        .setIdentifier(
            Preconditions.checkNotNull(
                WRITE_TRANSFORMS.get(sink.toLowerCase()),
                "An unsupported sink was specified: '%s'. Please specify one of the following sinks: %s",
                sink,
                WRITE_TRANSFORMS.keySet()))
        .setSupportedIdentifiers(new ArrayList<>(WRITE_TRANSFORMS.values()))
        .build();
  }

  @AutoValue
  public abstract static class ManagedTransform extends PTransform<PInput, PCollectionRowTuple> {
    abstract String getIdentifier();

    abstract @Nullable Map<String, Object> getConfig();

    abstract @Nullable String getConfigUrl();

    @VisibleForTesting
    abstract List<String> getSupportedIdentifiers();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setIdentifier(String identifier);

      abstract Builder setConfig(@Nullable Map<String, Object> config);

      abstract Builder setConfigUrl(@Nullable String configUrl);

      @VisibleForTesting
      abstract Builder setSupportedIdentifiers(List<String> supportedIdentifiers);

      abstract ManagedTransform build();
    }

    /**
     * Use the input Map of configuration arguments to build and instantiate the underlying
     * transform. The map can ignore nullable parameters, but needs to include all required
     * parameters. Check the underlying transform's schema ({@link
     * SchemaTransformProvider#configurationSchema()}) to see which parameters are available.
     */
    public ManagedTransform withConfig(Map<String, Object> config) {
      return toBuilder().setConfig(config).build();
    }

    /**
     * Like {@link #withConfig(Map)}, but instead extracts the configuration arguments from a
     * specified YAML file location.
     */
    public ManagedTransform withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    @VisibleForTesting
    ManagedTransform withSupportedIdentifiers(List<String> supportedIdentifiers) {
      return toBuilder().setSupportedIdentifiers(supportedIdentifiers).build();
    }

    @Override
    public PCollectionRowTuple expand(PInput input) {
      PCollectionRowTuple inputTuple = resolveInput(input);

      ManagedSchemaTransformProvider.ManagedConfig managedConfig =
          ManagedSchemaTransformProvider.ManagedConfig.builder()
              .setTransformIdentifier(getIdentifier())
              .setConfig(YamlUtils.yamlStringFromMap(getConfig()))
              .setConfigUrl(getConfigUrl())
              .build();

      SchemaTransform underlyingTransform =
          new ManagedSchemaTransformProvider(getSupportedIdentifiers()).from(managedConfig);

      return inputTuple.apply(underlyingTransform);
    }

    @VisibleForTesting
    static PCollectionRowTuple resolveInput(PInput input) {
      if (input instanceof PBegin) {
        return PCollectionRowTuple.empty(input.getPipeline());
      } else if (input instanceof PCollection) {
        PCollection<?> inputCollection = (PCollection<?>) input;
        Preconditions.checkArgument(
            inputCollection.getCoder() instanceof RowCoder,
            "Input PCollection must contain Row elements with a set Schema "
                + "(using .setRowSchema()). Instead, found collection %s with coder: %s.",
            inputCollection.getName(),
            inputCollection.getCoder());
        return PCollectionRowTuple.of(
            ManagedTransformConstants.INPUT, (PCollection<Row>) inputCollection);
      } else if (input instanceof PCollectionRowTuple) {
        return (PCollectionRowTuple) input;
      }

      throw new IllegalArgumentException("Unsupported input type: " + input.getClass());
    }
  }
}
