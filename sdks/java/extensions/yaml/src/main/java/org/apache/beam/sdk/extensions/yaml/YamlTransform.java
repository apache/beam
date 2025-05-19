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
package org.apache.beam.sdk.extensions.yaml;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Allows one to invoke <a href="https://beam.apache.org/documentation/sdks/yaml/">Beam YAML</a>
 * transforms from Java.
 *
 * <p>This leverages Beam's cross-langauge transforms. Although python is required to parse and
 * expand the given transforms, the actual implementation may still be in Java.
 *
 * @param <InputT> the type of the input to this PTransform
 * @param <OutputT> the type of the output to this PTransform
 */
public class YamlTransform<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {

  /** The YAML definition of this transform. */
  private final String yamlDefinition;
  /**
   * If non-null, the set of input tags that are expected to be passed to this transform.
   *
   * <p>If null, a {@literal PCollection<Row>} or PBegin is expected.
   */
  private final @Nullable Set<String> inputTags;

  /**
   * If non-null, the set of output tags that are expected to be produced by this transform.
   *
   * <p>If null, exactly one output is expected and will be returned as a {@literal
   * PCollection<Row>}.
   */
  private final @Nullable Set<String> outputTags;

  private YamlTransform(
      String yamlDefinition,
      @Nullable Iterable<String> inputTags,
      @Nullable Iterable<String> outputTags) {
    this.yamlDefinition = yamlDefinition;
    this.inputTags = inputTags == null ? null : ImmutableSet.copyOf(inputTags);
    this.outputTags = outputTags == null ? null : ImmutableSet.copyOf(outputTags);
  }

  /**
   * Creates a new YamlTransform mapping a single input {@literal PCollection<Row>} to a single
   * {@literal PCollection<Row>} output.
   *
   * <p>Use {@link #withMultipleInputs} or {@link #withMultipleOutputs} to indicate that this
   * transform has multiple inputs and/or outputs.
   *
   * @param yamlDefinition a YAML string defining this transform.
   * @return a PTransform that applies this YAML to its inputs.
   */
  public static YamlTransform<PCollection<Row>, PCollection<Row>> of(String yamlDefinition) {
    return new YamlTransform<PCollection<Row>, PCollection<Row>>(yamlDefinition, null, null);
  }

  /**
   * Creates a new YamlTransform PBegin a single {@literal PCollection<Row>} output.
   *
   * @param yamlDefinition a YAML string defining this source.
   * @return a PTransform that applies this YAML as a root transform.
   */
  public static YamlTransform<PBegin, PCollection<Row>> source(String yamlDefinition) {
    return new YamlTransform<PBegin, PCollection<Row>>(yamlDefinition, null, null);
  }

  /**
   * Creates a new YamlTransform mapping a single input {@literal PCollection<Row>} to a single
   * {@literal PCollection<Row>} output.
   *
   * <p>Use {@link #withMultipleOutputs} to indicate that this sink has multiple (or no) or outputs.
   *
   * @param yamlDefinition a YAML string defining this sink.
   * @return a PTransform that applies this YAML to its inputs.
   */
  public static YamlTransform<PCollection<Row>, PCollection<Row>> sink(String yamlDefinition) {
    return of(yamlDefinition);
  }

  /**
   * Indicates that this YamlTransform expects multiple, named inputs.
   *
   * @param inputTags the set of expected input tags to this transform
   * @return a PTransform like this but with a {@link PCollectionRowTuple} input type.
   */
  public YamlTransform<PCollectionRowTuple, OutputT> withMultipleInputs(String... inputTags) {
    return new YamlTransform<PCollectionRowTuple, OutputT>(
        yamlDefinition, ImmutableSet.copyOf(inputTags), outputTags);
  }

  /**
   * Indicates that this YamlTransform expects multiple, named outputs.
   *
   * @param outputTags the set of expected output tags to this transform
   * @return a PTransform like this but with a {@link PCollectionRowTuple} output type.
   */
  public YamlTransform<InputT, PCollectionRowTuple> withMultipleOutputs(String... outputTags) {
    return new YamlTransform<InputT, PCollectionRowTuple>(
        yamlDefinition, inputTags, ImmutableSet.copyOf(outputTags));
  }

  @Override
  public OutputT expand(InputT input) {
    if (inputTags != null) {
      Set<String> actualInputTags =
          input.expand().keySet().stream()
              .map(TupleTag::getId)
              .collect(Collectors.toCollection(HashSet::new));
      if (!inputTags.equals(actualInputTags)) {
        throw new IllegalArgumentException(
            "Input has tags "
                + Joiner.on(", ").join(actualInputTags)
                + " but expected input tags "
                + Joiner.on(", ").join(inputTags));
      }
    }

    // There is no generic apply...
    POutput output;
    @SuppressWarnings("rawtypes")
    PTransform externalTransform =
        PythonExternalTransform.from("apache_beam.yaml.yaml_transform.YamlTransform")
            .withArgs(yamlDefinition)
            .withExtraPackages(ImmutableList.of("jinja2", "pyyaml", "virtualenv-clone"));
    if (input instanceof PBegin) {
      output = ((PBegin) input).apply(externalTransform);
    } else if (input instanceof PCollection) {
      output = ((PCollection<?>) input).apply(externalTransform);
    } else if (input instanceof PCollection) {
      output = ((PCollection<?>) input).apply(externalTransform);
    } else if (input instanceof PCollectionRowTuple) {
      output = ((PCollectionRowTuple) input).apply(externalTransform);
    } else {
      throw new IllegalArgumentException("Unrecognized input type: " + input);
    }

    if (outputTags == null) {
      if (!(output instanceof PCollection)) {
        throw new IllegalArgumentException(
            "Expected a single PCollection output, but got "
                + output
                + ". Perhaps withMultipleOutputs() needs to be specified?");
      }
      return (OutputT) output;
    } else {
      if (output instanceof PCollection) {
        // ExternalPythonTransform always returns single outputs as PCollections.
        if (outputTags.size() != 1) {
          throw new IllegalArgumentException(
              "Expected " + outputTags.size() + " outputs, but got exactly one.");
        }
        return (OutputT)
            PCollectionRowTuple.of(outputTags.iterator().next(), (PCollection<Row>) output);
      } else {
        Map<TupleTag<?>, PValue> expandedOutputs = output.expand();
        Set<String> actualOutputTags =
            expandedOutputs.keySet().stream()
                .map(TupleTag::getId)
                .collect(Collectors.toCollection(HashSet::new));
        if (!outputTags.equals(actualOutputTags)) {
          throw new IllegalArgumentException(
              "Output has tags "
                  + Joiner.on(", ").join(actualOutputTags)
                  + " but expected output tags "
                  + Joiner.on(", ").join(outputTags));
        }
        PCollectionRowTuple result = PCollectionRowTuple.empty(input.getPipeline());
        for (Map.Entry<TupleTag<?>, PValue> subOutput : expandedOutputs.entrySet()) {
          result = result.and(subOutput.getKey().getId(), (PCollection<Row>) subOutput.getValue());
        }
        return (OutputT) result;
      }
    }
  }
}
