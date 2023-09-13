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
package org.apache.beam.runners.core.construction;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.External.ExpandableTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation.KnownTransformPayloadTranslator;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.graph.PipelineValidator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transformservice.launcher.TransformServiceLauncher;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline toProto(Pipeline pipeline) {
    return toProto(pipeline, SdkComponents.create(pipeline.getOptions()));
  }

  public static RunnerApi.Pipeline toProto(Pipeline pipeline, boolean useDeprecatedViewTransforms) {
    return toProto(
        pipeline, SdkComponents.create(pipeline.getOptions()), useDeprecatedViewTransforms);
  }

  public static RunnerApi.Pipeline toProto(Pipeline pipeline, SdkComponents components) {
    return toProto(pipeline, components, false);
  }

  public static RunnerApi.Pipeline toProto(
      final Pipeline pipeline,
      final SdkComponents components,
      boolean useDeprecatedViewTransforms) {
    final List<String> rootIds = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          private final ListMultimap<Node, AppliedPTransform<?, ?, ?>> children =
              ArrayListMultimap.create();

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode()) {
              for (AppliedPTransform<?, ?, ?> pipelineRoot : children.get(node)) {
                rootIds.add(components.getExistingPTransformId(pipelineRoot));
              }
            } else {
              // TODO: Include DisplayData in the proto
              children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
              try {
                components.registerPTransform(
                    node.toAppliedPTransform(pipeline), children.get(node));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            // TODO: Include DisplayData in the proto
            children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
            try {
              components.registerPTransform(
                  node.toAppliedPTransform(pipeline), Collections.emptyList());
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });
    RunnerApi.Pipeline res =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(components.toComponents())
            .addAllRequirements(components.requirements())
            .addAllRootTransformIds(rootIds)
            .build();
    if (!useDeprecatedViewTransforms) {
      // TODO(JIRA-5649): Don't even emit these transforms in the generated protos.
      res = elideDeprecatedViews(res);
    }

    ExternalTranslationOptions externalTranslationOptions =
        pipeline.getOptions().as(ExternalTranslationOptions.class);
    List<String> urnsToOverride = externalTranslationOptions.getTransformsToOverride();
    if (urnsToOverride.size() > 0) {
      // We use PTransformPayloadTranslators and the Transform Service to re-generate the pipeline
      // proto components for the updated transform and update the pipeline proto.
      Map<String, PTransform> transforms = res.getComponents().getTransformsMap();
      List<String> alreadyCheckedURns = new ArrayList<>();
      for (Entry<String, PTransform> entry : transforms.entrySet()) {
        String urn = entry.getValue().getSpec().getUrn();
        if (!alreadyCheckedURns.contains(urn) && urnsToOverride.contains(urn)) {
          alreadyCheckedURns.add(urn);
          // All transforms in the pipeline with the given urns have to be overridden.
          List<
                  AppliedPTransform<
                      PInput,
                      POutput,
                      org.apache.beam.sdk.transforms.PTransform<? super PInput, POutput>>>
              appliedPTransforms =
                  findAppliedPTransforms(
                      urn, pipeline, KnownTransformPayloadTranslator.KNOWN_PAYLOAD_TRANSLATORS);
          for (AppliedPTransform<
                  PInput,
                  POutput,
                  org.apache.beam.sdk.transforms.PTransform<? super PInput, POutput>>
              appliedPTransform : appliedPTransforms) {
            TransformPayloadTranslator<
                    org.apache.beam.sdk.transforms.PTransform<? super PInput, POutput>>
                payloadTranslator =
                    KnownTransformPayloadTranslator.KNOWN_PAYLOAD_TRANSLATORS.get(
                        appliedPTransform.getTransform().getClass());
            try {
              // Override the transform using the transform service.
              res =
                  updateTransformViaTransformService(
                      urn, appliedPTransform, payloadTranslator, pipeline, res);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }

    // Validate that translation didn't produce an invalid pipeline.
    PipelineValidator.validate(res);
    return res;
  }

  private static int findAvailablePort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    try {
      return s.getLocalPort();
    } finally {
      s.close();
      try {
        // Some systems don't free the port for future use immediately.
        Thread.sleep(100);
      } catch (InterruptedException exn) {
        // ignore
      }
    }
  }

  // Override the given transform to the version available in a new transform service.
  private static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends org.apache.beam.sdk.transforms.PTransform<InputT, OutputT>>
      RunnerApi.Pipeline updateTransformViaTransformService(
          String urn,
          AppliedPTransform<
                  PInput,
                  POutput,
                  org.apache.beam.sdk.transforms.PTransform<? super PInput, POutput>>
              appliedPTransform,
          TransformPayloadTranslator<
                  org.apache.beam.sdk.transforms.PTransform<? super PInput, POutput>>
              originalPayloadTranslator,
          Pipeline pipeline,
          RunnerApi.Pipeline runnerAPIpipeline)
          throws IOException {
    ExternalTranslationOptions externalTranslationOptions =
        pipeline.getOptions().as(ExternalTranslationOptions.class);

    // Config row to re-construct the transform within the transform service.
    Row configRow = originalPayloadTranslator.toConfigRow(appliedPTransform.getTransform());
    ByteStringOutputStream outputStream = new ByteStringOutputStream();
    try {
      RowCoder.of(configRow.getSchema()).encode(configRow, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Java expansion serivice able to identify and expand transforms that includes the construction
    // config provided here.
    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .setSchema(SchemaTranslation.schemaToProto(configRow.getSchema(), true))
            .setPayload(outputStream.toByteString())
            .build();

    String serviceAddress = null;
    TransformServiceLauncher service = null;
    try {
      if (externalTranslationOptions.getTransformServiceAddress() != null) {
        serviceAddress = externalTranslationOptions.getTransformServiceAddress();
      } else if (externalTranslationOptions.getTransformServiceBeamVersion() != null) {
        String projectName = UUID.randomUUID().toString();
        service = TransformServiceLauncher.forProject(projectName, findAvailablePort());
        service.setBeamVersion(externalTranslationOptions.getTransformServiceBeamVersion());

        // Starting the transform service.
        service.start();
        // Waiting the service to be ready.
        service.waitTillUp(15000);
      } else {
        throw new IllegalArgumentException(
            "Either option TransformServiceAddress or option TransformServiceBeamVersion should be provided to override a transform using the transform service");
      }

      if (serviceAddress == null) {
        throw new IllegalArgumentException(
            "Cannot override the transform "
                + urn
                + " since a valid transform service address could not be determined");
      }

      // Creating an ExternalTransform and expanding it using the transform service.
      // Input will be the same input provided to the transform bing overridden.
      ExpandableTransform<InputT, OutputT> externalTransform =
          (ExpandableTransform<InputT, OutputT>)
              External.of(urn, payload.toByteArray(), serviceAddress);

      PCollectionTuple input = PCollectionTuple.empty(pipeline);
      for (TupleTag<?> tag : (Set<TupleTag<?>>) appliedPTransform.getInputs().keySet()) {
        PCollection<?> pc = appliedPTransform.getInputs().get(tag);
        if (pc == null) {
          throw new IllegalArgumentException(
              "Input of transform " + appliedPTransform + " with tag " + tag + " was null.");
        }
        input = input.and(tag, (PCollection) pc);
      }
      POutput output = externalTransform.expand((InputT) input);

      // Outputs of the transform being overridden.
      Map<TupleTag<?>, PCollection<?>> originalOutputs = appliedPTransform.getOutputs();

      // After expansion some transforms might still refer to the output of the already overridden
      // transform as their input.
      // Such inputs have to be overridden to use the output of the new upgraded transform.
      Map<String, String> inputReplacements = new HashMap<>();

      // Will contain the outputs of the upgraded transform.
      Map<TupleTag<?>, PCollection<?>> newOutputs = new HashMap<>();

      if (output instanceof PCollectionTuple) {
        newOutputs.putAll(((PCollectionTuple) output).getAll());
        for (Map.Entry<TupleTag<?>, PCollection<?>> entry : newOutputs.entrySet()) {
          if (entry == null) {
            throw new IllegalArgumentException(
                "Found unexpected null entry when iterating the outputs of expanded "
                    + "ExpandableTransform "
                    + externalTransform);
          }
          if (!appliedPTransform.getOutputs().containsKey(entry.getKey())) {
            throw new RuntimeException(
                "Could not find the tag " + entry.getKey() + " in the original set of outputs");
          }
          PCollection<?> originalOutputPc = originalOutputs.get(entry.getKey());
          if (originalOutputPc == null) {
            throw new IllegalArgumentException(
                "Original output of transform "
                    + appliedPTransform
                    + " with tag "
                    + entry.getKey()
                    + " was null");
          }
          inputReplacements.put(originalOutputPc.getName(), entry.getValue().getName());
        }
      } else if (output instanceof PCollection) {
        newOutputs.put(new TupleTag<>("temp_main_tag"), (PCollection) output);
        inputReplacements.put(
            originalOutputs.get(originalOutputs.keySet().iterator().next()).getName(),
            ((PCollection) output).getName());
      } else {
        throw new RuntimeException("Unexpected output type");
      }

      // We create a new AppliedPTransform to represent the upgraded transform and register it in an
      // SdkComponents object.
      AppliedPTransform<?, ?, ?> updatedAppliedPTransform =
          AppliedPTransform.of(
              appliedPTransform.getFullName() + "_external",
              appliedPTransform.getInputs(),
              newOutputs,
              externalTransform,
              externalTransform.getResourceHints(),
              appliedPTransform.getPipeline());
      SdkComponents updatedComponents =
          SdkComponents.create(
              runnerAPIpipeline.getComponents(), runnerAPIpipeline.getRequirementsList());
      String updatedTransformId =
          updatedComponents.registerPTransform(updatedAppliedPTransform, Collections.emptyList());
      RunnerApi.Components updatedRunnerApiComponents = updatedComponents.toComponents();

      // Recording input updates to the transforms to refer to the upgraded transform instead of the
      // old one.
      // Also recording the newly generated id of the old (overridden) transform in the
      // updatedRunnerApiComponents.
      Map<String, Map<String, String>> transformInputUpdates = new HashMap<>();
      List<String> oldTransformIds = new ArrayList<>();
      updatedRunnerApiComponents
          .getTransformsMap()
          .forEach(
              (transformId, transform) -> {
                // Mapping from existing key to new value.
                Map<String, String> updatedInputMap = new HashMap<>();
                for (Map.Entry<String, String> entry : transform.getInputsMap().entrySet()) {
                  if (inputReplacements.containsKey(entry.getValue())) {
                    updatedInputMap.put(entry.getKey(), inputReplacements.get(entry.getValue()));
                  }
                }
                for (Map.Entry<String, String> entry : transform.getOutputsMap().entrySet()) {
                  if (inputReplacements.containsKey(entry.getValue())
                      && urn.equals(transform.getSpec().getUrn())) {
                    oldTransformIds.add(transformId);
                  }
                }
                if (updatedInputMap.size() > 0) {
                  transformInputUpdates.put(transformId, updatedInputMap);
                }
              });
      // There should be only one recorded old (upgraded) transform.
      if (oldTransformIds.size() != 1) {
        throw new IOException(
            "Expected exactly one transform to be updated by "
                + oldTransformIds.size()
                + " were updated.");
      }
      String oldTransformId = oldTransformIds.get(0);

      // Updated list of root transforms (in case a root was upgraded).
      List<String> updaterRootTransformIds = new ArrayList<>();
      updaterRootTransformIds.addAll(runnerAPIpipeline.getRootTransformIdsList());
      if (updaterRootTransformIds.contains(oldTransformId)) {
        updaterRootTransformIds.remove(oldTransformId);
        updaterRootTransformIds.add(updatedTransformId);
      }

      // Generating the updated list of transforms.
      // Also updates the input references to refer to the upgraded transform.
      // Also updates the sub-transform reference to refer to the new transform.
      Map<String, RunnerApi.PTransform> updatedTransforms = new HashMap<>();
      updatedRunnerApiComponents
          .getTransformsMap()
          .forEach(
              (transformId, transform) -> {
                if (transformId.equals(oldTransformId)) {
                  // Do not include the old (upgraded) transform.
                  return;
                }
                PTransform.Builder transformBuilder = transform.toBuilder();
                if (transformInputUpdates.containsKey(transformId)) {
                  Map<String, String> inputUpdates = transformInputUpdates.get(transformId);
                  transformBuilder
                      .getInputsMap()
                      .forEach(
                          (key, value) -> {
                            if (inputUpdates.containsKey(key)) {
                              transformBuilder.putInputs(key, inputUpdates.get(key));
                            }
                          });
                }
                if (transform.getSubtransformsList().contains(oldTransformId)) {
                  List<String> updatedSubTransformsList = new ArrayList<>();
                  updatedSubTransformsList.addAll(transform.getSubtransformsList());
                  updatedSubTransformsList.remove(oldTransformId);
                  updatedSubTransformsList.add(updatedTransformId);
                  transformBuilder.clearSubtransforms();
                  transformBuilder.addAllSubtransforms(updatedSubTransformsList);
                }
                updatedTransforms.put(transformId, transformBuilder.build());
              });

      // Generating components with the updated list of transforms without including the old
      // (upgraded) transform.
      updatedRunnerApiComponents =
          updatedRunnerApiComponents
              .toBuilder()
              .putAllTransforms(updatedTransforms)
              .removeTransforms(oldTransformId)
              .build();

      // Generating the updated pipeline.
      RunnerApi.Pipeline updatedPipeline =
          RunnerApi.Pipeline.newBuilder()
              .setComponents(updatedRunnerApiComponents)
              .addAllRequirements(updatedComponents.requirements())
              .addAllRootTransformIds(updaterRootTransformIds)
              .build();

      return updatedPipeline;
    } catch (TimeoutException e) {
      throw new IOException(e);
    } finally {
      if (service != null) {
        service.shutdown();
      }
    }
  }

  // Find all AppliedPTransforms that represent transforms with the given URN.
  @SuppressWarnings({
    // Pre-registered 'knownTranslators' are defined as raw types.
    "rawtypes"
  })
  private static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends org.apache.beam.sdk.transforms.PTransform<? super InputT, OutputT>>
      List<AppliedPTransform<InputT, OutputT, TransformT>> findAppliedPTransforms(
          String urn,
          Pipeline pipeline,
          Map<
                  Class<? extends org.apache.beam.sdk.transforms.PTransform>,
                  TransformPayloadTranslator>
              knownTranslators) {

    List<AppliedPTransform<InputT, OutputT, TransformT>> appliedPTransforms = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {

          void findMatchingAppliedPTransform(Node node) {
            org.apache.beam.sdk.transforms.PTransform<?, ?> transform = node.getTransform();
            if (transform == null) {
              return;
            }
            if (knownTranslators.containsKey(transform.getClass())) {
              TransformPayloadTranslator<TransformT> translator =
                  knownTranslators.get(transform.getClass());
              if (translator.getUrn() != null && translator.getUrn().equals(urn)) {
                appliedPTransforms.add(
                    (AppliedPTransform<InputT, OutputT, TransformT>)
                        node.toAppliedPTransform(pipeline));
              }
            }
          }

          @Override
          public void leaveCompositeTransform(Node node) {
            findMatchingAppliedPTransform(node);
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            findMatchingAppliedPTransform(node);
          }
        });

    return appliedPTransforms;
  }

  private static RunnerApi.Pipeline elideDeprecatedViews(RunnerApi.Pipeline pipeline) {
    // Record data on CreateView operations.
    Set<String> viewTransforms = new HashSet<>();
    Map<String, String> viewOutputsToInputs = new HashMap<>();
    pipeline
        .getComponents()
        .getTransformsMap()
        .forEach(
            (transformId, transform) -> {
              if (transform
                  .getSpec()
                  .getUrn()
                  .equals(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN)) {
                viewTransforms.add(transformId);
                viewOutputsToInputs.put(
                    Iterables.getOnlyElement(transform.getOutputsMap().values()),
                    Iterables.getOnlyElement(transform.getInputsMap().values()));
              }
            });
    // Fix up view references.
    Map<String, RunnerApi.PTransform> newTransforms = new HashMap<>();
    pipeline
        .getComponents()
        .getTransformsMap()
        .forEach(
            (transformId, transform) -> {
              RunnerApi.PTransform.Builder transformBuilder = transform.toBuilder();
              transform
                  .getInputsMap()
                  .forEach(
                      (key, value) -> {
                        if (viewOutputsToInputs.containsKey(value)) {
                          transformBuilder.putInputs(key, viewOutputsToInputs.get(value));
                        }
                      });
              transform
                  .getOutputsMap()
                  .forEach(
                      (key, value) -> {
                        if (viewOutputsToInputs.containsKey(value)) {
                          transformBuilder.putOutputs(key, viewOutputsToInputs.get(value));
                        }
                      });
              // Unfortunately transformBuilder.getSubtransformsList().removeAll(viewTransforms)
              // throws UnsupportedOperationException.
              transformBuilder.clearSubtransforms();
              transformBuilder.addAllSubtransforms(
                  transform.getSubtransformsList().stream()
                      .filter(id -> !viewTransforms.contains(id))
                      .collect(Collectors.toList()));
              newTransforms.put(transformId, transformBuilder.build());
            });

    RunnerApi.Pipeline.Builder newPipeline = pipeline.toBuilder();
    // Replace transforms.
    newPipeline.getComponentsBuilder().putAllTransforms(newTransforms);
    // Remove CreateView operation components.
    viewTransforms.forEach(newPipeline.getComponentsBuilder()::removeTransforms);
    viewOutputsToInputs.keySet().forEach(newPipeline.getComponentsBuilder()::removePcollections);
    newPipeline.clearRootTransformIds();
    newPipeline.addAllRootTransformIds(
        pipeline.getRootTransformIdsList().stream()
            .filter(id -> !viewTransforms.contains(id))
            .collect(Collectors.toList()));
    return newPipeline.build();
  }
}
