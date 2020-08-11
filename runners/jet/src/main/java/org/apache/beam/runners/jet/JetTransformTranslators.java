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
package org.apache.beam.runners.jet;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.CreatePCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.jet.processors.AssignWindowP;
import org.apache.beam.runners.jet.processors.BoundedSourceP;
import org.apache.beam.runners.jet.processors.FlattenP;
import org.apache.beam.runners.jet.processors.ImpulseP;
import org.apache.beam.runners.jet.processors.ParDoP;
import org.apache.beam.runners.jet.processors.StatefulParDoP;
import org.apache.beam.runners.jet.processors.UnboundedSourceP;
import org.apache.beam.runners.jet.processors.ViewP;
import org.apache.beam.runners.jet.processors.WindowGroupP;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

@SuppressWarnings("unchecked")
class JetTransformTranslators {

  /** A map from a Transform URN to the translator. */
  private static final Map<String, JetTransformTranslator> TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator());
    TRANSLATORS.put(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, new CreateViewTranslator());
    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslator());
    TRANSLATORS.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
    TRANSLATORS.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator());
    TRANSLATORS.put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowTranslator());
    TRANSLATORS.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
  }

  static JetTransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
    String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSLATORS.get(urn);
  }

  private static class ReadSourceTranslator<T>
      implements JetTransformTranslator<PTransform<PBegin, PCollection<T>>> {

    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());

      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);
      SerializablePipelineOptions pipelineOptions = context.getOptions();
      ProcessorMetaSupplier processorSupplier =
          getProcessorSupplier(
              (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
                  appliedTransform,
              outputCoder,
              vertexId,
              pipelineOptions);

      Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }

    private ProcessorMetaSupplier getProcessorSupplier(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>
            appliedTransform,
        Coder outputCoder,
        String vertexId,
        SerializablePipelineOptions pipelineOptions) {
      try {
        if (Utils.isBounded(appliedTransform)) {
          BoundedSource<T> source = ReadTranslation.boundedSourceFromTransform(appliedTransform);
          return BoundedSourceP.supplier(source, pipelineOptions, outputCoder, vertexId);
        } else {
          UnboundedSource<T, ?> source =
              ReadTranslation.unboundedSourceFromTransform(appliedTransform);
          if (source.requiresDeduping()) {
            throw new UnsupportedOperationException(
                "Sources requiring deduping not supported!"); // todo
          }
          return UnboundedSourceP.supplier(source, pipelineOptions, outputCoder, vertexId);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ParDoTranslator
      implements JetTransformTranslator<PTransform<PCollection, PCollectionTuple>> {

    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      boolean usesStateOrTimers = Utils.usesStateOrTimers(appliedTransform);
      DoFn<?, ?> doFn = Utils.getDoFn(appliedTransform);

      Map<TupleTag<?>, PValue> outputs = Utils.getOutputs(appliedTransform);

      TupleTag<?> mainOutputTag;
      try {
        mainOutputTag = ParDoTranslation.getMainOutputTag(appliedTransform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Map<TupleTag<?>, Integer> outputMap = new HashMap<>();
      int count = 1;
      for (TupleTag<?> tag : outputs.keySet()) {
        if (!outputMap.containsKey(tag)) {
          outputMap.put(tag, count++);
        }
      }
      final WindowingStrategy<?, ?> windowingStrategy =
          Utils.getWindowingStrategy(appliedTransform);

      Map<TupleTag<?>, Coder<?>> outputValueCoders = Utils.getOutputValueCoders(appliedTransform);
      Map<TupleTag<?>, Coder> outputCoders =
          Utils.getCoders(Utils.getOutputs(appliedTransform), Map.Entry::getKey);

      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String stepId =
          transformName.contains("/")
              ? transformName.substring(0, transformName.indexOf('/'))
              : transformName;
      String vertexId =
          dagBuilder.newVertexId(transformName) + (usesStateOrTimers ? " - STATEFUL" : "");
      SerializablePipelineOptions pipelineOptions = context.getOptions();
      Coder inputValueCoder = ((PCollection) Utils.getInput(appliedTransform)).getCoder();
      Coder inputCoder = Utils.getCoder((PCollection) Utils.getInput(appliedTransform));
      Collection<PCollectionView<?>> sideInputs = Utils.getSideInputs(appliedTransform);
      Map<? extends PCollectionView<?>, Coder> sideInputCoders =
          sideInputs.stream()
              .collect(Collectors.toMap(si -> si, si -> Utils.getCoder(si.getPCollection())));
      DoFnSchemaInformation doFnSchemaInformation =
          ParDoTranslation.getSchemaInformation(appliedTransform);
      Map<String, PCollectionView<?>> sideInputMappings = ParDoTranslation.getSideInputMapping(appliedTransform);
      SupplierEx<Processor> processorSupplier =
          usesStateOrTimers
              ? new StatefulParDoP.Supplier(
                  stepId,
                  vertexId,
                  doFn,
                  windowingStrategy,
                  doFnSchemaInformation,
                  pipelineOptions,
                  mainOutputTag,
                  outputMap.keySet(),
                  inputCoder,
                  sideInputCoders,
                  outputCoders,
                  inputValueCoder,
                  outputValueCoders,
                  sideInputs,
                  sideInputMappings)
              : new ParDoP.Supplier(
                  stepId,
                  vertexId,
                  doFn,
                  windowingStrategy,
                  doFnSchemaInformation,
                  pipelineOptions,
                  mainOutputTag,
                  outputMap.keySet(),
                  inputCoder,
                  sideInputCoders,
                  outputCoders,
                  inputValueCoder,
                  outputValueCoders,
                  sideInputs,
                  sideInputMappings);

      Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);
      dagBuilder.registerConstructionListeners((DAGBuilder.WiringListener) processorSupplier);

      PValue mainInput = Utils.getMainInput(pipeline, node);
      dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(mainInput), vertex);

      Map<TupleTag<?>, PValue> additionalInputs = Utils.getAdditionalInputs(node);
      if (additionalInputs != null && !additionalInputs.isEmpty()) {
        for (TupleTag<?> tupleTag : additionalInputs.keySet()) {
          dagBuilder.registerEdgeEndPoint(tupleTag.getId(), vertex);
        }
      }

      for (Map.Entry<TupleTag<?>, PValue> entry : outputs.entrySet()) {
        TupleTag<?> pCollId = entry.getKey();
        String edgeId = Utils.getTupleTagId(entry.getValue());
        dagBuilder.registerCollectionOfEdge(edgeId, pCollId.getId());
        dagBuilder.registerEdgeStartPoint(edgeId, vertex, outputCoders.get(pCollId));
      }

      return vertex;
    }
  }

  private static class GroupByKeyTranslator<K, InputT>
      implements JetTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      String transformName = appliedTransform.getFullName();

      PCollection<KV<K, InputT>> input =
          (PCollection<KV<K, InputT>>) Utils.getInput(appliedTransform);
      WindowedValue.WindowedValueCoder<KV<K, InputT>> inputCoder =
          Utils.getWindowedValueCoder(input);
      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder = Utils.getCoder((PCollection) output.getValue());

      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);
      Vertex vertex =
          dagBuilder.addVertex(
              vertexId,
              WindowGroupP.supplier(
                  context.getOptions(), inputCoder, outputCoder, windowingStrategy, vertexId));

      dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }
  }

  private static class CreateViewTranslator<T>
      implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      PCollectionView<T> view;
      try {
        view =
            CreatePCollectionViewTranslation.getView(
                (AppliedPTransform<
                        PCollection<T>, PCollection<T>, PTransform<PCollection<T>, PCollection<T>>>)
                    appliedTransform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);
      PCollection<T> input = (PCollection<T>) Utils.getInput(appliedTransform);
      Coder inputCoder = Utils.getCoder(input);
      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder = Utils.getCoder((PCollection) output.getValue());

      Vertex vertex =
          dagBuilder.addVertex(
              vertexId,
              ViewP.supplier(inputCoder, outputCoder, input.getWindowingStrategy(), vertexId));

      dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

      String viewTag = Utils.getTupleTagId(view);
      dagBuilder.registerSideInput(view);
      dagBuilder.registerCollectionOfEdge(viewTag, view.getTagInternal().getId());
      dagBuilder.registerEdgeStartPoint(viewTag, vertex, outputCoder);

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }
  }

  private static class FlattenTranslator<T>
      implements JetTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      Collection<PValue> mainInputs = Utils.getMainInputs(pipeline, node);
      Map<String, Coder> inputCoders =
          Utils.getCoders(
              Utils.getInputs(appliedTransform), e -> Utils.getTupleTagId(e.getValue()));
      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder = Utils.getCoder((PCollection) output.getValue());

      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(appliedTransform.getFullName());
      FlattenP.Supplier processorSupplier =
          new FlattenP.Supplier(inputCoders, outputCoder, vertexId);
      Vertex vertex = dagBuilder.addVertex(vertexId, processorSupplier);
      dagBuilder.registerConstructionListeners(processorSupplier);

      for (PValue value : mainInputs) {
        PCollection<T> input = (PCollection<T>) value;
        dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);
      }

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }
  }

  private static class WindowTranslator<T>
      implements JetTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>)
              ((PCollection) Utils.getOutput(appliedTransform).getValue()).getWindowingStrategy();

      PCollection<WindowedValue> input =
          (PCollection<WindowedValue>) Utils.getInput(appliedTransform);
      Coder inputCoder = Utils.getCoder(input);
      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());

      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);

      Vertex vertex =
          dagBuilder.addVertex(
              vertexId,
              AssignWindowP.supplier(inputCoder, outputCoder, windowingStrategy, vertexId));
      dagBuilder.registerEdgeEndPoint(Utils.getTupleTagId(input), vertex);

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }
  }

  private static class ImpulseTranslator
      implements JetTransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {
    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        Node node,
        JetTranslationContext context) {
      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);

      Map.Entry<TupleTag<?>, PValue> output = Utils.getOutput(appliedTransform);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());
      Vertex vertex = dagBuilder.addVertex(vertexId, ImpulseP.supplier(outputCoder, vertexId));

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }
  }
}
