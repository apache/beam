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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Stack;
import org.apache.beam.runners.mapreduce.MapReduceRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

/**
 * Pipeline translator for {@link MapReduceRunner}.
 */
public class GraphConverter extends Pipeline.PipelineVisitor.Defaults {

  private final TranslationContext context;
  private final Stack<StringBuilder> dotfileNodesBuilders;
  private final Map<TransformHierarchy.Node, Integer> enclosedTransformCounts;
  private final StringBuilder dotfileEdgesBuilder;

  private int indent;

  public GraphConverter(TranslationContext context) {
    this.context = checkNotNull(context, "context");
    this.enclosedTransformCounts = Maps.newHashMap();
    this.dotfileNodesBuilders = new Stack<>();
    this.dotfileEdgesBuilder = new StringBuilder();
    this.indent = 0;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    // check if current composite transforms need to be translated.
    // If not, all sub transforms will be translated in visitPrimitiveTransform.
    PTransform<?, ?> transform = node.getTransform();
    dotfileNodesBuilders.push(new StringBuilder());
    if (transform != null) {
      markEnclosedTransformCounts(node);
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyTransform(transform, node, translator);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
      indent += 2;
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    if (node.getTransform() != null) {
      if (enclosedTransformCounts.get(node) > 1) {
        dotfileNodesBuilders.peek().insert(0, new StringBuilder()
            .append(getIndent()).append(
                String.format("subgraph \"cluster_%s\" {", node.getFullName()))
            .append('\n')
            .append(getIndent()).append(
                String.format("  label=\"%s\";", node.getFullName()))
            .append('\n')
            .toString());
        dotfileNodesBuilders.peek().append(new StringBuilder()
            .append(getIndent()).append("}").append('\n')
            .toString());
      }
      StringBuilder top = dotfileNodesBuilders.pop();
      dotfileNodesBuilders.peek().append(top.toString());
      indent -= 2;
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (!node.isRootNode()) {
      markEnclosedTransformCounts(node);

      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);
      if (translator == null || !applyCanTranslate(transform, node, translator)) {
        throw new UnsupportedOperationException(
            "The transform " + transform + " is currently not supported.");
      }
      applyTransform(transform, node, translator);
    }
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    dotfileNodesBuilders.peek().append(getIndent())
        .append(String.format("\"%s\" [shape=ellipse];", value.getName()))
        .append('\n');
  }

  private void markEnclosedTransformCounts(TransformHierarchy.Node node) {
    TransformHierarchy.Node parent = node.getEnclosingNode();
    Integer primitiveCount = enclosedTransformCounts.get(parent);
    if (primitiveCount == null) {
      primitiveCount = 0;
    }
    enclosedTransformCounts.put(parent, primitiveCount + 1);
  }

  public String getDotfile() {
    return String.format(
        "\ndigraph G {\n%s%s}\n",
        dotfileNodesBuilders.peek().toString(),
        dotfileEdgesBuilder.toString());
  }

  private <T extends PTransform<?, ?>> void applyTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    dotfileNodesBuilders.peek()
        .append(getIndent())
        .append(String.format("\"%s\" [shape=box];", node.getFullName()))
        .append('\n');
    for (PValue input : node.getInputs().values()) {
      dotfileEdgesBuilder
          .append(String.format("  \"%s\" -> \"%s\";", input.getName(), node.getFullName()))
          .append('\n');
    }
    for (PValue output : node.getOutputs().values()) {
      dotfileEdgesBuilder
          .append(String.format("  \"%s\" -> \"%s\";", node.getFullName(), output.getName()))
          .append('\n');
    }

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
    context.getUserGraphContext().setCurrentNode(node);
    typedTranslator.translateNode(typedTransform, context);
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
    context.getUserGraphContext().setCurrentNode(node);
    return typedTranslator.canTranslate(typedTransform, context);
  }

  private String getIndent() {
    StringBuilder ret = new StringBuilder();
    for (int i = 0; i < indent; ++i) {
      ret.append(' ');
    }
    return ret.toString();
  }
}

