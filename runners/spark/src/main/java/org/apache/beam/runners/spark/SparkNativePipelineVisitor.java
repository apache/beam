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
package org.apache.beam.runners.spark;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * Pipeline visitor for translating a Beam pipeline into equivalent Spark operations. Used for
 * debugging purposes using {@link SparkRunnerDebugger}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkNativePipelineVisitor extends SparkRunner.Evaluator {
  private final List<NativeTransform> transforms;
  private final List<String> knownCompositesPackages =
      Lists.newArrayList(
          "org.apache.beam.sdk.transforms", "org.apache.beam.runners.spark.examples");

  SparkNativePipelineVisitor(SparkPipelineTranslator translator, EvaluationContext ctxt) {
    super(translator, ctxt);
    this.transforms = new ArrayList<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    CompositeBehavior compositeBehavior = super.enterCompositeTransform(node);
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      @SuppressWarnings("unchecked")
      final Class<PTransform<?, ?>> transformClass = (Class<PTransform<?, ?>>) transform.getClass();
      if (compositeBehavior == CompositeBehavior.ENTER_TRANSFORM
          && !knownComposite(transformClass)
          && shouldDebug(node)) {
        transforms.add(new NativeTransform(node, null, transform, true));
      }
    }
    return compositeBehavior;
  }

  private boolean knownComposite(Class<PTransform<?, ?>> transform) {
    String transformPackage = transform.getPackage().getName();
    for (String knownCompositePackage : knownCompositesPackages) {
      if (transformPackage.startsWith(knownCompositePackage)) {
        return true;
      }
    }
    return false;
  }

  private boolean shouldDebug(final TransformHierarchy.Node node) {
    return node == null
        || (transforms.stream()
                .noneMatch(
                    debugTransform ->
                        debugTransform.getNode().equals(node) && debugTransform.isComposite())
            && shouldDebug(node.getEnclosingNode()));
  }

  @Override
  <TransformT extends PTransform<? super PInput, POutput>> void doVisitTransform(
      TransformHierarchy.Node node) {
    @SuppressWarnings("unchecked")
    TransformT transform = (TransformT) node.getTransform();
    TransformEvaluator<TransformT> evaluator = translate(node, transform);
    if (shouldDebug(node)) {
      transforms.add(new NativeTransform(node, evaluator, transform, false));
    }
  }

  String getDebugString() {
    return Joiner.on("\n").join(transforms);
  }

  private static class NativeTransform {
    private final TransformHierarchy.Node node;
    private final TransformEvaluator<?> transformEvaluator;
    private final PTransform<?, ?> transform;
    private final boolean composite;

    NativeTransform(
        TransformHierarchy.Node node,
        TransformEvaluator<?> transformEvaluator,
        PTransform<?, ?> transform,
        boolean composite) {
      this.node = node;
      this.transformEvaluator = transformEvaluator;
      this.transform = transform;
      this.composite = composite;
    }

    TransformHierarchy.Node getNode() {
      return node;
    }

    boolean isComposite() {
      return composite;
    }

    @Override
    public String toString() {
      try {
        Class<? extends PTransform> transformClass = transform.getClass();
        if ("KafkaIO.Read".equals(node.getFullName())) {
          return "KafkaUtils.createDirectStream(...)";
        }
        if (composite) {
          return "_.<" + transformClass.getName() + ">";
        }
        String transformString = transformEvaluator.toNativeString();
        if (transformString.contains("<fn>")) {
          transformString = replaceFnString(transformClass, transformString, "fn");
        } else if (transformString.contains("<windowFn>")) {
          transformString = replaceFnString(transformClass, transformString, "windowFn");
        } else if (transformString.contains("<source>")) {
          String sourceName = "...";
          if (transform instanceof SplittableParDo.PrimitiveBoundedRead) {
            sourceName =
                ((SplittableParDo.PrimitiveBoundedRead<?>) transform)
                    .getSource()
                    .getClass()
                    .getName();
          } else if (transform instanceof SplittableParDo.PrimitiveUnboundedRead) {
            sourceName =
                ((SplittableParDo.PrimitiveUnboundedRead<?>) transform)
                    .getSource()
                    .getClass()
                    .getName();
          }
          transformString = transformString.replace("<source>", sourceName);
        }
        if (transformString.startsWith("sparkContext")
            || transformString.startsWith("streamingContext")) {
          return transformString;
        }
        return "_." + transformString;
      } catch (NoSuchMethodException
          | InvocationTargetException
          | IllegalAccessException
          | NoSuchFieldException e) {
        return "<FailedTranslation>";
      }
    }

    private String replaceFnString(
        Class<? extends PTransform> transformClass, String transformString, String fnFieldName)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            NoSuchFieldException {
      Object fn =
          transformClass.getMethod("get" + StringUtils.capitalize(fnFieldName)).invoke(transform);
      Class<?> fnClass = fn.getClass();
      String doFnName;
      Class<?> enclosingClass = fnClass.getEnclosingClass();
      if (enclosingClass != null && enclosingClass.equals(MapElements.class)) {
        Field parent = fnClass.getSuperclass().getDeclaredField("outer");
        parent.setAccessible(true);
        Field fnField = enclosingClass.getDeclaredField(fnFieldName);
        fnField.setAccessible(true);
        doFnName = fnField.get(parent.get(fn)).getClass().getName();
      } else {
        doFnName = fnClass.getName();
      }
      transformString = transformString.replace("<" + fnFieldName + ">", doFnName);
      return transformString;
    }
  }
}
