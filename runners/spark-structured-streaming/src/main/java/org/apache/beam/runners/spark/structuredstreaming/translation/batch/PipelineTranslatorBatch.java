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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;

/**
 * {@link PipelineTranslator} for executing a {@link Pipeline} in Spark in batch mode. This contains
 * only the components specific to batch: registry of batch {@link TransformTranslator} and registry
 * lookup code.
 */
public class PipelineTranslatorBatch extends PipelineTranslator {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSFORM_TRANSLATORS = new HashMap<>();

  //TODO the ability to have more than one TransformTranslator per URN
  // that could be dynamically chosen by a predicated that evaluates based on PCollection
  // obtainable though node.getInputs.getValue()
  // See https://github.com/seznam/euphoria/blob/master/euphoria-spark/src/main/java/cz/seznam/euphoria/spark/SparkFlowTranslator.java#L83
  // And https://github.com/seznam/euphoria/blob/master/euphoria-spark/src/main/java/cz/seznam/euphoria/spark/SparkFlowTranslator.java#L106

  static {
    TRANSFORM_TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(Combine.Globally.class, new CombineGloballyTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(Flatten.PCollections.class, new FlattenTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(Window.Assign.class, new WindowAssignTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(ParDo.MultiOutput.class, new ParDoTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(Read.Bounded.class, new ReadSourceTranslatorBatch());

    TRANSFORM_TRANSLATORS
        .put(View.CreatePCollectionView.class, new CreatePCollectionViewTranslatorBatch());
  }

  public PipelineTranslatorBatch(SparkPipelineOptions options) {
    translationContext = new TranslationContext(options);
  }

  /** Returns a translator for the given node, if it is possible, otherwise null. */
  @Override
  protected TransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    @Nullable PTransform<?, ?> transform = node.getTransform();
    // Root of the graph is null
    if (transform == null) {
      return null;
    }
    return TRANSFORM_TRANSLATORS.get(transform.getClass());
  }
}
