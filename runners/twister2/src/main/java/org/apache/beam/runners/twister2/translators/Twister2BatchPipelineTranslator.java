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
package org.apache.beam.runners.twister2.translators;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.Twister2PipelineOptions;
import org.apache.beam.runners.twister2.translators.batch.AssignWindowTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.FlattenTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.GroupByKeyTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.ImpulseTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.PCollectionViewTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.ParDoMultiOutputTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.ReadSourceTranslatorBatch;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Twister pipeline translator for batch pipelines. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Twister2BatchPipelineTranslator extends Twister2PipelineTranslator {

  private static final Logger LOG =
      Logger.getLogger(Twister2BatchPipelineTranslator.class.getName());

  /**
   * A map from {@link PTransform} subclass to the corresponding {@link BatchTransformTranslator} to
   * use to translate that transform.
   */
  private static final Map<String, BatchTransformTranslator> TRANSFORM_TRANSLATORS =
      new HashMap<>();

  private final Twister2BatchTranslationContext translationContext;

  static {
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoMultiOutputTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, new PCollectionViewTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new AssignWindowTranslatorBatch());
  }

  public Twister2BatchPipelineTranslator(
      Twister2PipelineOptions options, Twister2BatchTranslationContext twister2TranslationContext) {
    this.translationContext = twister2TranslationContext;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.fine(String.format("visiting transform %s", node.getTransform()));
    PTransform transform = node.getTransform();
    BatchTransformTranslator translator = getTransformTranslator(transform);
    if (null == translator) {
      throw new IllegalStateException("no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    translator.translateNode(transform, translationContext);
  }

  private BatchTransformTranslator<?> getTransformTranslator(PTransform transform) {
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSFORM_TRANSLATORS.get(urn);
  }
}
