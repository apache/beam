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

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Translates a {@link View.CreatePCollectionView} to a {@link FileWriteOperation}.
 */
public class ViewTranslator extends TransformTranslator.Default<View.CreatePCollectionView<?, ?>> {

  @Override
  public void translateNode(
      View.CreatePCollectionView<?, ?> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();

    PCollection<?> inPCollection = transform.getView().getPCollection();
    WindowingStrategy<?, ?> windowingStrategy = inPCollection.getWindowingStrategy();

    Graphs.Tag outTag = Iterables.getOnlyElement(userGraphContext.getOutputTags());
    String fileName = ConfigurationUtils.toFileName(outTag.getName());

    FileWriteOperation<?> operation = new FileWriteOperation<>(
        fileName,
        WindowedValue.getFullCoder(
            inPCollection.getCoder(), windowingStrategy.getWindowFn().windowCoder()));
    context.addInitStep(
        Graphs.Step.of(userGraphContext.getStepName(), operation),
        userGraphContext.getInputTags(),
        userGraphContext.getOutputTags());
  }
}
