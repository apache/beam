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
package org.apache.beam.runners.jstorm.translation.translator;

import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.UnboundedSourceSpout;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Translates a {@link Read.Bounded} into a Storm spout.
 *
 * @param <T>
 */
public class BoundedSourceTranslator<T> extends TransformTranslator.Default<Read.Bounded<T>> {

  @Override
  public void translateNode(Read.Bounded<T> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    String description = describeTransform(transform, userGraphContext.getInputs(), userGraphContext.getOutputs());

    TupleTag<?> outputTag = userGraphContext.getOutputTag();
    PValue outputValue = userGraphContext.getOutput();
    UnboundedSourceSpout spout = new UnboundedSourceSpout(
        description,
        new UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter(transform.getSource()),
        userGraphContext.getOptions(), outputTag);

    context.getExecutionGraphContext().registerSpout(spout, TaggedPValue.of(outputTag, outputValue));
  }
}
