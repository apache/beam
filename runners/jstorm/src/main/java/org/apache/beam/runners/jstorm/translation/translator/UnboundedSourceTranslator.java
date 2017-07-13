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

import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.UnboundedSourceSpout;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Translates a Read.Unbounded into a Storm spout.
 * 
 * @param <T>
 */
public class UnboundedSourceTranslator<T> extends TransformTranslator.Default<Read.Unbounded<T>> {
    public void translateNode(Read.Unbounded<T> transform, TranslationContext context) {
        TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
        String description = describeTransform(transform, userGraphContext.getInputs(), userGraphContext.getOutputs());

        TupleTag<?> tag = userGraphContext.getOutputTag();
        PValue output = userGraphContext.getOutput();

        UnboundedSourceSpout spout = new UnboundedSourceSpout(
                description,
                transform.getSource(), userGraphContext.getOptions(), tag);
        context.getExecutionGraphContext().registerSpout(spout, TaggedPValue.of(tag, output));
    }
}
