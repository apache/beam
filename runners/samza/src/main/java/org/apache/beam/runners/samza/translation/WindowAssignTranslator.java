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

package org.apache.beam.runners.samza.translation;

import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.runtime.WindowAssignOp;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.operators.MessageStream;

/**
 * Translates {@link org.apache.beam.sdk.transforms.windowing.Window.Assign} to Samza
 * {@link WindowAssignOp}.
 */
class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {
  @Override
  public void translate(Window.Assign<T> transform,
                        TransformHierarchy.Node node,
                        TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);

    @SuppressWarnings("unchecked")
    final WindowFn<T, ?> windowFn =
        (WindowFn<T, ?>) output.getWindowingStrategy().getWindowFn();

    final MessageStream<OpMessage<T>> inputStream =
        ctx.getMessageStream(ctx.getInput(transform));

    final MessageStream<OpMessage<T>> outputStream =
        inputStream.flatMap(OpAdapter.adapt(new WindowAssignOp<>(windowFn)));

    ctx.registerMessageStream(output, outputStream);
  }
}
