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
package org.apache.beam.runners.twister2.translators.batch;

import edu.iu.dsc.tws.tset.sets.batch.BatchTSetImpl;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import java.util.Iterator;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.runners.twister2.translators.functions.AssignWindowsFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Assign Window translator. */
public class AssignWindowTranslatorBatch<T> implements BatchTransformTranslator<Window.Assign<T>> {
  @Override
  public void translateNode(Window.Assign<T> transform, Twister2BatchTranslationContext context) {
    BatchTSetImpl<WindowedValue<T>> inputTTSet =
        context.getInputDataSet(context.getInput(transform));

    final WindowingStrategy<T, BoundedWindow> windowingStrategy =
        (WindowingStrategy<T, BoundedWindow>) context.getOutput(transform).getWindowingStrategy();

    WindowFn<T, BoundedWindow> windowFn = windowingStrategy.getWindowFn();
    ComputeTSet<WindowedValue<T>, Iterator<WindowedValue<T>>> outputTSet =
        inputTTSet.direct().compute(new AssignWindowsFunction(windowFn, context.getOptions()));
    context.setOutputDataSet(context.getOutput(transform), outputTSet);
  }
}
