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

import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translation.wrappers.Twister2BoundedSource;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PCollection;

/** Source translator. */
public class ReadSourceTranslatorBatch<T>
    implements BatchTransformTranslator<SplittableParDo.PrimitiveBoundedRead<T>> {

  @Override
  public void translateNode(
      SplittableParDo.PrimitiveBoundedRead<T> transform, Twister2BatchTranslationContext context) {
    BoundedSource<T> boundedSource = transform.getSource();
    Twister2BoundedSource<T> twister2BoundedSource =
        new Twister2BoundedSource<T>(boundedSource, context, context.getOptions());
    final TSetEnvironment tsetEnv = context.getEnvironment();

    SourceTSet<WindowedValue<T>> sourceTSet =
        ((BatchTSetEnvironment) tsetEnv)
            .createSource(twister2BoundedSource, context.getOptions().getParallelism());
    PCollection<T> output = context.getOutput(transform);
    context.setOutputDataSet(output, sourceTSet);
  }
}
