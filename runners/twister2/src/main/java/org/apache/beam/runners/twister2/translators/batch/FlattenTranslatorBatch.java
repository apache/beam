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

import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.BatchTSetImpl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translation.wrappers.Twister2EmptySource;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PValue;

/** Flatten translator. */
public class FlattenTranslatorBatch<T>
    implements BatchTransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translateNode(
      Flatten.PCollections<T> transform, Twister2BatchTranslationContext context) {
    Collection<PValue> pcs = context.getInputs().values();
    List<BatchTSetImpl<WindowedValue<T>>> tSets = new ArrayList<>();
    BatchTSetImpl<WindowedValue<T>> unionTSet;

    if (pcs.isEmpty()) {
      final TSetEnvironment tsetEnv = context.getEnvironment();
      unionTSet =
          ((BatchTSetEnvironment) tsetEnv)
              .createSource(new Twister2EmptySource(), context.getOptions().getParallelism());
    } else {
      for (PValue pc : pcs) {
        BatchTSetImpl<WindowedValue<T>> curr = context.getInputDataSet(pc);
        tSets.add(curr);
      }

      BatchTSetImpl<WindowedValue<T>> first = tSets.remove(0);
      Collection<TSet<WindowedValue<T>>> others = new ArrayList<>();
      others.addAll(tSets);
      if (tSets.size() > 0) {
        unionTSet = first.union(others);
      } else {
        unionTSet = first;
      }
    }
    context.setOutputDataSet(context.getOutput(transform), unionTSet);
  }
}
