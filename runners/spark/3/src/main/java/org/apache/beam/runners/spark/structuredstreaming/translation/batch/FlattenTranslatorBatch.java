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

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;

import java.util.Collection;
import java.util.Iterator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

class FlattenTranslatorBatch<T>
    extends TransformTranslator<PCollectionList<T>, PCollection<T>, Flatten.PCollections<T>> {

  FlattenTranslatorBatch() {
    super(0.1f);
  }

  @Override
  public void translate(Flatten.PCollections<T> transform, Context cxt) {
    Collection<PCollection<?>> pCollections = cxt.getInputs().values();
    Coder<T> outputCoder = cxt.getOutput().getCoder();
    Encoder<WindowedValue<T>> outputEnc =
        cxt.windowedEncoder(outputCoder, windowCoder(cxt.getOutput()));

    Dataset<WindowedValue<T>> result;
    Iterator<PCollection<T>> pcIt = (Iterator) pCollections.iterator();
    if (pcIt.hasNext()) {
      result = getDataset(pcIt.next(), outputCoder, outputEnc, cxt);
      while (pcIt.hasNext()) {
        result = result.union(getDataset(pcIt.next(), outputCoder, outputEnc, cxt));
      }
    } else {
      result = cxt.createDataset(ImmutableList.of(), outputEnc);
    }
    cxt.putDataset(cxt.getOutput(), result);
  }

  private Dataset<WindowedValue<T>> getDataset(
      PCollection<T> pc, Coder<T> coder, Encoder<WindowedValue<T>> enc, Context cxt) {
    Dataset<WindowedValue<T>> current = cxt.getDataset(pc);
    // if coders don't match, map using identity function to replace encoder
    return pc.getCoder().equals(coder) ? current : current.map(fun1(v -> v), enc);
  }
}
