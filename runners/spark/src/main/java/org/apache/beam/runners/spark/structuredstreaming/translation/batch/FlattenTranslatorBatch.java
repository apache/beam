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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.sql.Dataset;

class FlattenTranslatorBatch<T>
    implements TransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

  @Override
  public void translateTransform(
      PTransform<PCollectionList<T>, PCollection<T>> transform, TranslationContext context) {
    Collection<PValue> pcollectionList = context.getInputs().values();
    Dataset<WindowedValue<T>> result = null;
    if (pcollectionList.isEmpty()) {
      result = context.emptyDataset();
    } else {
      for (PValue pValue : pcollectionList) {
        checkArgument(
            pValue instanceof PCollection,
            "Got non-PCollection input to flatten: %s of type %s",
            pValue,
            pValue.getClass().getSimpleName());
        @SuppressWarnings("unchecked")
        PCollection<T> pCollection = (PCollection<T>) pValue;
        Dataset<WindowedValue<T>> current = context.getDataset(pCollection);
        if (result == null) {
          result = current;
        } else {
          result = result.union(current);
        }
      }
    }
    context.putDataset(context.getOutput(), result);
  }
}
