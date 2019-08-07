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
package org.apache.beam.runners.apex.translation;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.apex.translation.operators.ApexFlattenOperator;
import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.runners.apex.translation.utils.ValuesSource;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/** {@link Flatten.PCollections} translation to Apex operator. */
class FlattenPCollectionTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(Flatten.PCollections<T> transform, TranslationContext context) {
    List<PCollection<T>> inputCollections = extractPCollections(context.getInputs());

    if (inputCollections.isEmpty()) {
      // create a dummy source that never emits anything
      @SuppressWarnings("unchecked")
      UnboundedSource<T, ?> unboundedSource =
          new ValuesSource<>(Collections.EMPTY_LIST, VoidCoder.of());
      ApexReadUnboundedInputOperator<T, ?> operator =
          new ApexReadUnboundedInputOperator<>(unboundedSource, context.getPipelineOptions());
      context.addOperator(operator, operator.output);
    } else if (inputCollections.size() == 1) {
      context.addAlias(context.getOutput(), inputCollections.get(0));
    } else {
      @SuppressWarnings("unchecked")
      PCollection<T> output = (PCollection<T>) context.getOutput();
      Map<PCollection<?>, Integer> unionTags = Collections.emptyMap();
      flattenCollections(inputCollections, unionTags, output, context);
    }
  }

  private List<PCollection<T>> extractPCollections(Map<TupleTag<?>, PValue> inputs) {
    List<PCollection<T>> collections = Lists.newArrayList();
    for (PValue pv : inputs.values()) {
      checkArgument(
          pv instanceof PCollection,
          "Non-PCollection provided as input to flatten: %s of type %s",
          pv,
          pv.getClass().getSimpleName());
      collections.add((PCollection<T>) pv);
    }
    return collections;
  }

  /**
   * Flatten the given collections into the given result collection. Translates into a cascading
   * merge with 2 input ports per operator. The optional union tags can be used to identify the
   * source in the result stream, used to channel multiple side inputs to a single Apex operator
   * port.
   *
   * @param collections
   * @param unionTags
   * @param finalCollection
   * @param context
   */
  static <T> void flattenCollections(
      List<PCollection<T>> collections,
      Map<PCollection<?>, Integer> unionTags,
      PCollection<T> finalCollection,
      TranslationContext context) {
    List<PCollection<T>> remainingCollections = Lists.newArrayList();
    PCollection<T> firstCollection = null;
    while (!collections.isEmpty()) {
      for (PCollection<T> collection : collections) {
        if (null == firstCollection) {
          firstCollection = collection;
        } else {
          ApexFlattenOperator<T> operator = new ApexFlattenOperator<>();
          context.addStream(firstCollection, operator.data1);
          Integer unionTag = unionTags.get(firstCollection);
          operator.data1Tag = (unionTag != null) ? unionTag : 0;
          context.addStream(collection, operator.data2);
          unionTag = unionTags.get(collection);
          operator.data2Tag = (unionTag != null) ? unionTag : 0;

          if (!collection.getCoder().equals(firstCollection.getCoder())) {
            throw new UnsupportedOperationException("coders don't match");
          }

          if (collections.size() > 2) {
            PCollection<T> intermediateCollection =
                PCollection.createPrimitiveOutputInternal(
                    collection.getPipeline(),
                    collection.getWindowingStrategy(),
                    collection.isBounded(),
                    collection.getCoder());
            context.addOperator(operator, operator.out, intermediateCollection);
            remainingCollections.add(intermediateCollection);
          } else {
            // final stream merge
            context.addOperator(operator, operator.out, finalCollection);
          }
          firstCollection = null;
        }
      }
      if (firstCollection != null) {
        // push to next merge level
        remainingCollections.add(firstCollection);
        firstCollection = null;
      }
      if (remainingCollections.size() > 1) {
        collections = remainingCollections;
        remainingCollections = Lists.newArrayList();
      } else {
        collections = Lists.newArrayList();
      }
    }
  }
}
