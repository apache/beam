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

package org.apache.beam.runners.apex.translators;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.datatorrent.lib.stream.StreamMerger;

/**
 * Flatten.FlattenPCollectionList translation to Apex operator.
 * TODO: support more than two streams
 */
public class FlattenPCollectionTranslator<T> implements
    TransformTranslator<Flatten.FlattenPCollectionList<T>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(Flatten.FlattenPCollectionList<T> transform, TranslationContext context) {
    StreamMerger<T> operator = null;
    PCollectionList<T> collections = context.getInput();
    if (collections.size() > 2) {
      throw new UnsupportedOperationException("Currently supports only 2 collections: " + transform);
    }
    for (PCollection<T> collection : collections.getAll()) {
      if (null == operator) {
        operator = new StreamMerger<T>();
        context.addStream(collection, operator.data1);
      } else {
        context.addStream(collection, operator.data2);
      }
    }
    context.addOperator(operator, operator.out);
  }
}
