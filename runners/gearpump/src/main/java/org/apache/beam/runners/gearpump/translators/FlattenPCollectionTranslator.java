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

package org.apache.beam.runners.gearpump.translators;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;

/**
 * Flatten.FlattenPCollectionList is translated to Gearpump merge function.
 * Note only two-way merge is working now
 */
public class FlattenPCollectionTranslator<T> implements
    TransformTranslator<Flatten.FlattenPCollectionList<T>> {

  @Override
  public void translate(Flatten.FlattenPCollectionList<T> transform, TranslationContext context) {
    JavaStream<T> merged = null;
    for (PCollection<T> collection : context.getInput(transform).getAll()) {
      JavaStream<T> inputStream = context.getInputStream(collection);
      if (null == merged) {
        merged = inputStream;
      } else {
        merged = merged.merge(inputStream, transform.getName());
      }
    }
    context.setOutputStream(context.getOutput(transform), merged);
  }
}
