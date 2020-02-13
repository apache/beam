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

import io.gearpump.streaming.dsl.api.functions.MapFunction;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.gearpump.translators.io.UnboundedSourceWrapper;
import org.apache.beam.runners.gearpump.translators.io.ValuesSource;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/** Flatten.FlattenPCollectionList is translated to Gearpump merge function. */
public class FlattenPCollectionsTranslator<T>
    implements TransformTranslator<Flatten.PCollections<T>> {

  private static final long serialVersionUID = -5552148802472944759L;

  @Override
  public void translate(Flatten.PCollections<T> transform, TranslationContext context) {
    JavaStream<T> merged = null;
    Set<PCollection<T>> unique = new HashSet<>();
    for (PValue input : context.getInputs().values()) {
      PCollection<T> collection = (PCollection<T>) input;
      JavaStream<T> inputStream = context.getInputStream(collection);
      if (null == merged) {
        merged = inputStream;
      } else {
        // duplicate edges are not allowed in Gearpump graph
        // so we route through a dummy node
        if (unique.contains(collection)) {
          inputStream = inputStream.map(new DummyFunction<>(), "dummy");
        }

        merged = merged.merge(inputStream, 1, transform.getName());
      }
      unique.add(collection);
    }

    if (null == merged) {
      UnboundedSourceWrapper<String, ?> unboundedSourceWrapper =
          new UnboundedSourceWrapper<>(
              new ValuesSource<>(Lists.newArrayList("dummy"), StringUtf8Coder.of()),
              context.getPipelineOptions());
      merged = context.getSourceStream(unboundedSourceWrapper);
    }
    context.setOutputStream(context.getOutput(), merged);
  }

  private static class DummyFunction<T> extends MapFunction<T, T> {

    private static final long serialVersionUID = 5454396869997290471L;

    @Override
    public T map(T t) {
      return t;
    }
  }
}
