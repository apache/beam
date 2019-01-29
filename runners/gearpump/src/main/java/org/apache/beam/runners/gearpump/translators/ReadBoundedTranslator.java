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

import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.source.DataSource;
import org.apache.beam.runners.gearpump.translators.io.BoundedSourceWrapper;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * {@link Read.Bounded} is translated to Gearpump source function and {@link BoundedSource} is
 * wrapped into Gearpump {@link DataSource}.
 */
public class ReadBoundedTranslator<T> implements TransformTranslator<Read.Bounded<T>> {

  private static final long serialVersionUID = -3899020490896998330L;

  @Override
  public void translate(Read.Bounded<T> transform, TranslationContext context) {
    BoundedSource<T> boundedSource = transform.getSource();
    BoundedSourceWrapper<T> sourceWrapper =
        new BoundedSourceWrapper<>(boundedSource, context.getPipelineOptions());
    JavaStream<WindowedValue<T>> sourceStream = context.getSourceStream(sourceWrapper);

    context.setOutputStream(context.getOutput(), sourceStream);
  }
}
