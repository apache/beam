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

import org.apache.beam.runners.gearpump.translators.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.source.DataSource;

/**
 * {@link Read.Unbounded} is translated to Gearpump source function
 * and {@link UnboundedSource} is wrapped into Gearpump {@link DataSource}.
 */

public class ReadUnboundedTranslator<T> implements TransformTranslator<Read.Unbounded<T>> {

  private static final long serialVersionUID = 3529494817859948619L;

  @Override
  public void translate(Read.Unbounded<T> transform, TranslationContext context) {
    UnboundedSource<T, ?> unboundedSource = transform.getSource();
    UnboundedSourceWrapper<T, ?> unboundedSourceWrapper = new UnboundedSourceWrapper<>(
        unboundedSource, context.getPipelineOptions());
    JavaStream<WindowedValue<T>> sourceStream = context.getSourceStream(unboundedSourceWrapper);

    context.setOutputStream(context.getOutput(), sourceStream);
  }

}
