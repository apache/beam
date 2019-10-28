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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.source.DataSource;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PValue;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

/** Tests for {@link ReadUnboundedTranslator}. */
public class ReadUnboundedTranslatorTest {

  private static class UnboundedSourceWrapperMatcher implements ArgumentMatcher<DataSource> {
    @Override
    public boolean matches(DataSource o) {
      return o instanceof UnboundedSourceWrapper;
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testTranslate() {
    ReadUnboundedTranslator translator = new ReadUnboundedTranslator();
    GearpumpPipelineOptions options =
        PipelineOptionsFactory.create().as(GearpumpPipelineOptions.class);
    Read.Unbounded transform = mock(Read.Unbounded.class);
    UnboundedSource source = mock(UnboundedSource.class);
    when(transform.getSource()).thenReturn(source);

    TranslationContext translationContext = mock(TranslationContext.class);
    when(translationContext.getPipelineOptions()).thenReturn(options);

    JavaStream stream = mock(JavaStream.class);
    PValue mockOutput = mock(PValue.class);
    when(translationContext.getOutput()).thenReturn(mockOutput);
    when(translationContext.getSourceStream(any(DataSource.class))).thenReturn(stream);

    translator.translate(transform, translationContext);
    verify(translationContext).getSourceStream(argThat(new UnboundedSourceWrapperMatcher()));
    verify(translationContext).setOutputStream(mockOutput, stream);
  }
}
