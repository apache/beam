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

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.gearpump.streaming.dsl.api.functions.MapFunction;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.source.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

/** Tests for {@link FlattenPCollectionsTranslator}. */
public class FlattenPCollectionsTranslatorTest {

  private FlattenPCollectionsTranslator translator = new FlattenPCollectionsTranslator();
  private Flatten.PCollections transform = mock(Flatten.PCollections.class);

  private static class UnboundedSourceWrapperMatcher implements ArgumentMatcher<DataSource> {
    @Override
    public boolean matches(DataSource o) {
      return o instanceof UnboundedSourceWrapper;
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testTranslateWithEmptyCollection() {
    PCollection mockOutput = mock(PCollection.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    when(translationContext.getInputs()).thenReturn(Collections.EMPTY_MAP);
    when(translationContext.getOutput()).thenReturn(mockOutput);
    when(translationContext.getPipelineOptions())
        .thenReturn(PipelineOptionsFactory.as(GearpumpPipelineOptions.class));

    translator.translate(transform, translationContext);
    verify(translationContext).getSourceStream(argThat(new UnboundedSourceWrapperMatcher()));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testTranslateWithOneCollection() {
    JavaStream javaStream = mock(JavaStream.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    Map<TupleTag<?>, PValue> inputs = new HashMap<>();
    TupleTag tag = mock(TupleTag.class);
    PCollection mockCollection = mock(PCollection.class);
    inputs.put(tag, mockCollection);

    when(translationContext.getInputs()).thenReturn(inputs);
    when(translationContext.getInputStream(mockCollection)).thenReturn(javaStream);

    PValue mockOutput = mock(PValue.class);
    when(translationContext.getOutput()).thenReturn(mockOutput);

    translator.translate(transform, translationContext);
    verify(translationContext, times(1)).setOutputStream(mockOutput, javaStream);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testWithMoreThanOneCollections() {
    String transformName = "transform";
    when(transform.getName()).thenReturn(transformName);

    JavaStream javaStream1 = mock(JavaStream.class);
    JavaStream javaStream2 = mock(JavaStream.class);
    JavaStream mergedStream = mock(JavaStream.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    Map<TupleTag<?>, PValue> inputs = new HashMap<>();
    TupleTag tag1 = mock(TupleTag.class);
    PCollection mockCollection1 = mock(PCollection.class);
    inputs.put(tag1, mockCollection1);

    TupleTag tag2 = mock(TupleTag.class);
    PCollection mockCollection2 = mock(PCollection.class);
    inputs.put(tag2, mockCollection2);

    PCollection output = mock(PCollection.class);

    when(translationContext.getInputs()).thenReturn(inputs);
    when(translationContext.getInputStream(mockCollection1)).thenReturn(javaStream1);
    when(translationContext.getInputStream(mockCollection2)).thenReturn(javaStream2);
    when(javaStream1.merge(javaStream2, 1, transformName)).thenReturn(mergedStream);
    when(javaStream2.merge(javaStream1, 1, transformName)).thenReturn(mergedStream);

    when(translationContext.getOutput()).thenReturn(output);

    translator.translate(transform, translationContext);
    verify(translationContext).setOutputStream(output, mergedStream);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testWithDuplicatedCollections() {
    String transformName = "transform";
    when(transform.getName()).thenReturn(transformName);

    JavaStream javaStream1 = mock(JavaStream.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    Map<TupleTag<?>, PValue> inputs = new HashMap<>();
    TupleTag tag1 = mock(TupleTag.class);
    PCollection mockCollection1 = mock(PCollection.class);
    inputs.put(tag1, mockCollection1);

    TupleTag tag2 = mock(TupleTag.class);
    inputs.put(tag2, mockCollection1);

    when(translationContext.getInputs()).thenReturn(inputs);
    when(translationContext.getInputStream(mockCollection1)).thenReturn(javaStream1);
    when(translationContext.getPipelineOptions())
        .thenReturn(PipelineOptionsFactory.as(GearpumpPipelineOptions.class));

    translator.translate(transform, translationContext);
    verify(javaStream1).map(any(MapFunction.class), eq("dummy"));
    verify(javaStream1).merge(eq(null), eq(1), eq(transformName));
  }
}
