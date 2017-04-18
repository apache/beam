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

import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.beam.runners.gearpump.translators.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.gearpump.streaming.dsl.api.functions.MapFunction;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.source.DataSource;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

/** Tests for {@link FlattenPCollectionsTranslator}. */
public class FlattenPCollectionsTranslatorTest {

  private FlattenPCollectionsTranslator translator = new FlattenPCollectionsTranslator();
  private Flatten.PCollections transform = mock(Flatten.PCollections.class);

  class UnboundedSourceWrapperMatcher extends ArgumentMatcher<DataSource> {
    @Override
    public boolean matches(Object o) {
      return o instanceof UnboundedSourceWrapper;
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testTranslateWithEmptyCollection() {
    PValue mockOutput = mock(PValue.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    when(translationContext.getInputs()).thenReturn(Collections.EMPTY_LIST);
    when(translationContext.getOutput()).thenReturn(mockOutput);

    translator.translate(transform, translationContext);
    verify(translationContext).getSourceStream(argThat(new UnboundedSourceWrapperMatcher()));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testTranslateWithOneCollection() {
    JavaStream javaStream = mock(JavaStream.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    TaggedPValue mockInput = mock(TaggedPValue.class);
    PCollection mockCollection = mock(PCollection.class);
    when(mockInput.getValue()).thenReturn(mockCollection);

    when(translationContext.getInputs()).thenReturn(Lists.newArrayList(mockInput));
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
    TranslationContext translationContext = mock(TranslationContext.class);

    TaggedPValue mockInput1 = mock(TaggedPValue.class);
    PCollection mockCollection1 = mock(PCollection.class);
    when(mockInput1.getValue()).thenReturn(mockCollection1);

    TaggedPValue mockInput2 = mock(TaggedPValue.class);
    PCollection mockCollection2 = mock(PCollection.class);
    when(mockInput2.getValue()).thenReturn(mockCollection2);

    when(translationContext.getInputs()).thenReturn(Lists.newArrayList(mockInput1, mockInput2));
    when(translationContext.getInputStream(mockCollection1)).thenReturn(javaStream1);
    when(translationContext.getInputStream(mockCollection2)).thenReturn(javaStream2);

    translator.translate(transform, translationContext);
    verify(javaStream1).merge(javaStream2, transformName);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testWithDuplicatedCollections() {
    String transformName = "transform";
    when(transform.getName()).thenReturn(transformName);

    JavaStream javaStream1 = mock(JavaStream.class);
    TranslationContext translationContext = mock(TranslationContext.class);

    PCollection mockCollection1 = mock(PCollection.class);
    TaggedPValue mockInput1 = mock(TaggedPValue.class);
    when(mockInput1.getValue()).thenReturn(mockCollection1);

    TaggedPValue mockInput2 = mock(TaggedPValue.class);
    when(mockInput2.getValue()).thenReturn(mockCollection1);

    when(translationContext.getInputs()).thenReturn(Lists.newArrayList(mockInput1, mockInput2));
    when(translationContext.getInputStream(mockCollection1)).thenReturn(javaStream1);

    translator.translate(transform, translationContext);
    verify(javaStream1).map(any(MapFunction.class), eq("dummy"));
    verify(javaStream1).merge(any(JavaStream.class), eq(transformName));
  }
}
