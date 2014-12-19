/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.InMemorySourceTest.encodedElements;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addStringList;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for InMemorySourceFactory.
 */
@RunWith(JUnit4.class)
public class InMemorySourceFactoryTest {
  static <T> com.google.api.services.dataflow.model.Source createInMemoryCloudSource(
      List<T> elements,
      Long start,
      Long end,
      Coder<T> coder)
      throws Exception {
    List<String> encodedElements = encodedElements(elements, coder);

    CloudObject spec = CloudObject.forClassName("InMemorySource");
    addStringList(spec, PropertyNames.ELEMENTS, encodedElements);

    if (start != null) {
      addLong(spec, PropertyNames.START_INDEX, start);
    }
    if (end != null) {
      addLong(spec, PropertyNames.END_INDEX, end);
    }

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(coder.asCloudObject());

    return cloudSource;
  }

  <T> void runTestCreateInMemorySource(List<T> elements,
                                       Long start,
                                       Long end,
                                       int expectedStart,
                                       int expectedEnd,
                                       Coder<T> coder)
      throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        createInMemoryCloudSource(elements, start, end, coder);

    Source<?> source = SourceFactory.create(PipelineOptionsFactory.create(), cloudSource,
                                            new BatchModeExecutionContext());
    Assert.assertThat(source, new IsInstanceOf(InMemorySource.class));
    InMemorySource<?> inMemorySource = (InMemorySource<?>) source;
    Assert.assertEquals(encodedElements(elements, coder),
                        inMemorySource.encodedElements);
    Assert.assertEquals(expectedStart, inMemorySource.startIndex);
    Assert.assertEquals(expectedEnd, inMemorySource.endIndex);
    Assert.assertEquals(coder, inMemorySource.coder);
  }

  @Test
  public void testCreatePlainInMemorySource() throws Exception {
    runTestCreateInMemorySource(
        Arrays.asList("hi", "there", "bob"),
        null, null,
        0, 3,
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichInMemorySource() throws Exception {
    runTestCreateInMemorySource(
        Arrays.asList(33, 44, 55, 66, 77, 88),
        1L, 3L,
        1, 3,
        BigEndianIntegerCoder.of());
  }
}
