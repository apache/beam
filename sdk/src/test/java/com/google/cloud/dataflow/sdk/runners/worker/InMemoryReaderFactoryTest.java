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

import static com.google.cloud.dataflow.sdk.runners.worker.InMemoryReaderTest.encodedElements;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addStringList;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for InMemoryReaderFactory.
 */
@RunWith(JUnit4.class)
public class InMemoryReaderFactoryTest {
  static <T> Source createInMemoryCloudSource(
      List<T> elements, Long start, Long end, Coder<T> coder) throws Exception {
    List<String> encodedElements = encodedElements(elements, coder);

    CloudObject spec = CloudObject.forClassName("InMemorySource");
    addStringList(spec, PropertyNames.ELEMENTS, encodedElements);

    if (start != null) {
      addLong(spec, PropertyNames.START_INDEX, start);
    }
    if (end != null) {
      addLong(spec, PropertyNames.END_INDEX, end);
    }

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(coder.asCloudObject());

    return cloudSource;
  }

  <T> void runTestCreateInMemoryReader(List<T> elements, Long start, Long end, int expectedStart,
      int expectedEnd, Coder<T> coder) throws Exception {
    Source cloudSource = createInMemoryCloudSource(elements, start, end, coder);

    Reader<?> reader = ReaderFactory.create(
        PipelineOptionsFactory.create(), cloudSource, new BatchModeExecutionContext());
    Assert.assertThat(reader, new IsInstanceOf(InMemoryReader.class));
    InMemoryReader inMemoryReader = (InMemoryReader<?>) reader;
    Assert.assertEquals(encodedElements(elements, coder), inMemoryReader.encodedElements);
    Assert.assertEquals(expectedStart, inMemoryReader.startIndex);
    Assert.assertEquals(expectedEnd, inMemoryReader.endIndex);
    Assert.assertEquals(coder, inMemoryReader.coder);
  }

  @Test
  public void testCreatePlainInMemoryReader() throws Exception {
    runTestCreateInMemoryReader(
        Arrays.asList("hi", "there", "bob"), null, null, 0, 3, StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichInMemoryReader() throws Exception {
    runTestCreateInMemoryReader(
        Arrays.asList(33, 44, 55, 66, 77, 88), 1L, 3L, 1, 3, BigEndianIntegerCoder.of());
  }
}
