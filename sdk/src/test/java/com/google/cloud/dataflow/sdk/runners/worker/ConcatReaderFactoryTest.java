/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.readFully;
import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addList;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addStringList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@code ConcatReaderFactory}.
 */
@RunWith(JUnit4.class)
public class ConcatReaderFactoryTest {

  Source createSourcesWithInMemorySources(List<List<String>> allData) {
    List<Map<String, Object>> sourcesList = new ArrayList<>();
    Source source = new Source();

    for (List<String> data : allData) {
      CloudObject inMemorySourceSpec = CloudObject.forClassName("InMemorySource");

      Map<String, Object> inMemorySourceDictionary = new HashMap<>();
      addStringList(inMemorySourceSpec, PropertyNames.ELEMENTS, data);
      addLong(inMemorySourceSpec, PropertyNames.START_INDEX, 0L);
      addLong(inMemorySourceSpec, PropertyNames.END_INDEX, data.size());

      inMemorySourceDictionary.put(PropertyNames.SOURCE_SPEC, inMemorySourceSpec);

      CloudObject textSourceEncoding = makeCloudEncoding("StringUtf8Coder");
      inMemorySourceDictionary.put(PropertyNames.ENCODING, textSourceEncoding);

      sourcesList.add(inMemorySourceDictionary);
    }
    CloudObject spec = CloudObject.forClassName("ConcatSource");
    addList(spec, PropertyNames.CONCAT_SOURCE_SOURCES, sourcesList);

    source.setSpec(spec);
    return source;
  }

  private List<List<String>> createInMemorySourceData(int numSources, int dataPerSource) {
    List<List<String>> allData = new ArrayList<>();
    for (int i = 0; i < numSources; i++) {
      List<String> data = new ArrayList<>();
      for (int j = 0; j < dataPerSource; j++) {
        data.add("data j of source i");
      }
      allData.add(data);
    }
    return allData;
  }

  @Test
  public void testCreateConcatReaderWithOneSubSource() throws Exception {
    List<List<String>> allData = createInMemorySourceData(1, 10);

    Source source = createSourcesWithInMemorySources(allData);
    Reader<String> reader = ReaderFactory.create(null, source, null);
    assertNotNull(reader);

    List<String> expected = new ArrayList<>();
    for (List<String> data : allData) {
      expected.addAll(data);
    }

    List<String> actual = new ArrayList<>();
    readFully(reader, actual);

    assertEquals(actual.size(), 10);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testCreateConcatReaderWithManySubSources() throws Exception {
    List<List<String>> allData = createInMemorySourceData(15, 10);

    Source source = createSourcesWithInMemorySources(allData);
    Reader<String> reader = ReaderFactory.create(null, source, null);
    assertNotNull(reader);

    List<String> expected = new ArrayList<>();
    for (List<String> data : allData) {
      expected.addAll(data);
    }

    List<String> actual = new ArrayList<>();
    readFully(reader, actual);

    assertEquals(actual.size(), 150);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }
}
