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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.addList;
import static org.apache.beam.runners.dataflow.util.Structs.addLong;
import static org.apache.beam.runners.dataflow.util.Structs.addStringList;
import static org.apache.beam.runners.dataflow.worker.ReaderUtils.readAllFromReader;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.Source;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@code ConcatReaderFactory}. */
@RunWith(JUnit4.class)
public class ConcatReaderFactoryTest {

  Source createSourcesWithInMemorySources(List<List<String>> allData) {
    List<Map<String, Object>> sourcesList = new ArrayList<>();
    Source source = new Source();

    for (List<String> data : allData) {
      CloudObject inMemorySourceSpec = CloudObject.forClassName("InMemorySource");

      Map<String, Object> inMemorySourceDictionary = new HashMap<>();
      addStringList(inMemorySourceSpec, WorkerPropertyNames.ELEMENTS, data);
      addLong(inMemorySourceSpec, WorkerPropertyNames.START_INDEX, 0L);
      addLong(inMemorySourceSpec, WorkerPropertyNames.END_INDEX, data.size());

      inMemorySourceDictionary.put(PropertyNames.SOURCE_SPEC, inMemorySourceSpec);

      CloudObject textSourceEncoding =
          CloudObjects.asCloudObject(StringUtf8Coder.of(), /*sdkComponents=*/ null);
      inMemorySourceDictionary.put(PropertyNames.ENCODING, textSourceEncoding);

      sourcesList.add(inMemorySourceDictionary);
    }
    CloudObject spec = CloudObject.forClassName("ConcatSource");
    addList(spec, WorkerPropertyNames.CONCAT_SOURCE_SOURCES, sourcesList);

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

    @SuppressWarnings("unchecked")
    NativeReader<String> reader =
        (NativeReader<String>) ReaderRegistry.defaultRegistry().create(source, null, null, null);
    assertNotNull(reader);

    List<String> expected = new ArrayList<>();
    for (List<String> data : allData) {
      expected.addAll(data);
    }
    assertThat(readAllFromReader(reader), containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testCreateConcatReaderWithManySubSources() throws Exception {
    List<List<String>> allData = createInMemorySourceData(15, 10);

    Source source = createSourcesWithInMemorySources(allData);

    @SuppressWarnings("unchecked")
    NativeReader<String> reader =
        (NativeReader<String>) ReaderRegistry.defaultRegistry().create(source, null, null, null);
    assertNotNull(reader);

    List<String> expected = new ArrayList<>();
    for (List<String> data : allData) {
      expected.addAll(data);
    }

    assertThat(readAllFromReader(reader), containsInAnyOrder(expected.toArray()));
  }
}
