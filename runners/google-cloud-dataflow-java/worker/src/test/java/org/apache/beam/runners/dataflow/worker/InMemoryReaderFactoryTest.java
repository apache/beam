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

import static org.apache.beam.runners.dataflow.util.Structs.addLong;
import static org.apache.beam.runners.dataflow.util.Structs.addStringList;

import com.google.api.services.dataflow.model.Source;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for InMemoryReaderFactory. */
@RunWith(JUnit4.class)
public class InMemoryReaderFactoryTest {
  static <T> Source createInMemoryCloudSource(
      List<T> elements, Long start, Long end, Coder<T> coder) throws Exception {
    List<String> encodedElements = InMemoryReaderTest.encodedElements(elements, coder);

    CloudObject spec = CloudObject.forClassName("InMemorySource");
    addStringList(spec, WorkerPropertyNames.ELEMENTS, encodedElements);

    if (start != null) {
      addLong(spec, WorkerPropertyNames.START_INDEX, start);
    }
    if (end != null) {
      addLong(spec, WorkerPropertyNames.END_INDEX, end);
    }

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null));

    return cloudSource;
  }

  <T> void runTestCreateInMemoryReader(
      List<T> elements, Long start, Long end, int expectedStart, int expectedEnd, Coder<T> coder)
      throws Exception {
    Source cloudSource = createInMemoryCloudSource(elements, start, end, coder);

    NativeReader<?> reader =
        ReaderRegistry.defaultRegistry()
            .create(
                cloudSource,
                PipelineOptionsFactory.create(),
                BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage"),
                TestOperationContext.create());
    Assert.assertThat(reader, new IsInstanceOf(InMemoryReader.class));
    InMemoryReader<?> inMemoryReader = (InMemoryReader<?>) reader;
    Assert.assertEquals(
        InMemoryReaderTest.encodedElements(elements, coder), inMemoryReader.encodedElements);
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
