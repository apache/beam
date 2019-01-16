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

import static org.apache.beam.runners.dataflow.util.Structs.addString;

import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AvroByteSinkFactory}. */
@RunWith(JUnit4.class)
public class AvroByteSinkFactoryTest {
  private final String pathToAvroFile = "/path/to/file.avro";

  private Sink<?> runTestCreateAvroSink(String filename, Coder<?> coder) throws Exception {
    CloudObject spec = CloudObject.forClassName("AvroSink");
    addString(spec, "filename", filename);
    PipelineOptions options = PipelineOptionsFactory.create();

    AvroByteSinkFactory factory = new AvroByteSinkFactory();
    Sink<?> sink =
        factory.create(
            spec,
            coder,
            options,
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create());
    return sink;
  }

  @Test
  public void testCreateAvroByteSink() throws Exception {
    Coder<?> coder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);
    Sink<?> sink = runTestCreateAvroSink(pathToAvroFile, coder);

    Assert.assertThat(sink, new IsInstanceOf(AvroByteSink.class));
    AvroByteSink<?> avroSink = (AvroByteSink<?>) sink;
    Assert.assertEquals(pathToAvroFile, avroSink.resourceId.toString());
    Assert.assertEquals(coder, avroSink.coder);
  }
}
