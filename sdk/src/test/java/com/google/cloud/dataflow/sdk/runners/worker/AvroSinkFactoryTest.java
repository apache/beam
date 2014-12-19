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

import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for AvroSinkFactory.
 */
@RunWith(JUnit4.class)
public class AvroSinkFactoryTest {
  private final String pathToAvroFile = "/path/to/file.avro";

  Sink<?> runTestCreateAvroSink(String filename,
                                CloudObject encoding)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("AvroSink");
    addString(spec, "filename", filename);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(encoding);

    Sink<?> sink = SinkFactory.create(PipelineOptionsFactory.create(), cloudSink,
                                      new BatchModeExecutionContext());
    return sink;
  }

  @Test
  public void testCreateAvroByteSink() throws Exception {
    Coder<?> coder =
        WindowedValue.getValueOnlyCoder(BigEndianIntegerCoder.of());
    Sink<?> sink = runTestCreateAvroSink(
        pathToAvroFile, coder.asCloudObject());

    Assert.assertThat(sink, new IsInstanceOf(AvroByteSink.class));
    AvroByteSink avroSink = (AvroByteSink) sink;
    Assert.assertEquals(pathToAvroFile, avroSink.avroSink.filenamePrefix);
    Assert.assertEquals(coder, avroSink.coder);
  }

  @Test
  public void testCreateAvroSink() throws Exception {
    WindowedValue.WindowedValueCoder<?> coder =
        WindowedValue.getValueOnlyCoder(AvroCoder.of(Integer.class));
    Sink<?> sink = runTestCreateAvroSink(pathToAvroFile, coder.asCloudObject());

    Assert.assertThat(sink, new IsInstanceOf(AvroSink.class));
    AvroSink<?> avroSink = (AvroSink<?>) sink;
    Assert.assertEquals(pathToAvroFile, avroSink.filenamePrefix);
    Assert.assertEquals(coder.getValueCoder(), avroSink.avroCoder);
  }
}
