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

import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for AvroSourceFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class AvroSourceFactoryTest {
  private final String pathToAvroFile = "/path/to/file.avro";

  Source<?> runTestCreateAvroSource(String filename,
                               @Nullable Long start,
                               @Nullable Long end,
                               CloudObject encoding)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("AvroSource");
    addString(spec, "filename", filename);
    if (start != null) {
      addLong(spec, "start_offset", start);
    }
    if (end != null) {
      addLong(spec, "end_offset", end);
    }

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    Source<?> source = SourceFactory.create(PipelineOptionsFactory.create(),
                                            cloudSource,
                                            new BatchModeExecutionContext());
    return source;
  }

  @Test
  public void testCreatePlainAvroByteSource() throws Exception {
    Coder<?> coder =
        WindowedValue.getValueOnlyCoder(BigEndianIntegerCoder.of());
    Source<?> source = runTestCreateAvroSource(
        pathToAvroFile, null, null, coder.asCloudObject());

    Assert.assertThat(source, new IsInstanceOf(AvroByteSource.class));
    AvroByteSource avroSource = (AvroByteSource) source;
    Assert.assertEquals(pathToAvroFile, avroSource.avroSource.filename);
    Assert.assertEquals(null, avroSource.avroSource.startPosition);
    Assert.assertEquals(null, avroSource.avroSource.endPosition);
    Assert.assertEquals(coder, avroSource.coder);
  }

  @Test
  public void testCreateRichAvroByteSource() throws Exception {
    Coder<?> coder =
        WindowedValue.getValueOnlyCoder(BigEndianIntegerCoder.of());
    Source<?> source = runTestCreateAvroSource(
        pathToAvroFile, 200L, 500L, coder.asCloudObject());

    Assert.assertThat(source, new IsInstanceOf(AvroByteSource.class));
    AvroByteSource avroSource = (AvroByteSource) source;
    Assert.assertEquals(pathToAvroFile, avroSource.avroSource.filename);
    Assert.assertEquals(200L, (long) avroSource.avroSource.startPosition);
    Assert.assertEquals(500L, (long) avroSource.avroSource.endPosition);
    Assert.assertEquals(coder, avroSource.coder);
  }

  @Test
  public void testCreateRichAvroSource() throws Exception {
    WindowedValue.WindowedValueCoder<?> coder =
        WindowedValue.getValueOnlyCoder(AvroCoder.of(Integer.class));
    Source<?> source = runTestCreateAvroSource(
        pathToAvroFile, 200L, 500L, coder.asCloudObject());

    Assert.assertThat(source, new IsInstanceOf(AvroSource.class));
    AvroSource avroSource = (AvroSource) source;
    Assert.assertEquals(pathToAvroFile, avroSource.filename);
    Assert.assertEquals(200L, (long) avroSource.startPosition);
    Assert.assertEquals(500L, (long) avroSource.endPosition);
    Assert.assertEquals(coder.getValueCoder(), avroSource.avroCoder);
  }
}
