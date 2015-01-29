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

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for TextReaderFactory.
 */
@RunWith(JUnit4.class)
public class TextReaderFactoryTest {
  void runTestCreateTextReader(String filename, @Nullable Boolean stripTrailingNewlines,
      @Nullable Long start, @Nullable Long end, CloudObject encoding, Coder<?> coder,
      CompressionType compressionType)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("TextSource");
    addString(spec, "filename", filename);
    if (stripTrailingNewlines != null) {
      addBoolean(spec, "strip_trailing_newlines", stripTrailingNewlines);
    }
    if (start != null) {
      addLong(spec, "start_offset", start);
    }
    if (end != null) {
      addLong(spec, "end_offset", end);
    }
    addString(spec, "compression_type", compressionType.toString());

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    Reader<?> reader = ReaderFactory.create(
        PipelineOptionsFactory.create(), cloudSource, new BatchModeExecutionContext());
    Assert.assertThat(reader, new IsInstanceOf(TextReader.class));
    TextReader textReader = (TextReader<?>) reader;
    Assert.assertEquals(filename, textReader.filename);
    Assert.assertEquals(
        stripTrailingNewlines == null ? true : stripTrailingNewlines,
        textReader.stripTrailingNewlines);
    Assert.assertEquals(start, textReader.startPosition);
    Assert.assertEquals(end, textReader.endPosition);
    Assert.assertEquals(coder, textReader.coder);
    Assert.assertEquals(compressionType, textReader.compressionType);
  }

  @Test
  public void testCreatePlainTextReader() throws Exception {
    runTestCreateTextReader("/path/to/file.txt", null, null, null,
        makeCloudEncoding("StringUtf8Coder"), StringUtf8Coder.of(), CompressionType.UNCOMPRESSED);
  }

  @Test
  public void testCreateRichTextReader() throws Exception {
    runTestCreateTextReader("gs://bucket/path/to/file2.txt", false, 200L, 500L,
        makeCloudEncoding("TextualIntegerCoder"), TextualIntegerCoder.of(),
        CompressionType.UNCOMPRESSED);
  }
}
