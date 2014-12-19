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
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for TextSinkFactory.
 */
@RunWith(JUnit4.class)
public class TextSinkFactoryTest {
  void runTestCreateTextSink(String filename,
                             @Nullable Boolean appendTrailingNewlines,
                             @Nullable String header,
                             @Nullable String footer,
                             CloudObject encoding,
                             Coder<?> coder)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("TextSink");
    addString(spec, PropertyNames.FILENAME, filename);
    if (appendTrailingNewlines != null) {
      addBoolean(spec, PropertyNames.APPEND_TRAILING_NEWLINES, appendTrailingNewlines);
    }
    if (header != null) {
      addString(spec, PropertyNames.HEADER, header);
    }
    if (footer != null) {
      addString(spec, PropertyNames.FOOTER, footer);
    }

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(encoding);

    Sink<?> sink = SinkFactory.create(PipelineOptionsFactory.create(),
                                      cloudSink,
                                      new BatchModeExecutionContext());
    Assert.assertThat(sink, new IsInstanceOf(TextSink.class));
    TextSink<?> textSink = (TextSink<?>) sink;
    Assert.assertEquals(filename, textSink.namePrefix);
    Assert.assertEquals(
        appendTrailingNewlines == null ? true : appendTrailingNewlines,
        textSink.appendTrailingNewlines);
    Assert.assertEquals(header, textSink.header);
    Assert.assertEquals(footer, textSink.footer);
    Assert.assertEquals(coder, textSink.coder);
  }

  @Test
  public void testCreatePlainTextSink() throws Exception {
    runTestCreateTextSink(
        "/path/to/file.txt", null, null, null,
        makeCloudEncoding("StringUtf8Coder"),
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichTextSink() throws Exception {
    runTestCreateTextSink(
        "gs://bucket/path/to/file2.txt", false, "$$$", "***",
        makeCloudEncoding("TextualIntegerCoder"),
        TextualIntegerCoder.of());
  }
}
