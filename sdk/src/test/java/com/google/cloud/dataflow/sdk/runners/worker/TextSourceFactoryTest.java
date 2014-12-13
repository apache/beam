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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for TextSourceFactory.
 */
@RunWith(JUnit4.class)
public class TextSourceFactoryTest {
  void runTestCreateTextSource(String filename,
                               @Nullable Boolean stripTrailingNewlines,
                               @Nullable Long start,
                               @Nullable Long end,
                               CloudObject encoding,
                               Coder<?> coder)
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

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    Source<?> source = SourceFactory.create(PipelineOptionsFactory.create(),
                                            cloudSource,
                                            new BatchModeExecutionContext());
    Assert.assertThat(source, new IsInstanceOf(TextSource.class));
    TextSource textSource = (TextSource) source;
    Assert.assertEquals(filename, textSource.filename);
    Assert.assertEquals(
        stripTrailingNewlines == null ? true : stripTrailingNewlines,
        textSource.stripTrailingNewlines);
    Assert.assertEquals(start, textSource.startPosition);
    Assert.assertEquals(end, textSource.endPosition);
    Assert.assertEquals(coder, textSource.coder);
  }

  @Test
  public void testCreatePlainTextSource() throws Exception {
    runTestCreateTextSource(
        "/path/to/file.txt", null, null, null,
        makeCloudEncoding("StringUtf8Coder"),
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichTextSource() throws Exception {
    runTestCreateTextSource(
        "gs://bucket/path/to/file2.txt", false, 200L, 500L,
        makeCloudEncoding("TextualIntegerCoder"),
        TextualIntegerCoder.of());
  }
}
