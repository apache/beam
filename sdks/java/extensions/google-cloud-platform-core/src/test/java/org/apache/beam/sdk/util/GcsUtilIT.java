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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link GcsUtil}. These tests are designed to run against production Google
 * Cloud Storage.
 *
 * <p>This is a runnerless integration test, even though the Beam IT framework assumes one. Thus,
 * this test should only be run against single runner (such as DirectRunner).
 */
@RunWith(JUnit4.class)
public class GcsUtilIT {
  /** Tests a rewrite operation that requires multiple API calls (using a continuation token). */
  @Test
  public void testRewriteMultiPart() throws IOException {
    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    // Setting the KMS key is necessary to trigger multi-part rewrites (gcpTempLocation is created
    // with a bucket default key).
    assertNotNull(gcsOptions.getDataflowKmsKey());

    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    String srcFilename = "gs://dataflow-samples/wikipedia_edits/wiki_data-000000000000.json";
    String dstFilename =
        gcsOptions.getGcpTempLocation()
            + String.format(
                "/GcsUtilIT-%tF-%<tH-%<tM-%<tS-%<tL.testRewriteMultiPart.copy", new Date());
    gcsUtil.maxBytesRewrittenPerCall = 50L * 1024 * 1024;
    gcsUtil.numRewriteTokensUsed = new AtomicInteger();

    gcsUtil.copy(Lists.newArrayList(srcFilename), Lists.newArrayList(dstFilename));

    assertThat(gcsUtil.numRewriteTokensUsed.get(), equalTo(3));
    assertThat(
        gcsUtil.getObject(GcsPath.fromUri(srcFilename)).getMd5Hash(),
        equalTo(gcsUtil.getObject(GcsPath.fromUri(dstFilename)).getMd5Hash()));

    gcsUtil.remove(Lists.newArrayList(dstFilename));
  }
}
