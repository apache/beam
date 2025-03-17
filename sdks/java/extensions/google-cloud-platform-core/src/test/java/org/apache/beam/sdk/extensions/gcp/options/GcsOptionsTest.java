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
package org.apache.beam.sdk.extensions.gcp.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.extensions.gcp.options.GcsOptions}. */
@RunWith(JUnit4.class)
public class GcsOptionsTest {
  private static final String LONG_KEY = String.join("", Collections.nCopies(65, "a"));
  private static final String ENTRY_WITH_LONG_KEY =
      String.format("--gcsCustomAuditEntries={\"%s\":\"my_value\"}", LONG_KEY);
  private static final String LONG_VALUE = String.join("", Collections.nCopies(1201, "b"));
  private static final String ENTRY_WITH_LONG_VALUE =
      String.format("--gcsCustomAuditEntries={\"my_key\":\"%s\"}", LONG_VALUE);
  private static final String TOO_MANY_ENTRIES_WITHOUT_JOB =
      "--gcsCustomAuditEntries={\"a\":\"1\", \"b\":\"2\", \"c\":\"3\", \"d\":\"4\"}";
  private static final String TOO_MANY_ENTRIES_WITH_JOB =
      "--gcsCustomAuditEntries={\"a\":\"1\", \"b\":\"2\", \"c\":\"3\", \"job\":\"123\", \"d\":\"4\"}";

  @Test
  public void testEntries() throws Exception {
    GcsOptions options =
        PipelineOptionsFactory.fromArgs(
                "--gcsCustomAuditEntries={\"user\":\"test-user\", \"work\":\"test-work\", \"job\":\"test-job\", \"id\":\"1234\"}")
            .as(GcsOptions.class);

    Map<String, String> expected = new HashMap<String, String>();
    expected.put("x-goog-custom-audit-user", "test-user");
    expected.put("x-goog-custom-audit-work", "test-work");
    expected.put("x-goog-custom-audit-job", "test-job");
    expected.put("x-goog-custom-audit-id", "1234");
    assertEquals(expected, options.getGcsCustomAuditEntries());
  }

  @Test
  public void testEntriesWithErrors() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> PipelineOptionsFactory.fromArgs(ENTRY_WITH_LONG_KEY).as(GcsOptions.class));

    assertThrows(
        IllegalArgumentException.class,
        () -> PipelineOptionsFactory.fromArgs(ENTRY_WITH_LONG_VALUE).as(GcsOptions.class));

    assertThrows(
        IllegalArgumentException.class,
        () -> PipelineOptionsFactory.fromArgs(TOO_MANY_ENTRIES_WITHOUT_JOB).as(GcsOptions.class));

    assertThrows(
        IllegalArgumentException.class,
        () -> PipelineOptionsFactory.fromArgs(TOO_MANY_ENTRIES_WITH_JOB).as(GcsOptions.class));
  }
}
