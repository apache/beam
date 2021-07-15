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
package org.apache.beam.sdk.transforms.resourcehints;

import static junit.framework.TestCase.assertEquals;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ResourceHints} class. */
@RunWith(JUnit4.class)
public class ResourceHintsTest implements Serializable {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  private void verifyMinRamHintHelper(String hint, String expectedByteString) {
    assertEquals(
        expectedByteString,
        new String(
            ResourceHints.create()
                .withMinRam(hint)
                .hints()
                .get("beam:resources:min_ram_bytes:v1")
                .toBytes(),
            StandardCharsets.US_ASCII));
  }

  @Test
  public void testMinRamHintParsesCorrectly() {
    verifyMinRamHintHelper("123B", "123");
    verifyMinRamHintHelper("123 B", "123");
    verifyMinRamHintHelper("10 GiB", String.valueOf(1024L * 1024 * 1024 * 10));
    verifyMinRamHintHelper("10.5 GB", "10500000000");
  }

  @Test
  public void testMinRamStringHintDoesNotParseWhenNoUnitsSpecified() {
    thrown.expect(IllegalArgumentException.class);
    verifyMinRamHintHelper("10", null);
  }

  @Test
  public void testMinRamStringHintDoesNotParseWhenUnknownUnits() {
    thrown.expect(IllegalArgumentException.class);
    verifyMinRamHintHelper("10 BB", null);
  }

  @Test
  public void testAcceleratorHintParsesCorrectly() {
    assertEquals(
        "some_gpu",
        new String(
            ResourceHints.create()
                .withAccelerator("some_gpu")
                .hints()
                .get("beam:resources:accelerator:v1")
                .toBytes(),
            StandardCharsets.US_ASCII));
  }

  @Test
  public void testFromOptions() {
    ResourceHintsOptions options =
        PipelineOptionsFactory.fromArgs(
                "--resourceHints=minRam=1KB", "--resourceHints=beam:resources:bar=foo")
            .as(ResourceHintsOptions.class);
    assertEquals(
        ResourceHints.fromOptions(options),
        ResourceHints.create()
            .withMinRam(1000)
            .withHint("beam:resources:bar", new ResourceHints.StringHint("foo")));
    options =
        PipelineOptionsFactory.fromArgs(
                "--resourceHints=min_ram=1KB", "--resourceHints=accelerator=foo")
            .as(ResourceHintsOptions.class);
    assertEquals(
        ResourceHints.fromOptions(options),
        ResourceHints.create().withMinRam(1000).withAccelerator("foo"));
  }
}
