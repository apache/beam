/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.avro.generic.GenericRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for AvroIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class AvroIOTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testReadWithoutValidationFlag() throws Exception {
    AvroIO.Read.Bound<GenericRecord> read = AvroIO.Read.from("gs://bucket/foo*/baz");
    assertTrue(read.needsValidation());
    assertFalse(read.withoutValidation().needsValidation());
  }

  @Test
  public void testWriteWithoutValidationFlag() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write.to("gs://bucket/foo/baz");
    assertTrue(write.needsValidation());
    assertFalse(write.withoutValidation().needsValidation());
  }

  @Test
  public void testAvroIOGetName() {
    assertEquals("AvroIO.Read", AvroIO.Read.from("gs://bucket/foo*/baz").getName());
    assertEquals("AvroIO.Write", AvroIO.Write.to("gs://bucket/foo/baz").getName());
    assertEquals("ReadMyFile",
        AvroIO.Read.named("ReadMyFile").from("gs://bucket/foo*/baz").getName());
    assertEquals("WriteMyFile",
        AvroIO.Write.named("WriteMyFile").to("gs://bucket/foo/baz").getName());
  }

  // TODO: for Write only, test withSuffix, withNumShards,
  // withShardNameTemplate and withoutSharding.
}
