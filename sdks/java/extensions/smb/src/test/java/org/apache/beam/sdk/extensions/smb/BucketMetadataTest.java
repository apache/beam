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
package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link BucketMetadata}. */
public class BucketMetadataTest {

  @Test
  public void testCoding() throws Exception {
    final BucketMetadata<String, String> metadata =
        new TestBucketMetadata(1, 16, 4, HashType.MURMUR3_32);
    final BucketMetadata<String, String> copy = BucketMetadata.from(metadata.toString());

    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeterminism() {
    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new BucketMetadata(
                BucketMetadata.CURRENT_VERSION, 1, 1, Double.class, HashType.MURMUR3_32) {
              @Override
              public Object extractKey(Object value) {
                return null;
              }
            });
  }

  @Test
  public void testSubTyping() throws Exception {
    final BucketMetadata<String, String> test = new TestBucketMetadata(16, 4, HashType.MURMUR3_32);
    final BucketMetadata<String, GenericRecord> avro =
        new AvroBucketMetadata<>(16, 4, String.class, HashType.MURMUR3_32, "keyField");
    final BucketMetadata<String, TableRow> json =
        new JsonBucketMetadata<>(16, 4, String.class, HashType.MURMUR3_32, "keyField");

    Assert.assertEquals(TestBucketMetadata.class, BucketMetadata.from(test.toString()).getClass());
    Assert.assertEquals(AvroBucketMetadata.class, BucketMetadata.from(avro.toString()).getClass());
    Assert.assertEquals(JsonBucketMetadata.class, BucketMetadata.from(json.toString()).getClass());
  }

  @Test
  public void testCompatibility() throws Exception {
    final TestBucketMetadata m1 = new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32);
    final TestBucketMetadata m2 = new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32);
    final TestBucketMetadata m3 = new TestBucketMetadata(0, 1, 2, HashType.MURMUR3_32);
    final TestBucketMetadata m4 = new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_128);
    final TestBucketMetadata m5 = new TestBucketMetadata(1, 1, 1, HashType.MURMUR3_32);

    Assert.assertTrue(m1.isCompatibleWith(m2));
    Assert.assertTrue(m1.isCompatibleWith(m3));
    Assert.assertFalse(m1.isCompatibleWith(m4));
    Assert.assertFalse(m1.isCompatibleWith(m5));
  }
}
