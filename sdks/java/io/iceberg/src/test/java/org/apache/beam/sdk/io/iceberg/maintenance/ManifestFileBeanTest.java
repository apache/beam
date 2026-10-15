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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManifestFileBeanTest {

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(TestFixtures.SCHEMA).withSpecId(3).identity("data").build();

  @Test
  public void testCarriedPartitionSpecSurvivesSerialization() throws Exception {
    ManifestFileBean bean =
        new ManifestFileBean(
                "/tmp/manifest.avro", 100L, SPEC.specId(), ManifestContent.DATA, 1L, false)
            .withPartitionSpec(SPEC);

    ManifestFileBean decoded = CoderUtils.clone(SerializableCoder.of(ManifestFileBean.class), bean);

    PartitionSpec carried = decoded.partitionSpec();
    assertNotNull(carried);
    assertEquals(SPEC.specId(), carried.specId());
    assertEquals(SPEC.fields(), carried.fields());
    assertEquals(SPEC.schema().asStruct(), carried.schema().asStruct());
  }

  @Test
  public void testNoCarriedPartitionSpecByDefault() {
    ManifestFileBean bean =
        new ManifestFileBean("/tmp/manifest.avro", 100L, 0, ManifestContent.DATA, 1L, true);
    assertNull(bean.partitionSpec());
    assertNull(bean.keyMetadata());
  }

  @Test
  public void testWithPartitionSpecRejectsMismatchedSpecId() {
    ManifestFileBean bean =
        new ManifestFileBean("/tmp/manifest.avro", 100L, 0, ManifestContent.DATA, 1L, true);
    assertThrows(IllegalArgumentException.class, () -> bean.withPartitionSpec(SPEC));
  }

  @Test
  public void testKeyMetadataPreservedFromManifestFile() throws Exception {
    byte[] key = "encryption-key-metadata".getBytes(StandardCharsets.UTF_8);
    ManifestFile source =
        new ManifestFileBean("/tmp/manifest.avro", 100L, 0, ManifestContent.DATA, 1L, true) {
          @Override
          public ByteBuffer keyMetadata() {
            return ByteBuffer.wrap(key);
          }
        };

    ManifestFileBean bean = ManifestFileBean.fromManifestFile(source, true);
    ManifestFileBean decoded = CoderUtils.clone(SerializableCoder.of(ManifestFileBean.class), bean);

    ByteBuffer keyMetadata = decoded.keyMetadata();
    assertNotNull(keyMetadata);
    assertArrayEquals(key, ByteBuffers.toByteArray(keyMetadata));
  }
}
