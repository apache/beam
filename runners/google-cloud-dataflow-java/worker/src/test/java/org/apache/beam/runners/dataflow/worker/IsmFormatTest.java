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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.Footer;
import org.apache.beam.runners.dataflow.internal.IsmFormat.FooterCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmShard;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmShardCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefix;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefixCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.CoderPropertiesTest.NonDeterministicCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link IsmFormat}. */
@RunWith(JUnit4.class)
public class IsmFormatTest {
  private static final Coder<String> NON_DETERMINISTIC_CODER = new NonDeterministicCoder();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testUsingNonDeterministicShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    IsmFormat.validateCoderIsCompatible(
        IsmRecordCoder.of(
            1, // number or shard key coders for value records
            0, // number of shard key coders for metadata records
            ImmutableList.<Coder<?>>of(NON_DETERMINISTIC_CODER, ByteArrayCoder.of()),
            ByteArrayCoder.of()));
  }

  @Test
  public void testUsingNonDeterministicNonShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    IsmFormat.validateCoderIsCompatible(
        IsmRecordCoder.of(
            1, // number or shard key coders for value records
            0, // number of shard key coders for metadata records
            ImmutableList.<Coder<?>>of(ByteArrayCoder.of(), NON_DETERMINISTIC_CODER),
            ByteArrayCoder.of()));
  }

  @Test
  public void testKeyPrefixCoder() throws Exception {
    KeyPrefix keyPrefixA = KeyPrefix.of(5, 7);
    KeyPrefix keyPrefixB = KeyPrefix.of(5, 7);
    CoderProperties.coderDecodeEncodeEqual(KeyPrefixCoder.of(), keyPrefixA);
    CoderProperties.coderDeterministic(KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    CoderProperties.coderConsistentWithEquals(KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    CoderProperties.coderSerializable(KeyPrefixCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(
        KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    assertTrue(KeyPrefixCoder.of().isRegisterByteSizeObserverCheap(keyPrefixA));
    assertEquals(2, KeyPrefixCoder.of().getEncodedElementByteSize(keyPrefixA));
  }

  @Test
  public void testFooterCoder() throws Exception {
    Footer footerA = Footer.of(1, 2, 3);
    Footer footerB = Footer.of(1, 2, 3);
    CoderProperties.coderDecodeEncodeEqual(FooterCoder.of(), footerA);
    CoderProperties.coderDeterministic(FooterCoder.of(), footerA, footerB);
    CoderProperties.coderConsistentWithEquals(FooterCoder.of(), footerA, footerB);
    CoderProperties.coderSerializable(FooterCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(FooterCoder.of(), footerA, footerB);
    assertTrue(FooterCoder.of().isRegisterByteSizeObserverCheap(footerA));
    assertEquals(25, FooterCoder.of().getEncodedElementByteSize(footerA));
  }

  @Test
  public void testNormalIsmRecordWithMetadataKeyIsError() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected key components to not contain metadata key");
    IsmRecord.of(ImmutableList.of(IsmFormat.getMetadataKey()), "test");
  }

  @Test
  public void testMetadataIsmRecordWithoutMetadataKeyIsError() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected key components to contain metadata key");
    IsmRecord.meta(ImmutableList.of("non-metadata key"), "test".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testIsmRecordCoder() throws Exception {
    IsmRecord<String> ismRecordA = IsmRecord.of(ImmutableList.of("0"), "1");
    IsmRecord<String> ismRecordB = IsmRecord.of(ImmutableList.of("0"), "1");
    IsmRecord<String> ismMetaRecordA =
        IsmRecord.meta(
            ImmutableList.of(IsmFormat.getMetadataKey()), "2".getBytes(StandardCharsets.UTF_8));
    IsmRecord<String> ismMetaRecordB =
        IsmRecord.meta(
            ImmutableList.of(IsmFormat.getMetadataKey()), "2".getBytes(StandardCharsets.UTF_8));
    IsmRecordCoder<String> coder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.<Coder<?>>of(StringUtf8Coder.of()), StringUtf8Coder.of());
    IsmRecordCoder<String> coderWithMetadata =
        IsmRecordCoder.of(
            1,
            1,
            ImmutableList.<Coder<?>>of(MetadataKeyCoder.of(StringUtf8Coder.of())),
            StringUtf8Coder.of());

    // Non-metadata records against coder without metadata key support
    CoderProperties.coderDecodeEncodeEqual(coder, ismRecordA);
    CoderProperties.coderDeterministic(coder, ismRecordA, ismRecordB);
    CoderProperties.coderConsistentWithEquals(coder, ismRecordA, ismRecordB);
    CoderProperties.coderSerializable(coder);
    CoderProperties.structuralValueConsistentWithEquals(coder, ismRecordA, ismRecordB);

    // Non-metadata records against coder with metadata key support
    CoderProperties.coderDecodeEncodeEqual(coderWithMetadata, ismRecordA);
    CoderProperties.coderDeterministic(coderWithMetadata, ismRecordA, ismRecordB);
    CoderProperties.coderConsistentWithEquals(coderWithMetadata, ismRecordA, ismRecordB);
    CoderProperties.coderSerializable(coderWithMetadata);
    CoderProperties.structuralValueConsistentWithEquals(coderWithMetadata, ismRecordA, ismRecordB);

    // Metadata records
    CoderProperties.coderDecodeEncodeEqual(coderWithMetadata, ismMetaRecordA);
    CoderProperties.coderDeterministic(coderWithMetadata, ismMetaRecordA, ismMetaRecordB);
    CoderProperties.coderConsistentWithEquals(coderWithMetadata, ismMetaRecordA, ismMetaRecordB);
    CoderProperties.coderSerializable(coderWithMetadata);
    CoderProperties.structuralValueConsistentWithEquals(
        coderWithMetadata, ismMetaRecordA, ismMetaRecordB);
  }

  @Test
  public void testIsmRecordCoderHashWithinExpectedRanges() throws Exception {
    IsmRecordCoder<String> coder =
        IsmRecordCoder.of(
            2,
            0,
            ImmutableList.<Coder<?>>of(StringUtf8Coder.of(), StringUtf8Coder.of()),
            StringUtf8Coder.of());
    IsmRecordCoder<String> coderWithMetadata =
        IsmRecordCoder.of(
            2,
            2,
            ImmutableList.<Coder<?>>of(
                MetadataKeyCoder.of(StringUtf8Coder.of()), StringUtf8Coder.of()),
            StringUtf8Coder.of());

    assertTrue(coder.hash(ImmutableList.of("A", "B")) < IsmFormat.SHARD_BITS + 1);
    int hash = coderWithMetadata.hash(ImmutableList.of(IsmFormat.getMetadataKey(), "B"));
    assertTrue(hash > IsmFormat.SHARD_BITS && hash < (IsmFormat.SHARD_BITS + 1) * 2);
  }

  @Test
  public void testIsmRecordCoderWithTooManyKeysIsError() throws Exception {
    IsmRecordCoder<String> coder =
        IsmRecordCoder.of(
            2,
            0,
            ImmutableList.<Coder<?>>of(StringUtf8Coder.of(), StringUtf8Coder.of()),
            StringUtf8Coder.of());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected at most");
    coder.hash(ImmutableList.of("A", "B", "C"));
  }

  @Test
  public void testIsmRecordCoderHashWithoutEnoughKeysIsError() throws Exception {
    IsmRecordCoder<String> coder =
        IsmRecordCoder.of(
            2,
            0,
            ImmutableList.<Coder<?>>of(StringUtf8Coder.of(), StringUtf8Coder.of()),
            StringUtf8Coder.of());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected at least");
    coder.hash(ImmutableList.of("A"));
  }

  @Test
  public void testIsmRecordCoderMetadataHashWithoutEnoughKeysIsError() throws Exception {
    IsmRecordCoder<String> coderWithMetadata =
        IsmRecordCoder.of(
            2,
            2,
            ImmutableList.<Coder<?>>of(
                MetadataKeyCoder.of(StringUtf8Coder.of()), StringUtf8Coder.of()),
            StringUtf8Coder.of());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected at least");
    coderWithMetadata.hash(ImmutableList.of(IsmFormat.getMetadataKey()));
  }

  @Test
  public void testIsmRecordCoderKeyCoderCountMismatch() throws Exception {
    IsmRecord<String> ismRecord = IsmRecord.of(ImmutableList.of("0", "too many"), "1");
    IsmRecordCoder<String> coder =
        IsmRecordCoder.of(
            1, 0, ImmutableList.<Coder<?>>of(StringUtf8Coder.of()), StringUtf8Coder.of());

    expectedException.expect(CoderException.class);
    expectedException.expectMessage("Expected 1 key component(s) but received key");
    coder.encode(ismRecord, new ByteArrayOutputStream());
  }

  @Test
  public void testIsmRecordToStringEqualsAndHashCode() {
    IsmRecord<String> ismRecordA = IsmRecord.of(ImmutableList.of("0"), "1");
    IsmRecord<String> ismRecordB = IsmRecord.of(ImmutableList.of("0"), "1");
    IsmRecord<String> ismRecordC = IsmRecord.of(ImmutableList.of("3"), "4");
    IsmRecord<String> ismRecordAWithMeta =
        IsmRecord.meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), "0"),
            "2".getBytes(StandardCharsets.UTF_8));
    IsmRecord<String> ismRecordBWithMeta =
        IsmRecord.meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), "0"),
            "2".getBytes(StandardCharsets.UTF_8));
    IsmRecord<String> ismRecordCWithMeta =
        IsmRecord.meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), "0"),
            "5".getBytes(StandardCharsets.UTF_8));

    assertEquals(ismRecordA, ismRecordB);
    assertEquals(ismRecordAWithMeta, ismRecordBWithMeta);
    assertNotEquals(ismRecordA, ismRecordAWithMeta);
    assertNotEquals(ismRecordA, ismRecordC);
    assertNotEquals(ismRecordAWithMeta, ismRecordCWithMeta);

    assertEquals(ismRecordA.hashCode(), ismRecordB.hashCode());
    assertEquals(ismRecordAWithMeta.hashCode(), ismRecordBWithMeta.hashCode());
    assertNotEquals(ismRecordA.hashCode(), ismRecordAWithMeta.hashCode());
    assertNotEquals(ismRecordA.hashCode(), ismRecordC.hashCode());
    assertNotEquals(ismRecordAWithMeta.hashCode(), ismRecordCWithMeta.hashCode());

    assertThat(
        ismRecordA.toString(),
        allOf(containsString("keyComponents=[0]"), containsString("value=1")));
    assertThat(
        ismRecordAWithMeta.toString(),
        allOf(containsString("keyComponents=[META, 0]"), containsString("metadata=")));
  }

  @Test
  public void testIsmShardCoder() throws Exception {
    IsmShard shardA = IsmShard.of(1, 2, 3);
    IsmShard shardB = IsmShard.of(1, 2, 3);
    CoderProperties.coderDecodeEncodeEqual(IsmShardCoder.of(), shardA);
    CoderProperties.coderDeterministic(IsmShardCoder.of(), shardA, shardB);
    CoderProperties.coderConsistentWithEquals(IsmShardCoder.of(), shardA, shardB);
    CoderProperties.coderSerializable(IsmShardCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(IsmShardCoder.of(), shardA, shardB);
  }

  @Test
  public void testIsmShardToStringEqualsAndHashCode() {
    IsmShard shardA = IsmShard.of(1, 2, 3);
    IsmShard shardB = IsmShard.of(1, 2, 3);
    IsmShard shardC = IsmShard.of(4, 5, 6);
    assertEquals(shardA, shardB);
    assertNotEquals(shardA, shardC);
    assertEquals(shardA.hashCode(), shardB.hashCode());
    assertNotEquals(shardA.hashCode(), shardC.hashCode());
    assertThat(
        shardA.toString(),
        allOf(
            containsString("id=1"),
            containsString("blockOffset=2"),
            containsString("indexOffset=3")));
  }

  @Test
  public void testUnknownVersion() throws Exception {
    byte[] data = new byte[25];
    data[24] = 5; // unknown version
    ByteArrayInputStream bais = new ByteArrayInputStream(data);

    expectedException.expect(IOException.class);
    expectedException.expectMessage("Unknown version 5");
    FooterCoder.of().decode(bais, Context.OUTER);
  }
}
