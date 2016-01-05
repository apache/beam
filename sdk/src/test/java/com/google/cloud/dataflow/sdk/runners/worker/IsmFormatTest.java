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
package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.Footer;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.FooterCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefix;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefixCoder;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Tests for {@link IsmFormat}.
 */
@RunWith(JUnit4.class)
public class IsmFormatTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testKeyPrefixCoder() throws Exception {
    KeyPrefix keyPrefixA = new KeyPrefix(5, 7);
    KeyPrefix keyPrefixB = new KeyPrefix(5, 7);
    CoderProperties.coderDecodeEncodeEqual(KeyPrefixCoder.of(), keyPrefixA);
    CoderProperties.coderDeterministic(KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    CoderProperties.coderConsistentWithEquals(KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    CoderProperties.coderSerializable(KeyPrefixCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(
        KeyPrefixCoder.of(), keyPrefixA, keyPrefixB);
    assertTrue(KeyPrefixCoder.of().isRegisterByteSizeObserverCheap(keyPrefixA, Context.NESTED));
    assertTrue(KeyPrefixCoder.of().isRegisterByteSizeObserverCheap(keyPrefixA, Context.OUTER));
    assertEquals(2, KeyPrefixCoder.of().getEncodedElementByteSize(keyPrefixA, Context.NESTED));
    assertEquals(2, KeyPrefixCoder.of().getEncodedElementByteSize(keyPrefixA, Context.OUTER));
  }

  @Test
  public void testFooterCoder() throws Exception {
    Footer footerA = new Footer(1, 2, 3);
    Footer footerB = new Footer(1, 2, 3);
    CoderProperties.coderDecodeEncodeEqual(FooterCoder.of(), footerA);
    CoderProperties.coderDeterministic(FooterCoder.of(), footerA, footerB);
    CoderProperties.coderConsistentWithEquals(FooterCoder.of(), footerA, footerB);
    CoderProperties.coderSerializable(FooterCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(FooterCoder.of(), footerA, footerB);
    assertTrue(FooterCoder.of().isRegisterByteSizeObserverCheap(footerA, Context.NESTED));
    assertTrue(FooterCoder.of().isRegisterByteSizeObserverCheap(footerA, Context.OUTER));
    assertEquals(25, FooterCoder.of().getEncodedElementByteSize(footerA, Context.NESTED));
    assertEquals(25, FooterCoder.of().getEncodedElementByteSize(footerA, Context.OUTER));
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

