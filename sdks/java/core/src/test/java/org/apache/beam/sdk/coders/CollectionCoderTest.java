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
package org.apache.beam.sdk.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link CollectionCoder}. */
@RunWith(JUnit4.class)
public class CollectionCoderTest {

  private static final Coder<Collection<Integer>> TEST_CODER = CollectionCoder.of(VarIntCoder.of());

  private static final List<Collection<Integer>> TEST_VALUES =
      Arrays.asList(
          Collections.emptyList(),
          Collections.emptySet(),
          Collections.singletonList(13),
          Arrays.asList(1, 2, 3, 4),
          new ArrayList<>(Arrays.asList(7, 6, 5)),
          new TreeSet<>(Arrays.asList(31, -5, 83)));

  @Test
  public void testDecodeEncodeContentsEqual() throws Exception {
    for (Collection<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList("AAAAAA", "AAAAAA", "AAAAAQ0", "AAAABAECAwQ", "AAAAAwcGBQ", "AAAAA_v___8PH1M");

  @Test
  public void testWireFormat() throws Exception {
    CoderProperties.coderDecodesBase64ContentsEqual(TEST_CODER, TEST_ENCODINGS, TEST_VALUES);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Collection");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(CollectionCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Collection<Integer>> expectedTypeDescriptor =
        new TypeDescriptor<Collection<Integer>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(expectedTypeDescriptor));
  }

  @Test
  public void testSingleElementAndHeaderHelpersMatchWireFormat() throws Exception {
    CollectionCoder<Integer> coder = CollectionCoder.of(VarIntCoder.of());
    ByteArrayOutputStream helperOut = new ByteArrayOutputStream();
    IterableLikeCoder.encodeSingleElement(VarIntCoder.of(), 13, helperOut);
    ByteArrayOutputStream fullOut = new ByteArrayOutputStream();
    coder.encode(Collections.singletonList(13), fullOut);
    assertThat(helperOut.toByteArray(), equalTo(fullOut.toByteArray()));

    ByteArrayInputStream singleIn = new ByteArrayInputStream(fullOut.toByteArray());
    assertThat(IterableLikeCoder.readSizeHeader(singleIn), equalTo(1));
    assertThat(VarIntCoder.of().decode(singleIn), equalTo(13));

    ByteArrayOutputStream multiOut = new ByteArrayOutputStream();
    coder.encode(Arrays.asList(1, 2, 3, 4), multiOut);
    ByteArrayInputStream multiIn = new ByteArrayInputStream(multiOut.toByteArray());
    int size = IterableLikeCoder.readSizeHeader(multiIn);
    assertThat(size, equalTo(4));
    assertThat(coder.decodeAfterSizeHeader(size, multiIn), equalTo(Arrays.asList(1, 2, 3, 4)));

    long[] helperSize = new long[1];
    long[] fullSize = new long[1];
    ElementByteSizeObserver helperObs =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {
            helperSize[0] += elementByteSize;
          }
        };
    ElementByteSizeObserver fullObs =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {
            fullSize[0] += elementByteSize;
          }
        };
    IterableLikeCoder.registerSingleElementByteSize(VarIntCoder.of(), 13, helperObs);
    helperObs.advance();
    coder.registerByteSizeObserver(Collections.singletonList(13), fullObs);
    fullObs.advance();
    assertThat(helperSize[0], equalTo(fullSize[0]));
  }
}
