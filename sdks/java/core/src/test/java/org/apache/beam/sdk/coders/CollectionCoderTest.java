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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link CollectionCoder}.
 */
@RunWith(JUnit4.class)
public class CollectionCoderTest {

  private static final Coder<Collection<Integer>> TEST_CODER = CollectionCoder.of(VarIntCoder.of());

  private static final List<Collection<Integer>> TEST_VALUES = Arrays.<Collection<Integer>>asList(
      Collections.<Integer>emptyList(),
      Collections.<Integer>emptySet(),
      Collections.singletonList(13),
      Arrays.asList(1, 2, 3, 4),
      new LinkedList<>(Arrays.asList(7, 6, 5)),
      new TreeSet<>(Arrays.asList(31, -5, 83)));

  @Test
  public void testDecodeEncodeContentsEqual() throws Exception {
    for (Collection<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsEqual(TEST_CODER, value);
    }
  }

  // If this becomes nonempty, it implies the binary format has changed.
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AAAAAA",
      "AAAAAA",
      "AAAAAQ0",
      "AAAABAECAwQ",
      "AAAAAwcGBQ",
      "AAAAA_v___8PH1M");

  @Test
  public void testWireFormat() throws Exception {
    CoderProperties.coderDecodesBase64ContentsEqual(TEST_CODER, TEST_ENCODINGS, TEST_VALUES);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Collection");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
