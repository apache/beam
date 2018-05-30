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
package org.apache.beam.sdk.io.solr;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.solr.common.SolrDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link JavaBinCodecCoder}. */
@RunWith(JUnit4.class)
public class JavaBinCodecCoderTest {
  private static final Coder<SolrDocument> TEST_CODER = JavaBinCodecCoder.of(SolrDocument.class);
  private static final List<SolrDocument> TEST_VALUES = new ArrayList<>();

  static {
    SolrDocument doc = new SolrDocument();
    doc.put("id", "1");
    doc.put("content", "wheel on the bus");
    doc.put("_version_", 1573597324260671488L);
    TEST_VALUES.add(doc);

    doc = new SolrDocument();
    doc.put("id", "2");
    doc.put("content", "goes round and round");
    doc.put("_version_", 1573597324260671489L);
    TEST_VALUES.add(doc);
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (SolrDocument value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsInSameOrder(TEST_CODER, value);
      CoderProperties.structuralValueDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null SolrDocument");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() {
    assertThat(
        TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(SolrDocument.class)));
  }
}
