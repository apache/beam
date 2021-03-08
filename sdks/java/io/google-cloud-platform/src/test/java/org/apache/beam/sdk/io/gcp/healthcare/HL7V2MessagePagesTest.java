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
package org.apache.beam.sdk.io.gcp.healthcare;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HL7v2MessagePages;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** The type HL7v2 message id pages test. */
@RunWith(JUnit4.class)
public class HL7V2MessagePagesTest {

  /** The Healthcare API. */
  private transient HttpHealthcareApiClient client;

  @Before
  public void setUp() {
    client = Mockito.mock(HttpHealthcareApiClient.class);
  }

  /** Test empty store. */
  @Test
  public void test_EmptyStoreEmptyIterator() throws IOException {
    Mockito.doReturn(new ListMessagesResponse())
        .when(client)
        .makeSendTimeBoundHL7v2ListRequest("foo", null, null, null, null, null);
    HL7v2MessagePages emptyPages = new HL7v2MessagePages(client, "foo", null, null);
    // In the case that the store is empty we should return a single empty list.
    assertFalse(emptyPages.iterator().hasNext());
  }

  /**
   * Test Non-empty with beta store list response store. This tests backwards compatibility with
   * Stores that return the deprecated messages field in list requests.
   */
  @Test
  public void test_NonEmptyExpectedIterator() throws IOException {
    ListMessagesResponse page0 =
        new ListMessagesResponse()
            .setHl7V2Messages(
                Stream.of("foo0", "foo1", "foo2")
                    .map(HL7v2IOTestUtil::testMessage)
                    .collect(Collectors.toList()))
            .setNextPageToken("page1");
    ListMessagesResponse page1 =
        new ListMessagesResponse()
            .setHl7V2Messages(
                Stream.of("foo3", "foo4", "foo5")
                    .map(HL7v2IOTestUtil::testMessage)
                    .collect(Collectors.toList()));
    Mockito.doReturn(page0)
        .when(client)
        .makeSendTimeBoundHL7v2ListRequest("foo", null, null, null, null, null);

    Mockito.doReturn(page1)
        .when(client)
        .makeSendTimeBoundHL7v2ListRequest("foo", null, null, null, null, "page1");

    HL7v2MessagePages pages = new HL7v2MessagePages(client, "foo", null, null);
    assertTrue(pages.iterator().hasNext());
    Iterator<List<HL7v2Message>> pagesIterator = pages.iterator();
    assertEquals(
        page0.getHl7V2Messages().stream().map(Message::getName).collect(Collectors.toList()),
        pagesIterator.next().stream().map(HL7v2Message::getName).collect(Collectors.toList()));
    assertEquals(
        page1.getHl7V2Messages().stream().map(Message::getName).collect(Collectors.toList()),
        pagesIterator.next().stream().map(HL7v2Message::getName).collect(Collectors.toList()));
    assertFalse(pagesIterator.hasNext());
  }
}
