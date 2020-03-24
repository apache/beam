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

import com.google.api.client.util.ArrayMap;
import com.google.api.services.healthcare.v1alpha2.model.ListMessagesResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HL7v2MessageIDPages;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** The type HL7v2 message id pages test. */
public class HL7v2MessageIDPagesTest {

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
        .makeHL7v2ListRequest("foo", null, null);
    HL7v2MessageIDPages emptyPages = new HL7v2MessageIDPages(client, "foo");
    // In the case that the store is empty we should return a single empty list.
    assertFalse(emptyPages.iterator().hasNext());
  }

  /** Test Non-empty with beta store list response store. */
  @Test
  public void test_NonEmptyLegacyStoreExpectedIterator() throws IOException {
    ListMessagesResponse page0 =
        new ListMessagesResponse()
            .setMessages(Arrays.asList("foo0", "foo1", "foo2"))
            .setNextPageToken("page1");
    ListMessagesResponse page1 =
        new ListMessagesResponse().setMessages(Arrays.asList("foo3", "foo4", "foo5"));
    Mockito.doReturn(page0).when(client).makeHL7v2ListRequest("foo", null, null);

    Mockito.doReturn(page1).when(client).makeHL7v2ListRequest("foo", null, "page1");

    HL7v2MessageIDPages pages = new HL7v2MessageIDPages(client, "foo");
    assertTrue(pages.iterator().hasNext());
    Iterator<List<String>> pagesIterator = pages.iterator();
    assertEquals(page0.getMessages(), pagesIterator.next());
    assertEquals(page1.getMessages(), pagesIterator.next());
    assertFalse(pagesIterator.hasNext());
  }

  /** Test Non-empty with alpha store list response store. */
  @Test
  public void test_NonEmptyStoreExpectedIterator() throws IOException {
    ArrayList<ArrayMap<String, String>> messageList =
        new ArrayList<>(
            Arrays.asList(
                ArrayMap.of("name", "foo0"),
                ArrayMap.of("name", "foo1"),
                ArrayMap.of("name", "foo2")));
    ListMessagesResponse page0 =
        new ListMessagesResponse().set("hl7V2Messages", messageList).setNextPageToken("page1");
    ListMessagesResponse page1 =
        new ListMessagesResponse().setMessages(Arrays.asList("foo3", "foo4", "foo5"));
    Mockito.doReturn(page0).when(client).makeHL7v2ListRequest("foo", null, null);

    Mockito.doReturn(page1).when(client).makeHL7v2ListRequest("foo", null, "page1");

    HL7v2MessageIDPages pages = new HL7v2MessageIDPages(client, "foo");
    assertTrue(pages.iterator().hasNext());
    Iterator<List<String>> pagesIterator = pages.iterator();
    assertEquals(page0.getMessages(), pagesIterator.next());
    assertEquals(page1.getMessages(), pagesIterator.next());
    assertFalse(pagesIterator.hasNext());
  }
}
