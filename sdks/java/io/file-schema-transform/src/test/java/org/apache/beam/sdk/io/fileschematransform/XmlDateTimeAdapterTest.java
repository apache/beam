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
package org.apache.beam.sdk.io.fileschematransform;

import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link XmlDateTimeAdapter}. */
@RunWith(JUnit4.class)
public class XmlDateTimeAdapterTest {

  @Test
  public void unmarshal() throws Exception {
    XmlDateTimeAdapter adapter = new XmlDateTimeAdapter();
    String dateTimeInput = "2022-12-29T21:04:51.171Z";
    assertEquals(
        Instant.ofEpochMilli(1672347891171L).toDateTime(), adapter.unmarshal(dateTimeInput));
  }

  @Test
  public void marshal() throws Exception {
    XmlDateTimeAdapter adapter = new XmlDateTimeAdapter();
    DateTime dateTime = Instant.ofEpochMilli(1672347891171L).toDateTime(DateTimeZone.UTC);
    assertEquals("2022-12-29T21:04:51.171Z", adapter.marshal(dateTime));
  }
}
