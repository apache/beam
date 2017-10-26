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
package org.apache.beam.sdk.io.tika;

import static org.junit.Assert.assertEquals;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;

/**
 * Tests ParseResult.
 */
public class ParseResultTest {
  @Test
  public void testEqualsAndHashCode() {
    ParseResult p1 = new ParseResult("a.txt", "hello", getMetadata());
    ParseResult p2 = new ParseResult("a.txt", "hello", getMetadata());
    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
  }

  static Metadata getMetadata() {
    Metadata m = new Metadata();
    m.add("Author", "BeamTikaUser");
    m.add("Author", "BeamTikaUser2");
    m.add("Date", "2017-09-01");
    return m;
  }
}
