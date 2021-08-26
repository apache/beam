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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link ParseResult}. */
@RunWith(JUnit4.class)
public class ParseResultTest {
  @Test
  public void testEqualsAndHashCode() {
    ParseResult successBase = ParseResult.success("a.txt", "hello", getMetadata());
    ParseResult successSame = ParseResult.success("a.txt", "hello", getMetadata());
    ParseResult successDifferentName = ParseResult.success("b.txt", "hello", getMetadata());
    ParseResult successDifferentContent = ParseResult.success("a.txt", "goodbye", getMetadata());
    ParseResult successDifferentMetadata = ParseResult.success("a.txt", "hello", new Metadata());

    RuntimeException oops = new RuntimeException("oops");
    ParseResult failureBase = ParseResult.failure("a.txt", "", new Metadata(), oops);
    ParseResult failureSame = ParseResult.failure("a.txt", "", new Metadata(), oops);
    ParseResult failureDifferentName = ParseResult.failure("b.txt", "", new Metadata(), oops);
    ParseResult failureDifferentContent =
        ParseResult.failure("b.txt", "partial", new Metadata(), oops);
    ParseResult failureDifferentMetadata = ParseResult.failure("b.txt", "", getMetadata(), oops);
    ParseResult failureDifferentError =
        ParseResult.failure("a.txt", "", new Metadata(), new RuntimeException("eek"));

    assertEquals(successBase, successSame);
    assertEquals(successBase.hashCode(), successSame.hashCode());

    assertThat(successDifferentName, not(equalTo(successBase)));
    assertThat(successDifferentContent, not(equalTo(successBase)));
    assertThat(successDifferentMetadata, not(equalTo(successBase)));

    assertThat(successDifferentName.hashCode(), not(equalTo(successBase.hashCode())));
    assertThat(successDifferentContent.hashCode(), not(equalTo(successBase.hashCode())));
    assertThat(successDifferentMetadata.hashCode(), not(equalTo(successBase.hashCode())));

    assertThat(failureBase, not(equalTo(successBase)));
    assertThat(successBase, not(equalTo(failureBase)));

    assertEquals(failureBase, failureSame);
    assertEquals(failureBase.hashCode(), failureSame.hashCode());

    assertThat(failureDifferentName, not(equalTo(failureBase)));
    assertThat(failureDifferentError, not(equalTo(failureBase)));
    assertThat(failureDifferentContent, not(equalTo(failureBase)));
    assertThat(failureDifferentMetadata, not(equalTo(failureBase)));

    assertThat(failureDifferentName.hashCode(), not(equalTo(failureBase.hashCode())));
    assertThat(failureDifferentError.hashCode(), not(equalTo(failureBase.hashCode())));
    assertThat(failureDifferentContent.hashCode(), not(equalTo(failureBase.hashCode())));
    assertThat(failureDifferentMetadata.hashCode(), not(equalTo(failureBase.hashCode())));
  }

  static Metadata getMetadata() {
    Metadata m = new Metadata();
    m.add("Author", "BeamTikaUser");
    m.add("Author", "BeamTikaUser2");
    m.add("Date", "2017-09-01");
    return m;
  }
}
