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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {}

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Read read = SpannerIO.read();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read().withInstanceId("123").withDatabaseId("aaa").withTimestamp(Timestamp.now());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires configuring query or read operation");
    read.validate(null);
  }

  @Test
  public void emptyColumns() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires a list of columns");
    read.validate(null);
  }

  @Test
  public void validRead() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name", "email");
    read.validate(null);
  }

  @Test
  public void validQuery() throws Exception {
    SpannerIO.Read read =
        SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withQuery("SELECT * FROM users");
    read.validate(null);
  }
}
