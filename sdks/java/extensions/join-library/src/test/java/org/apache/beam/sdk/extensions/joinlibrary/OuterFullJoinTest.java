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
package org.apache.beam.sdk.extensions.joinlibrary;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** This test Outer Full Join functionality. */
@SuppressWarnings("initialization.fields.uninitialized")
public class OuterFullJoinTest {
  private List<KV<String, Long>> leftListOfKv;
  private List<KV<String, String>> rightListOfKv;
  private List<KV<String, KV<Long, String>>> expectedResult;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Before
  public void setup() {

    leftListOfKv = new ArrayList<>();
    rightListOfKv = new ArrayList<>();

    expectedResult = new ArrayList<>();
  }

  @Test
  public void testJoinOneToOneMapping() {
    leftListOfKv.add(KV.of("Key1", 5L));
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key1", "foo"));
    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.fullOuterJoin(leftCollection, rightCollection, -1L, "");

    expectedResult.add(KV.of("Key1", KV.of(5L, "foo")));
    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinOneToManyMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    rightListOfKv.add(KV.of("Key2", "gazonk"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.fullOuterJoin(leftCollection, rightCollection, -1L, "");

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    expectedResult.add(KV.of("Key2", KV.of(4L, "gazonk")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinManyToOneMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    leftListOfKv.add(KV.of("Key2", 6L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.fullOuterJoin(leftCollection, rightCollection, -1L, "");

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));
    expectedResult.add(KV.of("Key2", KV.of(6L, "bar")));
    PAssert.that(output).containsInAnyOrder(expectedResult);

    p.run();
  }

  @Test
  public void testJoinNoneToNoneMapping() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key3", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    PCollection<KV<String, KV<Long, String>>> output =
        Join.fullOuterJoin(leftCollection, rightCollection, -1L, "");

    expectedResult.add(KV.of("Key2", KV.of(4L, "")));
    expectedResult.add(KV.of("Key3", KV.of(-1L, "bar")));
    PAssert.that(output).containsInAnyOrder(expectedResult);
    p.run();
  }

  @Test
  public void testMultipleJoinsInSamePipeline() {
    leftListOfKv.add(KV.of("Key2", 4L));
    PCollection<KV<String, Long>> leftCollection = p.apply("CreateLeft", Create.of(leftListOfKv));

    rightListOfKv.add(KV.of("Key2", "bar"));
    PCollection<KV<String, String>> rightCollection =
        p.apply("CreateRight", Create.of(rightListOfKv));

    expectedResult.add(KV.of("Key2", KV.of(4L, "bar")));

    PCollection<KV<String, KV<Long, String>>> output1 =
        Join.fullOuterJoin("Join1", leftCollection, rightCollection, -1L, "");
    PCollection<KV<String, KV<Long, String>>> output2 =
        Join.fullOuterJoin("Join2", leftCollection, rightCollection, -1L, "");
    PAssert.that(output1).containsInAnyOrder(expectedResult);
    PAssert.that(output2).containsInAnyOrder(expectedResult);

    p.run();
  }

  @SuppressWarnings("nullness")
  @Test(expected = NullPointerException.class)
  public void testJoinLeftCollectionNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.fullOuterJoin(
        null,
        p.apply(
            Create.of(rightListOfKv)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))),
        "",
        "");
  }

  @SuppressWarnings("nullness")
  @Test(expected = NullPointerException.class)
  public void testJoinRightCollectionNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.fullOuterJoin(
        p.apply(
            Create.of(leftListOfKv).withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))),
        null,
        -1L,
        -1L);
  }

  @Test(expected = NullPointerException.class)
  public void testJoinLeftNullValueIsNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.fullOuterJoin(
        p.apply("CreateLeft", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))),
        p.apply(
            "CreateRight", Create.empty(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))),
        null,
        "");
  }

  @Test(expected = NullPointerException.class)
  public void testJoinRightNullValueIsNull() {
    p.enableAbandonedNodeEnforcement(false);
    Join.fullOuterJoin(
        p.apply("CreateLeft", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))),
        p.apply(
            "CreateRight", Create.empty(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))),
        -1L,
        null);
  }
}
