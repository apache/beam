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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.CompositeProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.GenericTranslatorProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests of {@link BroadcastHashJoinTranslator}. */
@RunWith(JUnit4.class)
public class BroadcastHashJoinTranslatorTest {

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void twoUsesOneViewTest() {

    BroadcastHashJoinTranslator<?, ?, ?, ?> translatorUnderTest =
        new BroadcastHashJoinTranslator<>();

    EuphoriaOptions options = p.getOptions().as(EuphoriaOptions.class);
    // Every join in this test will be translated as Broadcast
    options.setTranslatorProvider(
        CompositeProvider.of(
            GenericTranslatorProvider.newBuilder()
                .register(Join.class, (op) -> true, translatorUnderTest)
                .build(),
            GenericTranslatorProvider.createWithDefaultTranslators()));

    // create input to be broadcast
    PCollection<KV<Integer, String>> lengthStrings =
        p.apply("names", Create.of(KV.of(1, "one"), KV.of(2, "two"), KV.of(3, "three")))
            .setTypeDescriptor(
                TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()));

    UnaryFunction<KV<Integer, String>, Integer> sharedKeyExtractor = KV::getKey;

    // other datasets to be joined with
    PCollection<String> letters =
        p.apply("letters", Create.of("a", "b", "c", "d"))
            .setTypeDescriptor(TypeDescriptors.strings());
    PCollection<String> acronyms =
        p.apply("acronyms", Create.of("B2K", "DIY", "FKA", "EOBD"))
            .setTypeDescriptor(TypeDescriptors.strings());

    PCollection<KV<Integer, String>> lettersJoined =
        LeftJoin.named("join-letters-with-lengths")
            .of(letters, lengthStrings)
            .by(String::length, sharedKeyExtractor, TypeDescriptors.integers())
            .using(
                (letter, maybeLength, ctx) ->
                    ctx.collect(letter + "-" + maybeLength.orElse(KV.of(-1, "null")).getValue()),
                TypeDescriptors.strings())
            .output();

    PCollection<KV<Integer, String>> acronymsJoined =
        RightJoin.named("join-acronyms-with-lengths")
            .of(lengthStrings, acronyms)
            .by(sharedKeyExtractor, String::length, TypeDescriptors.integers())
            .using(
                (maybeLength, acronym, ctx) ->
                    ctx.collect(maybeLength.orElse(KV.of(-1, "null")).getValue() + "-" + acronym),
                TypeDescriptors.strings())
            .output();

    PAssert.that(lettersJoined)
        .containsInAnyOrder(
            KV.of(1, "a-one"), KV.of(1, "b-one"), KV.of(1, "c-one"), KV.of(1, "d-one"));

    PAssert.that(acronymsJoined)
        .containsInAnyOrder(
            KV.of(3, "three-B2K"),
            KV.of(3, "three-DIY"),
            KV.of(3, "three-FKA"),
            KV.of(4, "null-EOBD"));

    p.run();
    Assert.assertEquals(1, translatorUnderTest.pViews.size());
  }
}
