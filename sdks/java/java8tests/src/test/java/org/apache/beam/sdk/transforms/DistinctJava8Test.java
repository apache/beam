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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 tests for {@link Distinct}.
 */
@RunWith(JUnit4.class)
public class DistinctJava8Test {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void withLambdaRepresentativeValuesFnAndTypeDescriptorShouldApplyFn() {

    Multimap<Integer, String> predupedContents = HashMultimap.create();
    predupedContents.put(3, "foo");
    predupedContents.put(4, "foos");
    predupedContents.put(6, "barbaz");
    predupedContents.put(6, "bazbar");
    PCollection<String> dupes =
        p.apply(Create.of("foo", "foos", "barbaz", "barbaz", "bazbar", "foo"));
    PCollection<String> deduped =
        dupes.apply(Distinct.withRepresentativeValueFn((String s) -> s.length())
                                    .withRepresentativeType(TypeDescriptor.of(Integer.class)));

    PAssert.that(deduped).satisfies((Iterable<String> strs) -> {
      Set<Integer> seenLengths = new HashSet<>();
      for (String s : strs) {
        assertThat(predupedContents.values(), hasItem(s));
        assertThat(seenLengths, not(contains(s.length())));
        seenLengths.add(s.length());
      }
      return null;
    });

    p.run();
  }

  @Test
  public void withLambdaRepresentativeValuesFnNoTypeDescriptorShouldThrow() {

    Multimap<Integer, String> predupedContents = HashMultimap.create();
    predupedContents.put(3, "foo");
    predupedContents.put(4, "foos");
    predupedContents.put(6, "barbaz");
    predupedContents.put(6, "bazbar");
    PCollection<String> dupes =
        p.apply(Create.of("foo", "foos", "barbaz", "barbaz", "bazbar", "foo"));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder for RemoveRepresentativeDupes");

    // Thrown when applying a transform to the internal WithKeys that withRepresentativeValueFn is
    // implemented with
    dupes.apply("RemoveRepresentativeDupes",
        Distinct.withRepresentativeValueFn((String s) -> s.length()));
  }
}
