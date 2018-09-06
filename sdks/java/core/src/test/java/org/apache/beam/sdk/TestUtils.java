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
package org.apache.beam.sdk;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** Utilities for tests. */
public class TestUtils {
  // Do not instantiate.
  private TestUtils() {}

  public static final String[] NO_LINES_ARRAY = new String[] {};

  public static final List<String> NO_LINES = Arrays.asList(NO_LINES_ARRAY);

  public static final String[] LINES_ARRAY =
      new String[] {
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ",
        "And by opposing end them? To die: to sleep; ",
        "No more; and by a sleep to say we end ",
        "The heart-ache and the thousand natural shocks ",
        "That flesh is heir to, 'tis a consummation ",
        "Devoutly to be wish'd. To die, to sleep; ",
        "To sleep: perchance to dream: ay, there's the rub; ",
        "For in that sleep of death what dreams may come ",
        "When we have shuffled off this mortal coil, ",
        "Must give us pause: there's the respect ",
        "That makes calamity of so long life; ",
        "For who would bear the whips and scorns of time, ",
        "The oppressor's wrong, the proud man's contumely, ",
        "The pangs of despised love, the law's delay, ",
        "The insolence of office and the spurns ",
        "That patient merit of the unworthy takes, ",
        "When he himself might his quietus make ",
        "With a bare bodkin? who would fardels bear, ",
        "To grunt and sweat under a weary life, ",
        "But that the dread of something after death, ",
        "The undiscover'd country from whose bourn ",
        "No traveller returns, puzzles the will ",
        "And makes us rather bear those ills we have ",
        "Than fly to others that we know not of? ",
        "Thus conscience does make cowards of us all; ",
        "And thus the native hue of resolution ",
        "Is sicklied o'er with the pale cast of thought, ",
        "And enterprises of great pith and moment ",
        "With this regard their currents turn awry, ",
        "And lose the name of action.--Soft you now! ",
        "The fair Ophelia! Nymph, in thy orisons ",
        "Be all my sins remember'd."
      };

  public static final List<String> LINES = Arrays.asList(LINES_ARRAY);

  public static final String[] LINES2_ARRAY = new String[] {"hi", "there", "bob!"};

  public static final List<String> LINES2 = Arrays.asList(LINES2_ARRAY);

  public static final Integer[] NO_INTS_ARRAY = new Integer[] {};

  public static final List<Integer> NO_INTS = Arrays.asList(NO_INTS_ARRAY);

  public static final Integer[] INTS_ARRAY =
      new Integer[] {3, 42, Integer.MAX_VALUE, 0, -1, Integer.MIN_VALUE, 666};

  public static final List<Integer> INTS = Arrays.asList(INTS_ARRAY);

  /** Matcher for KVs. */
  public static class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
    final Matcher<? super K> keyMatcher;
    final Matcher<? super V> valueMatcher;

    public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
      return new KvMatcher<>(keyMatcher, valueMatcher);
    }

    public KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
      return keyMatcher.matches(kv.getKey()) && valueMatcher.matches(kv.getValue());
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a KV(")
          .appendValue(keyMatcher)
          .appendText(", ")
          .appendValue(valueMatcher)
          .appendText(")");
    }
  }
}
