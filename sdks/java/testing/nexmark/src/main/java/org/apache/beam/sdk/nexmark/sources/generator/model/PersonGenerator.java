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
package org.apache.beam.sdk.nexmark.sources.generator.model;

import static org.apache.beam.sdk.nexmark.sources.generator.model.LongGenerator.nextLong;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextString;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.joda.time.DateTime;

/** Generates people. */
public class PersonGenerator {
  /** Number of yet-to-be-created people and auction ids allowed. */
  private static final int PERSON_ID_LEAD = 10;

  /**
   * Keep the number of states small so that the example queries will find results even with a small
   * batch of events.
   */
  private static final List<String> US_STATES = Arrays.asList("AZ,CA,ID,OR,WA,WY".split(","));

  private static final List<String> US_CITIES =
      Arrays.asList(
          "Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne"
              .split(","));

  private static final List<String> FIRST_NAMES =
      Arrays.asList("Peter,Paul,Luke,John,Saul,Vicky,Kate,Julie,Sarah,Deiter,Walter".split(","));

  private static final List<String> LAST_NAMES =
      Arrays.asList("Shultz,Abrams,Spencer,White,Bartels,Walton,Smith,Jones,Noris".split(","));

  /** Generate and return a random person with next available id. */
  public static Person nextPerson(
      long nextEventId, Random random, DateTime timestamp, GeneratorConfig config) {

    long id = lastBase0PersonId(nextEventId) + GeneratorConfig.FIRST_PERSON_ID;
    String name = nextPersonName(random);
    String email = nextEmail(random);
    String creditCard = nextCreditCard(random);
    String city = nextUSCity(random);
    String state = nextUSState(random);
    int currentSize =
        8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
    String extra = nextExtra(random, currentSize, config.getAvgPersonByteSize());
    return new Person(id, name, email, creditCard, city, state, timestamp.toInstant(), extra);
  }

  /** Return a random person id (base 0). */
  public static long nextBase0PersonId(long eventId, Random random, GeneratorConfig config) {
    // Choose a random person from any of the 'active' people, plus a few 'leads'.
    // By limiting to 'active' we ensure the density of bids or auctions per person
    // does not decrease over time for long running jobs.
    // By choosing a person id ahead of the last valid person id we will make
    // newPerson and newAuction events appear to have been swapped in time.
    long numPeople = lastBase0PersonId(eventId) + 1;
    long activePeople = Math.min(numPeople, config.getNumActivePeople());
    long n = nextLong(random, activePeople + PERSON_ID_LEAD);
    return numPeople - activePeople + n;
  }

  /**
   * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
   * due to generate a person.
   */
  public static long lastBase0PersonId(long eventId) {
    long epoch = eventId / GeneratorConfig.PROPORTION_DENOMINATOR;
    long offset = eventId % GeneratorConfig.PROPORTION_DENOMINATOR;
    if (offset >= GeneratorConfig.PERSON_PROPORTION) {
      // About to generate an auction or bid.
      // Go back to the last person generated in this epoch.
      offset = GeneratorConfig.PERSON_PROPORTION - 1;
    }
    // About to generate a person.
    return epoch * GeneratorConfig.PERSON_PROPORTION + offset;
  }

  /** return a random US state. */
  private static String nextUSState(Random random) {
    return US_STATES.get(random.nextInt(US_STATES.size()));
  }

  /** Return a random US city. */
  private static String nextUSCity(Random random) {
    return US_CITIES.get(random.nextInt(US_CITIES.size()));
  }

  /** Return a random person name. */
  private static String nextPersonName(Random random) {
    return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size()))
        + " "
        + LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
  }

  /** Return a random email address. */
  private static String nextEmail(Random random) {
    return nextString(random, 7) + "@" + nextString(random, 5) + ".com";
  }

  /** Return a random credit card number. */
  private static String nextCreditCard(Random random) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 4; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(String.format("%04d", random.nextInt(10000)));
    }
    return sb.toString();
  }
}
