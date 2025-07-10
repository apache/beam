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
package org.apache.beam.sdk.extensions.ordered;

/** Test event examiner. */
class StringEventExaminer implements EventExaminer<String, StringBuilderState> {

  public static final String LAST_INPUT = "z";
  private final long initialSequence;
  private final int emissionFrequency;

  public StringEventExaminer(long initialSequence, int emissionFrequency) {
    this.initialSequence = initialSequence;
    this.emissionFrequency = emissionFrequency;
  }

  @Override
  public boolean isInitialEvent(long sequenceNumber, String input) {
    return sequenceNumber == initialSequence;
  }

  @Override
  public StringBuilderState createStateOnInitialEvent(String input) {
    return new StringBuilderState(input, emissionFrequency);
  }

  @Override
  public boolean isLastEvent(long sequenceNumber, String input) {
    return input.equals(LAST_INPUT);
  }
}
