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

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Ordered processing handler used for testing.
 *
 * <p>It uses all the defaults of the parent class.
 */
public class StringBufferOrderedProcessingHandler
    extends OrderedProcessingHandler<String, String, StringBuilderState, String> {

  public static class StringBufferOrderedProcessingWithGlobalSequenceHandler
      extends OrderedProcessingGlobalSequenceHandler<String, String, StringBuilderState, String> {

    private final EventExaminer<String, StringBuilderState> eventExaminer;

    public StringBufferOrderedProcessingWithGlobalSequenceHandler(
        int emissionFrequency, long initialSequence) {
      super(String.class, String.class, StringBuilderState.class, String.class);
      this.eventExaminer = new StringEventExaminer(initialSequence, emissionFrequency);
    }

    @Override
    @NonNull
    public EventExaminer<String, StringBuilderState> getEventExaminer() {
      return eventExaminer;
    }
  }

  private final EventExaminer<String, StringBuilderState> eventExaminer;

  public StringBufferOrderedProcessingHandler(int emissionFrequency, long initialSequence) {
    super(String.class, String.class, StringBuilderState.class, String.class);
    this.eventExaminer = new StringEventExaminer(initialSequence, emissionFrequency);
  }

  @Override
  @NonNull
  public EventExaminer<String, StringBuilderState> getEventExaminer() {
    return eventExaminer;
  }
}
