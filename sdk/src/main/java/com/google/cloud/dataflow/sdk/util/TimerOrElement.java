/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.KeyedWorkItems.FakeKeyedWorkItemCoder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Empty class which exists because the back end will sometimes insert uses of
 * {@code com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder} and we'd like to be
 * able to rename/move that without breaking things.
 */
public class TimerOrElement {

  // TimerOrElement should never be created.
  private TimerOrElement() {}

  /**
   * Empty class which exists because the back end will sometimes insert uses of
   * {@code com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder} and we'd like to
   * be able to rename/move that without breaking things.
   */
  public static class TimerOrElementCoder<ElemT> extends FakeKeyedWorkItemCoder<Object, ElemT> {

    private TimerOrElementCoder(Coder<ElemT> elemCoder) {
      super(elemCoder);
    }

    public static <T> TimerOrElementCoder<T> of(Coder<T> elemCoder) {
      return new TimerOrElementCoder<>(elemCoder);
    }

    @JsonCreator
    public static TimerOrElementCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Object> components) {
      return of((Coder<?>) components.get(0));
    }
  }
}
