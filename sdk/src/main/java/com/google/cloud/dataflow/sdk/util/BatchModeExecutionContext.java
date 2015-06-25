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

import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ExecutionContext} for use in batch mode.
 */
public class BatchModeExecutionContext extends ExecutionContext {
  private Object key;

  /**
   * Creates a {@code BatchModeExecutionContext}.
   */
  public BatchModeExecutionContext() { }

  /**
   * Create a new {@link ExecutionContext.StepContext}.
   */
  @Override
  public ExecutionContext.StepContext createStepContext(String stepName) {
    return new StepContext(stepName);
  }

  /**
   * Sets the key of the work currently being processed.
   */
  public void setKey(Object key) {
    this.key = key;
  }

  /**
   * Returns the key of the work currently being processed.
   *
   * <p> If there is not a currently defined key, returns null.
   */
  public Object getKey() {
    return key;
  }

  @Override
  public TimerManager getTimerManager() {
    return null;
  }

  /**
   * {@link ExecutionContext.StepContext} used in batch mode.
   */
  class StepContext extends ExecutionContext.StepContext {
    private Map<Object, Map<CodedTupleTag<?>, Object>> state = new HashMap<>();
    private Map<Object, Map<CodedTupleTag<?>, List<?>>> tagLists =
        new HashMap<>();

    StepContext(String stepName) {
      super(stepName);
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp) {
      // We never read the timestamp, and batch doesn't need it. So don't store it.
      Map<CodedTupleTag<?>, Object> perKeyState = state.get(getKey());
      if (perKeyState == null) {
        perKeyState = new HashMap<>();
        state.put(getKey(), perKeyState);
      }
      perKeyState.put(tag, value);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag) {
      Map<CodedTupleTag<?>, Object> perKeyState = state.get(getKey());
      if (perKeyState != null) {
        perKeyState.remove(tag);
      }
    }

    @Override
    public CodedTupleTagMap lookup(Iterable<? extends CodedTupleTag<?>> tags) {
      Map<CodedTupleTag<?>, Object> perKeyState = state.get(getKey());
      if (perKeyState == null) {
        return CodedTupleTagMap.empty();
      }

      Map<CodedTupleTag<?>, Object> map = new HashMap<>();
      for (CodedTupleTag<?> tag : tags) {
        map.put(tag, perKeyState.get(tag));
      }
      return CodedTupleTagMap.of(map);
    }

    @Override
    public <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp) {
      // We never read the timestamp, and batch doesn't need it. So don't store it.
      Map<CodedTupleTag<?>, List<?>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists == null) {
        perKeyTagLists = new HashMap<>();
        tagLists.put(getKey(), perKeyTagLists);
      }
      @SuppressWarnings("unchecked")
      List<T> tagList = (List<T>) perKeyTagLists.get(tag);
      if (tagList == null) {
        tagList = new ArrayList<>();
        perKeyTagLists.put(tag, tagList);
      }

      tagList.add(value);
    }

    @Override
    public <T> void deleteTagList(CodedTupleTag<T> tag) {
      Map<CodedTupleTag<?>, List<?>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists != null) {
        perKeyTagLists.remove(tag);
      }
    }

    @Override
    public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) {
      Map<CodedTupleTag<?>, List<?>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists == null) {
        return Collections.emptyList();
      }

      @SuppressWarnings("unchecked")
      List<T> list = (List<T>) perKeyTagLists.get(tag);
      if (list == null) {
        return Collections.emptyList();
      }
      return list;
    }

    @Override
    public <T> Map<CodedTupleTag<T>, Iterable<T>> readTagLists(Iterable<CodedTupleTag<T>> tags)
        throws IOException {
      return FluentIterable.from(tags)
          .toMap(new Function<CodedTupleTag<T>, Iterable<T>>() {
            @Override
            public Iterable<T> apply(CodedTupleTag<T> input) {
              return readTagList(input);
            }
          });
    }
  }
}
