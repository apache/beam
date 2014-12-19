/*
 * Copyright (C) 2014 Google Inc.
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

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ExecutionContext} for use in batch mode.
 */
public class BatchModeExecutionContext extends ExecutionContext {
  private Object key;
  private Map<Object, Map<String, Instant>> timers = new HashMap<>();

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
  public void setTimer(String timer, Instant timestamp) {
    Map<String, Instant> keyTimers = timers.get(getKey());
    if (keyTimers == null) {
      keyTimers = new HashMap<>();
      timers.put(getKey(), keyTimers);
    }
    keyTimers.put(timer, timestamp);
  }

  @Override
  public void deleteTimer(String timer) {
    Map<String, Instant> keyTimers = timers.get(getKey());
    if (keyTimers != null) {
      keyTimers.remove(timer);
    }
  }

  public <E> List<TimerOrElement<E>> getAllTimers() {
    List<TimerOrElement<E>> result = new ArrayList<>();
    for (Map.Entry<Object, Map<String, Instant>> keyTimers : timers.entrySet()) {
      for (Map.Entry<String, Instant> timer : keyTimers.getValue().entrySet()) {
        result.add(TimerOrElement.<E>timer(timer.getKey(), timer.getValue(), keyTimers.getKey()));
      }
    }
    return result;
  }

  /**
   * {@link ExecutionContext.StepContext} used in batch mode.
   */
  class StepContext extends ExecutionContext.StepContext {
    private Map<Object, Map<CodedTupleTag<?>, Object>> state = new HashMap<>();
    private Map<Object, Map<CodedTupleTag<?>, List<Object>>> tagLists = new HashMap<>();

    StepContext(String stepName) {
      super(stepName);
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, T value) {
      Map<CodedTupleTag<?>, Object> perKeyState = state.get(getKey());
      if (perKeyState == null) {
        perKeyState = new HashMap<>();
        state.put(getKey(), perKeyState);
      }
      perKeyState.put(tag, value);
    }

    @Override
    public CodedTupleTagMap lookup(List<? extends CodedTupleTag<?>> tags) {
      Map<CodedTupleTag<?>, Object> perKeyState = state.get(getKey());
      Map<CodedTupleTag<?>, Object> map = new HashMap<>();
      if (perKeyState != null) {
        for (CodedTupleTag<?> tag : tags) {
          map.put(tag, perKeyState.get(tag));
        }
      }
      return CodedTupleTagMap.of(map);
    }

    @Override
    public <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp) {
      Map<CodedTupleTag<?>, List<Object>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists == null) {
        perKeyTagLists = new HashMap<>();
        tagLists.put(getKey(), perKeyTagLists);
      }
      List<Object> tagList = perKeyTagLists.get(tag);
      if (tagList == null) {
        tagList = new ArrayList<>();
        perKeyTagLists.put(tag, tagList);
      }
      tagList.add(value);
    }

    @Override
    public <T> void deleteTagList(CodedTupleTag<T> tag) {
      Map<CodedTupleTag<?>, List<Object>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists != null) {
        perKeyTagLists.remove(tag);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) {
      Map<CodedTupleTag<?>, List<Object>> perKeyTagLists = tagLists.get(getKey());
      if (perKeyTagLists == null || perKeyTagLists.get(tag) == null) {
        return new ArrayList<T>();
      }
      List<T> result = new ArrayList<T>();
      for (Object element : perKeyTagLists.get(tag)) {
        result.add((T) element);
      }
      return result;
    }
  }
}
