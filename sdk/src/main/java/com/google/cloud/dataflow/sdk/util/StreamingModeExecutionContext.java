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
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link ExecutionContext} for use in streaming mode.
 */
public class StreamingModeExecutionContext extends ExecutionContext {
  private String computation;
  private Windmill.WorkItem work;
  private StateFetcher stateFetcher;
  private Windmill.WorkItemCommitRequest.Builder outputBuilder;
  private Map<TupleTag<?>, Map<BoundedWindow, Object>> sideInputCache;

  public StreamingModeExecutionContext(String computation, StateFetcher stateFetcher) {
    this.computation = computation;
    this.stateFetcher = stateFetcher;
  }

  public void start(Windmill.WorkItem work, Windmill.WorkItemCommitRequest.Builder outputBuilder) {
    this.work = work;
    this.outputBuilder = outputBuilder;
    this.sideInputCache = new HashMap<>();
  }

  @Override
  public ExecutionContext.StepContext createStepContext(String stepName) {
    return new StepContext(stepName);
  }

  @Override
  public void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain) {
    long timestampMicros = TimeUnit.MILLISECONDS.toMicros(timestamp.getMillis());
    outputBuilder.addOutputTimers(
        Windmill.Timer.newBuilder()
        .setTimestamp(timestampMicros)
        .setTag(ByteString.copyFromUtf8(timer))
        .setType(timerType(domain))
        .build());
  }

  @Override
  public void deleteTimer(String timer, Trigger.TimeDomain domain) {
    outputBuilder.addOutputTimers(
        Windmill.Timer.newBuilder()
        .setTag(ByteString.copyFromUtf8(timer))
        .setType(timerType(domain))
        .build());
  }

  private Windmill.Timer.Type timerType(Trigger.TimeDomain domain) {
    return domain == Trigger.TimeDomain.EVENT_TIME
        ? Windmill.Timer.Type.WATERMARK
        : Windmill.Timer.Type.REALTIME;
  }

  @Override
  public <T> T getSideInput(
      PCollectionView<T> view, BoundedWindow mainInputWindow, PTuple sideInputs) {
    if (!sideInputs.has(view.getTagInternal())) {
      throw new IllegalArgumentException(
          "calling sideInput() with unknown view; " +
          "did you forget to pass the view in " +
          "ParDo.withSideInputs()?");
    }

    return fetchSideInput(view, mainInputWindow, SideInputState.CACHED_IN_WORKITEM);
  }

  /**
   * Fetch the given side input asynchronously and return true if it is present.
   */
  public boolean issueSideInputFetch(
      PCollectionView<?> view, BoundedWindow mainInputWindow, SideInputState state) {
    return fetchSideInput(view, mainInputWindow, state) != null;
  }

  /**
   * Fetches the requested sideInput, and maintains a view of the cache that doesn't remove
   * items until the active work item is finished.
   */
  private <T> T fetchSideInput(
      PCollectionView<T> view, BoundedWindow mainInputWindow, SideInputState state) {
    BoundedWindow sideInputWindow =
        view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(mainInputWindow);

    Map<BoundedWindow, Object> tagCache = sideInputCache.get(view.getTagInternal());
    if (tagCache == null) {
      tagCache = new HashMap<>();
      sideInputCache.put(view.getTagInternal(), tagCache);
    }

    @SuppressWarnings("unchecked")
    T sideInput = (T) tagCache.get(sideInputWindow);
    if (sideInput == null) {
      if (state == SideInputState.CACHED_IN_WORKITEM) {
        throw new IllegalStateException(
            "Expected side input to be cached. Tag: "
            + view.getTagInternal().getId());
      }
      T typed = (T) stateFetcher.fetchSideInput(view, sideInputWindow, state);
      sideInput = typed;
      if (sideInput != null) {
        tagCache.put(sideInputWindow, sideInput);
        return sideInput;
      } else {
        return null;
      }
    } else {
      return sideInput;
    }
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
      W window, Coder<W> windowCoder) throws IOException {
    if (getSerializedKey().size() != 0) {
      throw new IllegalStateException("writePCollectionViewData must follow a Combine.globally");
    }

    ByteString.Output dataStream = ByteString.newOutput();
    dataCoder.encode(data, dataStream, Coder.Context.OUTER);

    ByteString.Output windowStream = ByteString.newOutput();
    windowCoder.encode(window, windowStream, Coder.Context.OUTER);

    outputBuilder.addGlobalDataUpdates(
        Windmill.GlobalData.newBuilder()
        .setDataId(
            Windmill.GlobalDataId.newBuilder()
            .setTag(tag.getId())
            .setVersion(windowStream.toByteString())
            .build())
        .setData(dataStream.toByteString())
        .build());
  }

  public Iterable<Windmill.GlobalDataId> getSideInputNotifications() {
    return work.getGlobalDataIdNotificationsList();
  }

  /**
   * Note that there is data on the current key that is blocked on the given side input.
   */
  public void addBlockingSideInput(Windmill.GlobalDataRequest sideInput) {
    outputBuilder.addGlobalDataRequests(sideInput);
    outputBuilder.addGlobalDataIdRequests(sideInput.getDataId());
  }

  /**
   * Note that there is data on the current key that is blocked on the given side inputs.
   */
  public void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> sideInputs) {
    for (Windmill.GlobalDataRequest sideInput : sideInputs) {
      addBlockingSideInput(sideInput);
    }
  }

  public ByteString getSerializedKey() {
    return work.getKey();
  }

  public long getWorkToken() {
    return work.getWorkToken();
  }

  public Windmill.WorkItem getWork() {
    return work;
  }

  public Windmill.WorkItemCommitRequest.Builder getOutputBuilder() {
    return outputBuilder;
  }

  public void flushState() {
    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      try {
        ((StepContext) stepContext).flushState();
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush state");
      }
    }
  }

  private class TagLoader extends CacheLoader<CodedTupleTag<?>, Optional<?>> {

    private final String mangledPrefix;

    private TagLoader(String mangledPrefix) {
      this.mangledPrefix = mangledPrefix;
    }

    @Override
    public Optional<?> load(CodedTupleTag<?> key) throws Exception {
      return loadAll(Arrays.asList(key)).get(key);
    }

    @Override
    public Map<CodedTupleTag<?>, Optional<?>> loadAll(
        Iterable<? extends CodedTupleTag<?>> keys) throws Exception {
      return  stateFetcher.fetch(
          computation, getSerializedKey(), getWorkToken(), mangledPrefix, keys);
    }
  }

  private class TagListLoader extends CacheLoader<CodedTupleTag<?>, List<?>> {

    private final String mangledPrefix;

    private TagListLoader(String mangledPrefix) {
      this.mangledPrefix = mangledPrefix;
    }

    @Override
    public List<?> load(CodedTupleTag<?> key) throws Exception {
      return loadAll(Arrays.asList(key)).get(key);
    }

    @Override
    public Map<CodedTupleTag<?>, List<?>> loadAll(
        Iterable<? extends CodedTupleTag<?>> keys) throws Exception {
      return stateFetcher.fetchList(
          computation, getSerializedKey(), getWorkToken(), mangledPrefix, keys);
    }
  }

  class StepContext extends ExecutionContext.StepContext {
    private KeyedStateCache tagCache;

    public StepContext(String stepName) {
      super(stepName);

      // Mangle such that there are no partially overlapping prefixes.
      String mangledPrefix = stepName.length() + ":" + stepName;
      this.tagCache = new KeyedStateCache(
          mangledPrefix,
          CacheBuilder.newBuilder().build(new TagLoader(mangledPrefix)),
          CacheBuilder.newBuilder().build(new TagListLoader(mangledPrefix)));
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp) {
      tagCache.store(tag, value, timestamp);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag) {
      tagCache.removeTags(tag);
    }

    @Override
    public CodedTupleTagMap lookup(Iterable<? extends CodedTupleTag<?>> tags) throws IOException {
      return CodedTupleTagMap.of(tagCache.lookupTags(tags));
    }

    @Override
    public <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp) {
      tagCache.writeToTagList(tag, value, timestamp);
    }

    @Override
    public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) throws IOException {
      return readTagLists(Arrays.asList(tag)).get(tag);
    }

    @Override
    public <T> Map<CodedTupleTag<T>, Iterable<T>> readTagLists(Iterable<CodedTupleTag<T>> tags)
        throws IOException {
      @SuppressWarnings({"unchecked"})
      Iterable<CodedTupleTag<?>> wildcardTags = (Iterable) tags;
      Map<CodedTupleTag<?>, Iterable<?>> wildcardMap = tagCache.readTagLists(wildcardTags);
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<CodedTupleTag<T>, Iterable<T>> typedMap = (Map) wildcardMap;
      return typedMap;
    }

    @Override
    public <T> void deleteTagList(CodedTupleTag<T> tag) {
      tagCache.removeTagLists(tag);
    }

    public void flushState() throws IOException {
      tagCache.flushTo(outputBuilder);
    }
  }
}
