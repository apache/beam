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
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A write-back cache for the tag and tag list state computed during a given stage.
 *
 * <p>This does not synchronize changes across multiple threads or multiple workers.
 */
class KeyedStateCache {

  private static final Predicate<TagListUpdates<?>> IS_DELETE_TAG_LIST =
      new Predicate<TagListUpdates<?>>() {
        @Override
        public boolean apply(TagListUpdates<?> input) {
          return input.isDelete;
        }
  };

  private final LoadingCache<CodedTupleTag<?>, Optional<?>> tagCache;
  private final Map<CodedTupleTag<?>, KeyedStateCache.TagUpdates<?>> localTagUpdates =
      new LinkedHashMap<>();

  private final LoadingCache<CodedTupleTag<?>, List<?>> tagListCache;
  private final Map<CodedTupleTag<?>, KeyedStateCache.TagListUpdates<?>> localTagListUpdates =
      new LinkedHashMap<>();

  private String tagPrefix;

  public KeyedStateCache(String tagPrefix,
      LoadingCache<CodedTupleTag<?>, Optional<?>> tagCache,
      LoadingCache<CodedTupleTag<?>, List<?>> tagListCache) {
    this.tagPrefix = tagPrefix;
    this.tagCache = tagCache;
    this.tagListCache = tagListCache;
  }

  private <T> KeyedStateCache.TagUpdates<T> getOrCreateTagUpdate(CodedTupleTag<T> tag) {
    @SuppressWarnings("unchecked")
    KeyedStateCache.TagUpdates<T> update = (KeyedStateCache.TagUpdates<T>) localTagUpdates.get(tag);
    if (update == null) {
      update = new KeyedStateCache.TagUpdates<>();
      localTagUpdates.put(tag, update);
    }
    return update;
  }

  private <T> KeyedStateCache.TagListUpdates<T> getOrCreateTagListUpdate(CodedTupleTag<T> tag) {
    @SuppressWarnings("unchecked")
    KeyedStateCache.TagListUpdates<T> update =
        (KeyedStateCache.TagListUpdates<T>) localTagListUpdates.get(tag);
    if (update == null) {
      update = new KeyedStateCache.TagListUpdates<>();
      localTagListUpdates.put(tag, update);
    }
    return update;
  }

  public void removeTags(CodedTupleTag<?>... tags) {
    for (CodedTupleTag<?> tag : tags) {
      getOrCreateTagUpdate(tag).markRemoved();
    }
  }

  public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp) {
    getOrCreateTagUpdate(tag).set(value, timestamp);
  }

  public Map<CodedTupleTag<?>, Object> lookupTags(Iterable<? extends CodedTupleTag<?>> tags)
      throws IOException {
    try {
      ImmutableMap.Builder<CodedTupleTag<?>, Object> outputBuilder = ImmutableMap.builder();

      // Figure out which tags can be fully satisfied with local data, and add them to the output.
      // Other tags, will need to be looked up.
      List<CodedTupleTag<?>> nonLocalTags = new ArrayList<>();
      for (CodedTupleTag<?> tag : tags) {
        TagUpdates<?> localUpdates = localTagUpdates.get(tag);
        if (localUpdates != null) {
          // ImmutableMap's can't hold null, so we just skip putting the value in if its null.
          if (localUpdates.getUpdatedValue() != null) {
            outputBuilder.put(tag, localUpdates.getUpdatedValue());
          }
        } else {
          nonLocalTags.add(tag);
        }
      }

      for (Map.Entry<CodedTupleTag<?>, Optional<?>> entry
          : tagCache.getAll(nonLocalTags).entrySet()) {
        if (entry.getValue().isPresent()) {
          outputBuilder.put(entry.getKey(), entry.getValue().get());
        }
      }

      return outputBuilder.build();
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  public void removeTagLists(CodedTupleTag<?>... tagLists) {
    for (CodedTupleTag<?> tagList : tagLists) {
      getOrCreateTagListUpdate(tagList).markRemoved();
    }
  }

  public <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp) {
    getOrCreateTagListUpdate(tag).add(value, timestamp);
  }

  public Map<CodedTupleTag<?>, Iterable<?>> readTagLists(Iterable<CodedTupleTag<?>> tags)
      throws IOException  {
    try {
      ImmutableMap.Builder<CodedTupleTag<?>, Iterable<?>> outputBuilder = ImmutableMap.builder();

      // Figure out which tags can be fully satisfied with local data, and add them to the output.
      // Other tags, will need to be looked up.
      List<CodedTupleTag<?>> nonDeletedTags = new ArrayList<>();
      for (CodedTupleTag<?> tag : tags) {
        TagListUpdates<?> localUpdates = localTagListUpdates.get(tag);
        if (localUpdates != null && localUpdates.isDelete) {
          // For locally deleted items, we don't need to do a lookup at all
          outputBuilder.put(tag, localUpdates.getAddedItems());
        } else {
          nonDeletedTags.add(tag);
        }
      }

      // For any non-deleted tag, look it up in the tagListCache, and build output by combining
      ImmutableMap<CodedTupleTag<?>, List<?>> cachedContents = tagListCache.getAll(nonDeletedTags);
      for (Map.Entry<CodedTupleTag<?>, List<?>> lookedUp : cachedContents.entrySet()) {
        CodedTupleTag<?> tag = lookedUp.getKey();
        TagListUpdates<?> localUpdates = localTagListUpdates.get(tag);
        outputBuilder.put(tag, localUpdates == null
            ? lookedUp.getValue() : localUpdates.mergeWith(lookedUp.getValue()));
      }

      return outputBuilder.build();
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  public void flushTo(Windmill.WorkItemCommitRequest.Builder outputBuilder) throws IOException {
    // Make sure that we've done lookups for the tag-writes, tag-deletes, and tag-list-deletes.
    try {
      tagCache.getAll(localTagUpdates.keySet());
      tagListCache.getAll(Maps.filterValues(localTagListUpdates, IS_DELETE_TAG_LIST).keySet());
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }

    // Flush the local tag and tag list updates to the commit request
    for (Map.Entry<CodedTupleTag<?>, TagUpdates<?>> update : localTagUpdates.entrySet()) {
      update.getValue().flushTo(update.getKey(), outputBuilder);
    }

    for (Map.Entry<CodedTupleTag<?>, TagListUpdates<?>> update : localTagListUpdates.entrySet()) {
      update.getValue().flushTo(update.getKey(), outputBuilder);
    }

    // Clear the caches and local updates
    tagCache.invalidateAll();
    tagListCache.invalidateAll();
    localTagUpdates.clear();
    localTagListUpdates.clear();
  }

  private ByteString serializeTag(CodedTupleTag<?> tag) {
    return ByteString.copyFromUtf8(tagPrefix + tag.getId());
  }

  private class TagUpdates<T> {
    private T updatedValue;
    private Instant updatedTimestamp;

    boolean removed;

    private void set(T newValue, Instant newTimestamp) {
      removed = false;
      updatedTimestamp = newTimestamp;
      updatedValue = newValue;
    }

    public T getUpdatedValue() {
      return updatedValue;
    }

    private void markRemoved() {
      removed = true;
      updatedTimestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
      updatedValue = null;
    }

    private void flushTo(
        CodedTupleTag<?> wildcardTag, Windmill.WorkItemCommitRequest.Builder outputBuilder)
            throws CoderException, IOException {
      Windmill.Value.Builder valueBuilder = outputBuilder.addValueUpdatesBuilder()
          .setTag(serializeTag(wildcardTag))
          .getValueBuilder();

      if (removed) {
        valueBuilder
            .setTimestamp(Long.MAX_VALUE)
            .setData(ByteString.EMPTY);
      } else {
        @SuppressWarnings("unchecked")
        CodedTupleTag<T> tag = (CodedTupleTag<T>) wildcardTag;

        ByteString.Output stream = ByteString.newOutput();
        tag.getCoder().encode(updatedValue, stream, Coder.Context.OUTER);

        valueBuilder
            .setTimestamp(TimeUnit.MILLISECONDS.toMicros(updatedTimestamp.getMillis()))
            .setData(stream.toByteString());
      }
    }
  }

  private class TagListUpdates<T> {
    boolean isDelete = false;
    List<TimestampedValue<T>> added = new ArrayList<>();

    private void markRemoved() {
      isDelete = true;
      added.clear();
    }


    private void add(T value, Instant timestamp) {
      added.add(TimestampedValue.of(value, timestamp));
    }

    private Iterable<T> getAddedItems() {
      List<T> addedItems = Lists.newArrayList();
      for (TimestampedValue<T> item : added) {
        addedItems.add(item.getValue());
      }
      return addedItems;
    }

    public List<T> mergeWith(List<?> wildcardValue) {
      @SuppressWarnings("unchecked")
      List<T> value = (List<T>) wildcardValue;
      return ImmutableList.<T>builder().addAll(value).addAll(getAddedItems()).build();
    }

    private void flushTo(
        CodedTupleTag<?> wildcardTag, Windmill.WorkItemCommitRequest.Builder outputBuilder)
            throws IOException {
      // First do the delete, if necessary and there were previously elements
      try {
        if (isDelete && tagListCache.get(wildcardTag).size() > 0) {
          outputBuilder.addListUpdatesBuilder()
              .setTag(serializeTag(wildcardTag))
              .setEndTimestamp(Long.MAX_VALUE);
        }
      } catch (ExecutionException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
        throw Throwables.propagate(e.getCause());
      }

      // Then, add all the elements
      if (added.size() > 0) {
        @SuppressWarnings("unchecked")
        CodedTupleTag<T> tag = (CodedTupleTag<T>) wildcardTag;

        Windmill.TagList.Builder listBuilder = outputBuilder.addListUpdatesBuilder()
            .setTag(serializeTag(wildcardTag));
        for (TimestampedValue<T> value : added) {
          ByteString.Output stream = ByteString.newOutput();

          // Windmill does not support empty data for tag list state; prepend a zero byte.
          byte[] zero = {0x0};
          stream.write(zero);

          // Encode the value
          tag.getCoder().encode(value.getValue(), stream, Coder.Context.OUTER);

          listBuilder.addValuesBuilder()
              .setData(stream.toByteString())
              .setTimestamp(TimeUnit.MILLISECONDS.toMicros(value.getTimestamp().getMillis()));
        }
      }
    }
  }
}
