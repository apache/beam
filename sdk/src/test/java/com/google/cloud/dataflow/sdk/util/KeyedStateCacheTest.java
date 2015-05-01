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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItemCommitRequest;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link KeyedStateCache}.
 */
@RunWith(JUnit4.class)
public class KeyedStateCacheTest {

  private static final String MANGLED_STEP_PREFIX = "mangled-step-prefix-";

  private static final CodedTupleTag<String> TAG1 = CodedTupleTag.of("tag1", StringUtf8Coder.of());
  private static final CodedTupleTag<Integer> TAG2 = CodedTupleTag.of("tag2", VarIntCoder.of());
  private static final CodedTupleTag<Integer> TAG3 = CodedTupleTag.of("tag3", VarIntCoder.of());

  @Mock
  private CacheLoader<CodedTupleTag<?>, Optional<?>> mockTagLoader;
  @Mock
  private CacheLoader<CodedTupleTag<?>, List<?>> mockTagListLoader;

  private KeyedStateCache underTest;

  private List<CodedTupleTag<?>> tags(CodedTupleTag<?>... tags) {
    return Arrays.asList(tags);
  }

  private Iterable<? extends CodedTupleTag<?>> tagsMatcher(CodedTupleTag<?>... tags) {
    return Mockito.argThat(Matchers.containsInAnyOrder(tags));
  }

  private <T> Map<CodedTupleTag<?>, Optional<?>> lookup(
      CodedTupleTag<T> tag1, Optional<T> value1) {
    return ImmutableMap.<CodedTupleTag<?>, Optional<?>>of(tag1, value1);
  }

  private <T1, T2> Map<CodedTupleTag<?>, Optional<?>> lookup(
      CodedTupleTag<T1> tag1, Optional<T1> value1,
      CodedTupleTag<T2> tag2, Optional<T2> value2) {
    return ImmutableMap.of(tag1, value1, tag2, value2);
  }

  private <T> Map<CodedTupleTag<?>, List<?>> lookupList(
      CodedTupleTag<T> tag1, List<T> value1) {
    return ImmutableMap.<CodedTupleTag<?>, List<?>>of(tag1, value1);
  }

  private <T1, T2> Map<CodedTupleTag<?>, List<?>> lookupList(
      CodedTupleTag<T1> tag1, List<T1> value1,
      CodedTupleTag<T2> tag2, List<T2> value2) {
    return ImmutableMap.of(tag1, value1, tag2, value2);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.underTest = new KeyedStateCache(MANGLED_STEP_PREFIX,
        CacheBuilder.newBuilder().build(mockTagLoader),
        CacheBuilder.newBuilder().build(mockTagListLoader));
  }

  @Test
  public void testGetTagCaches() throws Exception {
    when(mockTagLoader.loadAll(tagsMatcher(TAG1, TAG2)))
        .thenReturn(lookup(TAG1, Optional.of("hello"), TAG2, Optional.of(5)));

    Map<CodedTupleTag<?>, Object> result = underTest.lookupTags(tags(TAG1, TAG2));
    assertEquals(2, result.size());
    assertEquals("hello", result.get(TAG1));
    assertEquals(5, result.get(TAG2));

    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG1, TAG2));
    Mockito.verifyNoMoreInteractions(mockTagLoader);

    when(mockTagLoader.loadAll(tagsMatcher(TAG3)))
        .thenReturn(lookup(TAG3, Optional.of(8)));

    result = underTest.lookupTags(tags(TAG2, TAG3));
    assertEquals(2, result.size());
    assertEquals(5, result.get(TAG2));
    assertEquals(8, result.get(TAG3));

    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG3));
    Mockito.verifyNoMoreInteractions(mockTagLoader);
  }

  @Test
  public void testGetTagLocalEdits() throws Exception {
    underTest.store(TAG2, 42, new Instant(5));
    underTest.store(TAG3, 5, new Instant(10));
    underTest.removeTags(TAG3);

    when(mockTagLoader.loadAll(tagsMatcher(TAG1))).thenReturn(lookup(TAG1, Optional.of("hello")));

    Map<CodedTupleTag<?>, Object> result = underTest.lookupTags(tags(TAG1, TAG2, TAG3));
    assertEquals("hello", result.get(TAG1));
    assertEquals(42, result.get(TAG2));
    assertNull(result.get(TAG3));

    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG1));
    Mockito.verifyNoMoreInteractions(mockTagLoader);

    underTest.store(TAG1, "world", new Instant(22));
    result = underTest.lookupTags(tags(TAG1));
    assertEquals("world", result.get(TAG1));

    Mockito.verifyNoMoreInteractions(mockTagLoader);
  }

  @Test
  public void testFlushTagAlreadyRead() throws Exception {
    // Read TAG1 and TAG2
    when(mockTagLoader.loadAll(tagsMatcher(TAG1, TAG2)))
        .thenReturn(lookup(TAG1, Optional.<String>absent(), TAG2, Optional.of(6)));
    underTest.lookupTags(tags(TAG1, TAG2));

    underTest.store(TAG2, 41, new Instant(5));
    underTest.store(TAG3, 43, new Instant(6));
    underTest.store(TAG2, 42, new Instant(7));
    underTest.removeTags(TAG3);

    // Load to prevent blind writes -- only need to read TAG3
    when(mockTagLoader.loadAll(tagsMatcher(TAG3)))
        .thenReturn(lookup(TAG3, Optional.of(6)));

    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.flushTo(outputBuilder);
    WorkItemCommitRequest commitRequest = outputBuilder.buildPartial();

    assertEquals(Joiner.on("\n").join(
        "value_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG2.getId() + "\"",
        "  value {",
        "    timestamp: 7000",
        "    data: \"*\"",
        "  }",
        "}",
        "value_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG3.getId() + "\"",
        "  value {",
        "    timestamp: " + Long.MAX_VALUE,
        "    data: \"\"",
        "  }",
        "}",
        ""),
        commitRequest.toString());

    // Should load 3 to prevent blind delete
    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG1, TAG2));
    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG3));
    Mockito.verifyNoMoreInteractions(mockTagLoader);
  }

  @Test
  public void testFlushTagBlindWrites() throws Exception {
    underTest.store(TAG2, 41, new Instant(5));
    underTest.store(TAG3, 43, new Instant(6));
    underTest.store(TAG2, 42, new Instant(7));
    underTest.removeTags(TAG3);

    // Load to prevent blind writes
    when(mockTagLoader.loadAll(tagsMatcher(TAG2, TAG3)))
        .thenReturn(lookup(TAG2, Optional.of(5), TAG3, Optional.of(6)));

    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.flushTo(outputBuilder);
    WorkItemCommitRequest commitRequest = outputBuilder.buildPartial();

    assertEquals(Joiner.on("\n").join(
        "value_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG2.getId() + "\"",
        "  value {",
        "    timestamp: 7000",
        "    data: \"*\"",
        "  }",
        "}",
        "value_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG3.getId() + "\"",
        "  value {",
        "    timestamp: " + Long.MAX_VALUE,
        "    data: \"\"",
        "  }",
        "}",
        ""),
        commitRequest.toString());

    // Should load 3 to prevent blind delete
    Mockito.verify(mockTagLoader).loadAll(tagsMatcher(TAG2, TAG3));
    Mockito.verifyNoMoreInteractions(mockTagLoader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTagListIsCached() throws Exception {
    when(mockTagListLoader.loadAll(tagsMatcher(TAG1, TAG2)))
        .thenReturn(lookupList(
            TAG1, Arrays.asList("hello", "world"),
            TAG2, Arrays.asList(5, 10)));
    when(mockTagListLoader.loadAll(tagsMatcher(TAG3)))
    .thenReturn(lookupList(
        TAG3, Arrays.asList(6, 7)));

    Map<CodedTupleTag<?>, Iterable<?>> results = underTest.readTagLists(tags(TAG1, TAG2));
    assertThat((Iterable<String>) results.get(TAG1), Matchers.contains("hello", "world"));
    assertThat((Iterable<Integer>) results.get(TAG2), Matchers.contains(5, 10));

    results = underTest.readTagLists(tags(TAG1, TAG2, TAG3));
    assertThat((Iterable<String>) results.get(TAG1), Matchers.contains("hello", "world"));
    assertThat((Iterable<Integer>) results.get(TAG2), Matchers.contains(5, 10));
    assertThat((Iterable<Integer>) results.get(TAG3), Matchers.contains(6, 7));

    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG1, TAG2));
    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG3));
    Mockito.verifyNoMoreInteractions(mockTagListLoader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTagListLocalEdits() throws Exception {
    // First make local edits
    underTest.writeToTagList(TAG1, "goodbye", new Instant(50));
    underTest.writeToTagList(TAG1, "also", new Instant(55));
    underTest.writeToTagList(TAG2, 15, new Instant(55));
    underTest.writeToTagList(TAG3, 20, new Instant(60));
    underTest.removeTagLists(TAG3);

    // Now look things up
    when(mockTagListLoader.loadAll(tagsMatcher(TAG1, TAG2)))
        .thenReturn(lookupList(
            TAG1, Arrays.asList("hello", "world"),
            TAG2, Arrays.asList(5, 10)));
    Map<CodedTupleTag<?>, Iterable<?>> results = underTest.readTagLists(tags(TAG1, TAG2, TAG3));
    assertThat((Iterable<String>) results.get(TAG1),
        Matchers.contains("hello", "world", "goodbye", "also"));
    assertThat((Iterable<Integer>) results.get(TAG2), Matchers.contains(5, 10, 15));
    assertThat((Iterable<Integer>) results.get(TAG3), Matchers.emptyIterable());

    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG1, TAG2));
    Mockito.verifyNoMoreInteractions(mockTagListLoader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTagListLocalFlush() throws Exception {
    // First make local edits
    underTest.writeToTagList(TAG1, "goodbye", new Instant(50));
    underTest.writeToTagList(TAG1, "also", new Instant(55));
    underTest.writeToTagList(TAG2, 15, new Instant(55));
    underTest.writeToTagList(TAG3, 20, new Instant(60));
    underTest.removeTagLists(TAG3);

    // User lookup -- shouldn't need to re-lookup
    when(mockTagListLoader.loadAll(tagsMatcher(TAG1)))
        .thenReturn(lookupList(
            TAG1, Arrays.asList("hello", "world")));
    underTest.readTagLists(tags(TAG1));
    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG1));
    Mockito.verifyNoMoreInteractions(mockTagListLoader);

    // When we flush, we should lookup TAG3 (to prevent blind deletes)
    when(mockTagListLoader.loadAll(tagsMatcher(TAG3)))
        .thenReturn(lookupList(TAG3, Arrays.asList(5)));

    // Flush and verify output
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.flushTo(outputBuilder);
    WorkItemCommitRequest commitRequest = outputBuilder.buildPartial();

    assertEquals(Joiner.on("\n").join(
        "list_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG1.getId() + "\"",
        "  values {",
        "    timestamp: 50000",
        "    data: \"\\000goodbye\"",
        "  }",
        "  values {",
        "    timestamp: 55000",
        "    data: \"\\000also\"",
        "  }",
        "}",
        "list_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG2.getId() + "\"",
        "  values {",
        "    timestamp: 55000",
        "    data: \"\\000\\017\"",
        "  }",
        "}",
        "list_updates {",
        "  tag: \"" + MANGLED_STEP_PREFIX + TAG3.getId() + "\"",
        "  end_timestamp: " + Long.MAX_VALUE,
        "}",
        ""),
        commitRequest.toString());

    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG3));
    Mockito.verifyNoMoreInteractions(mockTagListLoader);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTagListNoDeleteEmptyList() throws Exception {
    underTest.removeTagLists(TAG3);

    // When we flush, we should lookup TAG3 (to prevent blind deletes)
    when(mockTagListLoader.loadAll(tagsMatcher(TAG3)))
        .thenReturn(lookupList(TAG3, Arrays.<Integer>asList()));

    // Flush and verify output
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.flushTo(outputBuilder);
    WorkItemCommitRequest commitRequest = outputBuilder.buildPartial();

    assertEquals("", commitRequest.toString());

    Mockito.verify(mockTagListLoader).loadAll(tagsMatcher(TAG3));
    Mockito.verifyNoMoreInteractions(mockTagListLoader);
  }
}
