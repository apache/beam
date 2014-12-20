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

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for SideInputUtils.
 */
@RunWith(JUnit4.class)
public class SideInputUtilsTest {
  SideInputInfo createSingletonSideInputInfo(Source sideInputSource) {
    SideInputInfo sideInputInfo = new SideInputInfo();
    sideInputInfo.setSources(Arrays.asList(sideInputSource));
    sideInputInfo.setKind(CloudObject.forClassName("singleton"));
    return sideInputInfo;
  }

  SideInputInfo createCollectionSideInputInfo(Source... sideInputSources) {
    SideInputInfo sideInputInfo = new SideInputInfo();
    sideInputInfo.setSources(Arrays.asList(sideInputSources));
    sideInputInfo.setKind(CloudObject.forClassName("collection"));
    return sideInputInfo;
  }

  Source createSideInputSource(Integer... ints) throws Exception {
    return InMemoryReaderFactoryTest.createInMemoryCloudSource(
        Arrays.asList(ints), null, null, BigEndianIntegerCoder.of());
  }

  void assertThatContains(Object actual, Object... expected) {
    assertThat(actual, instanceOf(Iterable.class));
    Iterable<?> iter = (Iterable<?>) actual;
    if (expected.length == 0) {
      assertThat(iter, is(emptyIterable()));
    } else {
      assertThat(iter, contains(expected));
    }
  }

  @Test
  public void testReadSingletonSideInput() throws Exception {
    SideInputInfo sideInputInfo = createSingletonSideInputInfo(createSideInputSource(42));

    assertEquals(
        42,
        SideInputUtils.readSideInput(
            PipelineOptionsFactory.create(), sideInputInfo, new BatchModeExecutionContext()));
  }

  @Test
  public void testReadEmptyCollectionSideInput() throws Exception {
    SideInputInfo sideInputInfo = createCollectionSideInputInfo(createSideInputSource());

    assertThatContains(SideInputUtils.readSideInput(
        PipelineOptionsFactory.create(), sideInputInfo, new BatchModeExecutionContext()));
  }

  @Test
  public void testReadCollectionSideInput() throws Exception {
    SideInputInfo sideInputInfo = createCollectionSideInputInfo(createSideInputSource(3, 4, 5, 6));

    assertThatContains(
        SideInputUtils.readSideInput(
            PipelineOptionsFactory.create(), sideInputInfo, new BatchModeExecutionContext()),
        3, 4, 5, 6);
  }

  @Test
  public void testReadCollectionShardedSideInput() throws Exception {
    SideInputInfo sideInputInfo =
        createCollectionSideInputInfo(createSideInputSource(3), createSideInputSource(),
            createSideInputSource(4, 5), createSideInputSource(6), createSideInputSource());

    assertThatContains(
        SideInputUtils.readSideInput(
            PipelineOptionsFactory.create(), sideInputInfo, new BatchModeExecutionContext()),
        3, 4, 5, 6);
  }

  @Test
  public void testReadSingletonSideInputValue() throws Exception {
    CloudObject sideInputKind = CloudObject.forClassName("singleton");
    Object elem = "hi";
    List<Object> elems = Arrays.asList(elem);
    assertEquals(elem, SideInputUtils.readSideInputValue(sideInputKind, elems));
  }

  @Test
  public void testReadCollectionSideInputValue() throws Exception {
    CloudObject sideInputKind = CloudObject.forClassName("collection");
    List<Object> elems = Arrays.<Object>asList("hi", "there", "bob");
    assertEquals(elems, SideInputUtils.readSideInputValue(sideInputKind, elems));
  }
}
